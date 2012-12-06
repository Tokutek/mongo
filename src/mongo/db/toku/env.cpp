/**
*    Copyright (C) 2012 Tokutek Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the *    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "pch.h"

#include <toku_os.h>

#include "db/toku/env.h"
#include "db/toku/dbt-bson-inl.h"
#include "db/toku/invariant.h"

#include <boost/filesystem/operations.hpp>

#include "db/namespace_details.h"
#include "db/cmdline.h"

namespace toku {

    static DB_ENV *env;
    static std::vector<std::pair<std::string, DB *> > tokudb_indexes;

    // we use txns and logging if dur/journaling was set on the command line
    static bool using_txns(void) {
        return mongo::cmdLine.dur ? true : false;
    }

    // if using txns and logging, begin a txn for this cursor
    // requires: environment is opened from a previous call to get a db
    void env_maybe_begin_txn(DB_TXN **txn) {
        invariant(env);
        if (using_txns()) {
            int r = env->txn_begin(env, NULL, txn, 0);
            invariant(r == 0);
        } else {
            *txn = NULL;
        }
    }

    // if using txns and logging, commit the txn in the cursor
    // requires: environment is opened from a previous call to get a db
    void env_maybe_commit_txn(DB_TXN *txn) {
        invariant(env);
        if (using_txns()) {
            int r = txn->commit(txn, 0);
            invariant(r == 0);
        } else {
            invariant(txn == NULL);
        }
    }

    // for a given idx, whats the tokudb db name?
    static std::string generate_idx_db_name(const mongo::IndexDetails &idx) {
        // the index namespace has ns and index name, so it's unique
        const std::string &ns = idx.indexNamespace();
        return ns;
    }

    // tokudb environment comparison functions to compare
    // two bson objects, converted from DBTs
    static int comparison_function(DB *db, const DBT *key1, const DBT *key2) {
        int c;

        // extract bson objects from each dbt and get the ordering
        invariant(db->cmp_descriptor);
        const DBT *key_pattern_dbt = &db->cmp_descriptor->dbt;
        invariant(key_pattern_dbt->data);
        const mongo::BSONObj key_pattern = init_bson_from_dbt(key_pattern_dbt);
        const mongo::Ordering ordering = mongo::Ordering::make(key_pattern);

        // first compare by bson obj key
        const mongo::BSONObj obj1 = init_bson_from_dbt(key1); 
        const mongo::BSONObj obj2 = init_bson_from_dbt(key2); 
        c = obj1.woCompare(obj2, ordering);
        if (c < 0) {
            return -1;
        } else if (c > 0) {
            return 1;
        }

        // the bson objs must be equal, so compare by recordloc,
        // which is stored after the bson object in the buffer
        const char *buf1 = reinterpret_cast<char *>(key1->data) + obj1.objsize();
        const char *buf2 = reinterpret_cast<char *>(key2->data) + obj2.objsize();
        const mongo::DiskLoc *loc1 = reinterpret_cast<const mongo::DiskLoc *>(buf1);
        const mongo::DiskLoc *loc2 = reinterpret_cast<const mongo::DiskLoc *>(buf2);
        c = loc1->compare(*loc2);
        if (c < 0) {
            return -1;
        } else if (c == 0) {
            return 0;
        } else {
            return 1;
        }
    }

    static uint64_t calculate_cachesize(void) {
        uint64_t physmem, maxdata;
        physmem = toku_os_get_phys_memory_size();
        uint64_t cache_size = physmem / 2;
        int r = toku_os_get_max_process_data_size(&maxdata);
        if (r == 0) {
            if (cache_size > maxdata / 8)
                cache_size = maxdata / 8;
        }
        return cache_size;
    }

    static void open_env(void) {
        int r;
        r = db_env_create(&env, 0);
        invariant(r == 0);

        env->set_errfile(env, stderr);
        printf("tokudb: set errfile to stderr\n");

        const char *errpfx = "[tokudb environment error]";
        env->set_errpfx(env, errpfx);
        printf("tokudb: set errpfx to %s\n", errpfx);

        const uint64_t cachesize = calculate_cachesize();
        const uint32_t bytes = cachesize % (1024L * 1024L * 1024L);
        const uint32_t gigabytes = cachesize >> 30;
        r = env->set_cachesize(env, gigabytes, bytes, 1);
        invariant(r == 0);
        printf("tokudb: cachesize set to %u GB + %u bytes\n", gigabytes, bytes);

        r = env->set_default_bt_compare(env, comparison_function);
        invariant(r == 0);

        // tokudb on mongo can run with or without txn/logging. a single tokudb
        // environment can also run with logging for a while, then without logging
        // for a while, etc, without rebuilding the environment. the only catch
        // here is I think you MUST perform a clean shutdown with logging enabled
        // if you wish to subsequently run without logging, or else weird things
        // can happen with the log the next time you try to run with logging.
        const int log_flags = using_txns() ? DB_INIT_LOG | DB_INIT_TXN : 0;
        const int flags = DB_CREATE | DB_PRIVATE | DB_RECOVER | log_flags;
        boost::filesystem::path env_path = boost::filesystem::path(mongo::dbpath) / "tokudb";
        boost::filesystem::create_directory(env_path);
        const char *env_dir = env_path.string().c_str();
        r = env->open(env, env_dir, flags, 0755);
        invariant(r == 0);
        printf("tokudb: environment opened at %s, logging %s\n", env_dir, 
                using_txns() ? "enabled" : "disabled");

        const int checkpoint_period = 60;
        r = env->checkpointing_set_period(env, checkpoint_period);
        invariant(r == 0);
        printf("tokudb: checkpoint period set to %d sec\n", checkpoint_period);

        const int cleaner_period = 2;
        r = env->cleaner_set_period(env, cleaner_period);
        invariant(r == 0);
        printf("tokudb: cleaner period set to %d sec\n", cleaner_period);

        const int cleaner_iterations = 5;
        r = env->cleaner_set_iterations(env, cleaner_iterations);
        invariant(r == 0);
        printf("tokudb: cleaner iterations set to %d\n", cleaner_iterations);
    }

    // open the environment if it isn't already
    static void maybe_open_env(void) {
        if (env == NULL) {
            open_env();
        }
        invariant(env);
    }

    static TOKU_COMPRESSION_METHOD string_to_compression_method(const std::string s) {
        if (s == "lzma") {
            return TOKU_LZMA_METHOD;
        } else if (s == "quicklz") {
            return TOKU_QUICKLZ_METHOD;
        } else if (s == "zlib") {
            return TOKU_ZLIB_METHOD;
        } else if (s == "none") {
            return TOKU_NO_COMPRESSION;
        } else {
            uassert(16429, "compression must be one of: lzma, quicklz, zlib, none. drop this index and try again.", false);
        }
        return TOKU_DEFAULT_COMPRESSION_METHOD;
    }

    // open a db and save the mapping (name -> DB handle) in the vector of indexes
    static DB *open_db(const std::string &db_name, const mongo::BSONObj &info) {
        DB *db;
        mongo::BSONElement e;
        uint32_t size;
        int r = db_create(&db, env, 0);
        invariant(r == 0);

        e = info["blocksize"];
        if (e.ok() && !e.isNull()) {
            std::cout << "blocksize obj: " << e.toString() << std::endl;
            size = e.numberInt();
            uassert(16430, "blocksize must be a number > 0. drop this index and try again.", e.isNumber() && size > 0);
            r = db->set_pagesize(db, size);
            invariant(r == 0);
            printf("tokudb: db %s, set blocksize to %u\n", db_name.c_str(), size);
        }
        e = info["basementsize"];
        if (e.ok() && !e.isNull()) {
            size = e.numberInt();
            uassert(16431, "basementsize must be a number > 0. drop this index and try again.", e.isNumber () && size > 0);
            r = db->set_readpagesize(db, size);
            invariant(r == 0);
            printf("tokudb: db %s, set basementsize to %u\n", db_name.c_str(), size);
        }
        e = info["compression"];
        if (e.ok() && !e.isNull()) {
            std::string method_str = e.String();
            TOKU_COMPRESSION_METHOD method = string_to_compression_method(method_str);
            r = db->set_compression_method(db, method);
            invariant(r == 0);
            printf("tokudb: db %s, set compression to %s\n", db_name.c_str(), method_str.c_str());
        }
        r = db->open(db, NULL, db_name.c_str(), NULL, DB_BTREE, DB_CREATE, 0644);
        invariant(r == 0);
        printf("tokudb: opened db %s\n", db_name.c_str());
        tokudb_indexes.push_back(std::pair<std::string, DB *>(db_name, db));
        return db;
    }

    // close the db with the given name and remove it from the map
    static void close_db(const std::string &db_name) {
        DB *db = NULL;
        std::vector<std::pair<std::string, DB *> >::iterator i;
        for (i = tokudb_indexes.begin(); i != tokudb_indexes.end(); i++) {
            if (i->first == db_name) {
                db = i->second;
                break;
            }
        }
        invariant(db);
        int r = db->close(db, 0);
        invariant(r == 0);
        printf("tokudb: closed db named %s\n", db_name.c_str());
        tokudb_indexes.erase(i);
    }

    // search through the map of dbs for one with the given namespace and name
    // return:
    // - open db if it was found
    // - NULL if no open db was found
    static DB *get_db_by_name(const std::string &db_name) {
        DB *db = NULL;
        std::vector<std::pair<std::string, DB *> >::iterator i;
        for (i = tokudb_indexes.begin(); i != tokudb_indexes.end(); i++) {
            if (i->first == db_name) {
                db = i->second;
                break;
            }
        }
        return db;
    }

    // set a descriptor for the given dictionary. the descriptor is
    // a serialization of an idx's keyPattern.
    static void set_db_descriptor(DB *db, const mongo::BSONObj &keyPattern) {
        DBT ordering_dbt;
        ordering_dbt.data = (void *) keyPattern.objdata();
        ordering_dbt.size = keyPattern.objsize();
        ordering_dbt.ulen = keyPattern.objsize();
        ordering_dbt.flags = DB_DBT_USERMEM;
        //DB_TXN *txn;
        //env_maybe_begin_txn(&txn);
        const int flags = DB_UPDATE_CMP_DESCRIPTOR;
        int r = db->change_descriptor(db, NULL /*txn*/, &ordering_dbt, flags);
        //env_maybe_commit_txn(txn);
        invariant(r == 0);
    }

    // get and maybe open a DB handle for the given idx
    // return:
    // - a shared, open db handle for this idx.
    DB *env_get_db_handle_by_idx(const mongo::IndexDetails &idx) {
        maybe_open_env();
        // try to get an existing open db. this is fast so long
        // the number of indexes is small, and it will be for now.
        const std::string db_name = generate_idx_db_name(idx);
        DB *db = get_db_by_name(db_name);

        // open the db for this index name if it isn't already
        if (db == NULL) {
            db = open_db(db_name, idx.info.obj());
            set_db_descriptor(db, idx.keyPattern());
        }
        invariant(db);
        return db;
    }

    bool env_get_db_data_size(const mongo::IndexDetails &idx, uint64_t *data_size) {
        DB *db = env_get_db_handle_by_idx(idx);
        if (db) {
            TOKU_DB_FRAGMENTATION_S fragmentation;
            int r = db->get_fragmentation(db, &fragmentation);
            invariant(r == 0);
            *data_size = fragmentation.data_bytes;
            return true;
        } else {
            return false;
        }
    }

    // TODO: this index should be 'closed' by the caller first, not here.
    // drop the index by removing the db
    void env_drop_index(const mongo::IndexDetails &idx) {
        maybe_open_env();
        int r;
        // see if there is an open handle and close it if so
        const std::string &db_name = generate_idx_db_name(idx);
        DB *db = get_db_by_name(db_name);
        if (db) {
            close_db(db_name);
        }
        // remove the dictionary from the environment
        printf("tokudb: dropping db %s\n", db_name.c_str());
        r = env->dbremove(env, NULL, db_name.c_str(), NULL, 0); 
        // it's okay to drop an index that doesn't exist, only
        // if we didn't have it open before this call.
        invariant(r == 0 || (db == NULL && r == ENOENT));
    }

    // shutdown tokudb by closing all dictionaries and the env, if open.
    void env_shutdown(void) {
        maybe_open_env();
        int r;
        invariant(env);
        std::vector<std::pair<std::string, DB *> >::iterator i;
        for (i = tokudb_indexes.begin(); i != tokudb_indexes.end(); i++) {
            const std::string &db_name = i->first;
            DB *db = i->second;
            printf("tokudb: closing dictionary %s\n", db_name.c_str());
            r = db->close(db, 0);
            invariant(r == 0);
        }
        printf("tokudb: closing environment\n");
        r = env->close(env, 0);
        invariant(r == 0);
    }

} /* namespace toku */
