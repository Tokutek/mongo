/**
*    Copyright (C) 2012 Tokutek Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "env.h"

#include "mongo/pch.h"

#include <string>

#include <db.h>
#include <toku_time.h>
#include <toku_os.h>
#include <partitioned_counter.h>

#include <boost/filesystem.hpp>
#ifdef _WIN32
# error "Doesn't support windows."
#endif
#include <fcntl.h>

#include "mongo/db/client.h"
#include "mongo/db/cmdline.h"
#include "mongo/util/log.h"

namespace mongo {

    // TODO: Should be in CmdLine or something.
    extern string dbpath;

    namespace storage {

        DB_ENV *env;

        static int dbt_bson_compare(DB *db, const DBT *key1, const DBT *key2) {
            verify(db->cmp_descriptor);

            // Primary _id keys are represented by exactly one BSON Object.
            // Secondary keys are represented by exactly two, the secondary
            // key plus an associated _id key.
            dassert(key1->size > 0);
            dassert(key2->size > 0);
            const BSONObj obj1(static_cast<char *>(key1->data));
            const BSONObj obj2(static_cast<char *>(key2->data));
            dassert((int) key1->size >= obj1.objsize());
            dassert((int) key2->size >= obj2.objsize());

            // Compare by the first object. The ordering comes from the key pattern.
            {
                const BSONObj key_pattern(static_cast<char *>(db->cmp_descriptor->dbt.data));
                const Ordering ordering = Ordering::make(key_pattern);
                const int c = obj1.woCompare(obj2, ordering);
                if (c < 0) {
                    return -1;
                } else if (c > 0) {
                    return 1;
                }
            }

            // Compare by the second object, if it exists.
            int key1_bytes_left = key1->size - obj1.objsize();
            int key2_bytes_left = key2->size - obj2.objsize();
            if (key1_bytes_left > 0 && key2_bytes_left > 0) {
                const BSONObj other_obj1(static_cast<char *>(key1->data) + obj1.objsize());
                const BSONObj other_obj2(static_cast<char *>(key2->data) + obj2.objsize());
                dassert(obj1.objsize() + other_obj1.objsize() == (int) key1->size);
                dassert(obj2.objsize() + other_obj2.objsize() == (int) key2->size);

                static const Ordering id_ordering = Ordering::make(BSON("_id" << 1));
                const int c = other_obj1.woCompare(other_obj2, id_ordering);
                if (c < 0) {
                    return -1;
                } else if (c > 0) {
                    return 1;
                }
            } else {
                // The associated primary key must exist in both keys, or neither.
                dassert(key1_bytes_left == 0 && key2_bytes_left == 0);
            }
            return 0;
        }

        static uint64_t calculate_cachesize(void) {
            uint64_t physmem, maxdata;
            physmem = toku_os_get_phys_memory_size();
            uint64_t cache_size = physmem / 2;
            int r = toku_os_get_max_process_data_size(&maxdata);
            if (r == 0) {
                if (cache_size > maxdata / 8) {
                    cache_size = maxdata / 8;
                }
            }
            return cache_size;
        }

        void startup(void) {
            tokulog() << "startup" << endl;

            db_env_set_direct_io(cmdLine.directio);

            int r = db_env_create(&env, 0);
            verify(r == 0);

            const uint64_t cachesize = (cmdLine.cacheSize > 0
                                        ? cmdLine.cacheSize
                                        : calculate_cachesize());
            const uint32_t bytes = cachesize % (1024L * 1024L * 1024L);
            const uint32_t gigabytes = cachesize >> 30;
            r = env->set_cachesize(env, gigabytes, bytes, 1);
            verify(r == 0);
            tokulog(1) << "cachesize set to " << gigabytes << " GB + " << bytes << " bytes."<< endl;

            // Use 10% the size of the cachetable for lock tree memory
            const int32_t lock_memory = cachesize / 10;
            r = env->set_lk_max_memory(env, lock_memory);
            verify(r == 0);
            tokulog() << "locktree max memory set to " << lock_memory << " bytes." << endl;

            r = env->set_default_bt_compare(env, dbt_bson_compare);
            verify(r == 0);

            env->change_fsync_log_period(env, cmdLine.logFlushPeriod);

            const int env_flags = DB_INIT_LOCK|DB_INIT_MPOOL|DB_INIT_TXN|DB_CREATE|DB_PRIVATE|DB_INIT_LOG|DB_RECOVER;
            const int env_mode = S_IRWXU|S_IRGRP|S_IROTH|S_IXGRP|S_IXOTH;
            r = env->open(env, dbpath.c_str(), env_flags, env_mode);
            verify(r == 0);

            const int checkpoint_period = cmdLine.checkpointPeriod;
            r = env->checkpointing_set_period(env, checkpoint_period);
            verify(r == 0);
            tokulog(1) << "checkpoint period set to " << checkpoint_period << " seconds." << endl;

            const int cleaner_period = cmdLine.cleanerPeriod;
            r = env->cleaner_set_period(env, cleaner_period);
            verify(r == 0);
            tokulog(1) << "cleaner period set to " << cleaner_period << " seconds." << endl;

            const int cleaner_iterations = cmdLine.cleanerIterations;
            r = env->cleaner_set_iterations(env, cleaner_iterations);
            verify(r == 0);
            tokulog(1) << "cleaner iterations set to " << cleaner_iterations << "." << endl;
        }

        void shutdown(void) {
            tokulog() << "shutdown" << endl;
            // It's possible for startup to fail before storage::startup() is called
            if (env != NULL) {
                int r = env->close(env, 0);
                verify(r == 0);
            }
        }

        // set a descriptor for the given dictionary. the descriptor is
        // a serialization of the index's key pattern.
        static void set_db_descriptor(DB *db, DB_TXN *txn, const BSONObj &key_pattern) {
            DBT ordering_dbt;
            ordering_dbt.data = const_cast<char *>(key_pattern.objdata());
            ordering_dbt.size = key_pattern.objsize();
            const int flags = DB_UPDATE_CMP_DESCRIPTOR;
            int r = db->change_descriptor(db, txn, &ordering_dbt, flags);
            verify(r == 0);
            tokulog(1) << "set db " << db << " descriptor to key pattern: " << key_pattern << endl;
        }

        int db_open(DB **dbp, const string &name, const BSONObj &info, bool may_create) {
            Client::Context *ctx = cc().getContext();

            // TODO: Refactor this option setting code to someplace else. It's here because
            // the YDB api doesn't allow a db->close to be called before db->open, and we
            // would leak memory if we chose to do nothing. So we validate all the
            // options here before db_create + db->open.
            int readPageSize = 65536;
            int pageSize = 4*1024*1024;
            TOKU_COMPRESSION_METHOD compression = TOKU_DEFAULT_COMPRESSION_METHOD;
            BSONObj key_pattern = info["key"].Obj();
            
            BSONElement e;
            e = info["readPageSize"];
            if (e.ok() && !e.isNull()) {
                readPageSize = e.numberInt();
                uassert(16441, "readPageSize must be a number > 0.", e.isNumber () && readPageSize > 0);
                tokulog(1) << "db " << name << ", using read page size " << readPageSize << endl;
            }
            e = info["pageSize"];
            if (e.ok() && !e.isNull()) {
                pageSize = e.numberInt();
                uassert(16445, "pageSize must be a number > 0.", e.isNumber () && pageSize > 0);
                tokulog(1) << "db " << name << ", using page size " << pageSize << endl;
            }
            e = info["compression"];
            if (e.ok() && !e.isNull()) {
                std::string str = e.String();
                if (str == "lzma") {
                    compression = TOKU_LZMA_METHOD;
                } else if (str == "quicklz") {
                    compression = TOKU_QUICKLZ_METHOD;
                } else if (str == "zlib") {
                    compression = TOKU_ZLIB_WITHOUT_CHECKSUM_METHOD;
                } else if (str == "none") {
                    compression = TOKU_NO_COMPRESSION;
                } else {
                    uassert(16442, "compression must be one of: lzma, quicklz, zlib, none.", false);
                }
                tokulog(1) << "db " << name << ", using compression method \"" << str << "\"" << endl;
            }

            DB *db;
            int r = db_create(&db, env, 0);
            verify(r == 0);

            r = db->set_readpagesize(db, readPageSize);
            verify(r == 0);
            r = db->set_pagesize(db, pageSize);
            verify(r == 0);
            r = db->set_compression_method(db, compression);
            verify(r == 0);

            const int db_flags = may_create ? DB_CREATE : 0;
            if (may_create) {
                verify(ctx->hasTransaction());
                verify(ctx->transactionIsRoot());
            }
            DB_TXN *txn = ctx->hasTransaction() ? ctx->transaction().txn() : NULL;
            r = db->open(db, txn, name.c_str(), NULL, DB_BTREE, db_flags, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
            if (r == ENOENT) {
                verify(!may_create);
                goto exit;
            }
            verify(r == 0);

            set_db_descriptor(db, txn, key_pattern);
            *dbp = db;
        exit:
            return r;
        }

        void db_close(DB *db) {
            int r = db->close(db, 0);
            verify(r == 0);
        }

        void db_remove(const string &name) {
            Client::Context *ctx = cc().getContext();
            verify(ctx->transactionIsRoot());
            int r = env->dbremove(env, ctx->transaction().txn(), name.c_str(), NULL, 0);
            if (r == ENOENT) {
                uasserted(16444, "TODO: dbremove bug, should crash but won't right now");
            }
            verify(r == 0);
        }

        void get_status(BSONObjBuilder &status) {
            uint64_t num_rows;
            uint64_t panic;
            size_t panic_string_len = 128;
            char panic_string[panic_string_len];
            fs_redzone_state redzone_state;

            int r = storage::env->get_engine_status_num_rows(storage::env, &num_rows);
            verify( r == 0 );
            TOKU_ENGINE_STATUS_ROW_S mystat[num_rows];
            r = env->get_engine_status(env, mystat, num_rows, &redzone_state, &panic, panic_string, panic_string_len);
            verify( r == 0 );
            status.append( "panic code", (long long) panic );
            status.append( "panic string", panic_string );
            switch (redzone_state) {
                case FS_GREEN:
                    status.append( "filesystem status", "OK" );
                    break;
                case FS_YELLOW:
                    status.append( "filesystem status", "Getting full..." );
                    break;
                case FS_RED:
                    status.append( "filesystem status", "Critically full. Engine is read-only until space is freed." );
                    break;
                case FS_BLOCKED:
                    status.append( "filesystem status", "Completely full. Free up some space now." );
                    break;
                default:
                    {
                        StringBuilder s;
                        s << "Unknown. Code: " << (int) redzone_state;
                        status.append( "filesystem status", s.str() );
                    }
            }
            for (uint64_t i = 0; i < num_rows; i++) {
                TOKU_ENGINE_STATUS_ROW row = &mystat[i];
                switch (row->type) {
                case UINT64:
                    status.appendNumber( row->keyname, (uint64_t)row->value.num );
                    break;
                case CHARSTR:
                    status.append( row->keyname, row->value.str );
                    break;
                case UNIXTIME:
                    {
                        time_t t = row->value.num;
                        char tbuf[26];
                        status.appendNumber( row->keyname, (uint64_t)ctime_r(&t, tbuf) );
                    }
                    break;
                case TOKUTIME:
                    status.appendNumber( row->keyname, tokutime_to_seconds(row->value.num) );
                    break;
                case PARCOUNT:
                    {
                        uint64_t v = read_partitioned_counter(row->value.parcount);
                        status.appendNumber( row->keyname, v );
                    }
                    break;
                default:
                    {
                        StringBuilder s;
                        s << "Unknown type. Code: " << (int) row->type;
                        status.append( row->keyname, s.str() );
                    }
                    break;                
                }
            }
        }

        void log_flush() {
            int r = 0;
            r = env->log_flush(env, NULL);
            verify(r == 0);
        }

    } // namespace storage

} // namespace mongo
