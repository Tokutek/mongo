/**
*    Copyright (C) 2013 Tokutek Inc.
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

#include <errno.h>
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

#include "mongo/db/curop.h"
#include "mongo/db/client.h"
#include "mongo/db/cmdline.h"
#include "mongo/db/descriptor.h"
#include "mongo/db/storage/assert_ids.h"
#include "mongo/db/storage/dbt.h"
#include "mongo/db/storage/exception.h"
#include "mongo/db/storage/key.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

namespace mongo {

    // TODO: Should be in CmdLine or something.
    extern string dbpath;

    namespace storage {

        DB_ENV *env;

        static int dbt_key_compare(DB *db, const DBT *dbt1, const DBT *dbt2) {
            try {
                const DBT *desc = &db->cmp_descriptor->dbt;
                verify(desc->data != NULL);

                Descriptor descriptor(reinterpret_cast<const char *>(desc->data), desc->size);
                Key key1(dbt1);
                Key key2(dbt2);
                return descriptor.compareKeys(key1, key2);
            } catch (std::exception &e) {
                // We don't have a way to return an error from a comparison (through the ydb), and the ydb isn't exception-safe.
                // Of course, if a comparison throws, something is very wrong anyway.
                // The only safe thing to do here is to crash.
                log() << "Caught an exception in a comparison function, this is impossible to handle:" << endl;
                DBException *dbe = dynamic_cast<DBException *>(&e);
                if (dbe) {
                    log() << "DBException " << dbe->getCode() << ": " << e.what() << endl;
                } else {
                    log() << e.what() << endl;
                }
                fassertFailed(16455);
            }
        }

        static int generate_keys(DB *dest_db, DB *src_db,
                                 DBT_ARRAY *dest_keys,
                                 const DBT *src_key, const DBT *src_val) {
            try {
                const DBT *desc = &dest_db->cmp_descriptor->dbt;
                Descriptor descriptor(reinterpret_cast<const char *>(desc->data), desc->size);

                const Key sPK(src_key);
                dassert(sPK.pk().isEmpty());
                const BSONObj pk(sPK.key());
                const BSONObj obj(reinterpret_cast<const char *>(src_val->data));

                // The ydb knows that src_db does not need keys generated,
                // because the one and only key is src_key
                verify(dest_db != src_db);

                // Generate keys for a secondary index.
                BSONObjSet keys;
                descriptor.generateKeys(obj, keys);
                dbt_array_clear_and_resize(dest_keys, keys.size());
                for (BSONObjSet::const_iterator i = keys.begin(); i != keys.end(); i++) {
                    const Key sKey(*i, &pk);
                    dbt_array_push(dest_keys, sKey.buf(), sKey.size());
                }
                // Set the multiKey bool if it's provided and we generated multiple keys.
                // See NamespaceDetails::Indexer::Indexer()
                if (dest_db->app_private != NULL && keys.size() > 1) {
                    bool *multiKey = reinterpret_cast<bool *>(dest_db->app_private);
                    if (!*multiKey) {
                        *multiKey = true;
                    }
                }
            } catch (const DBException &ex) {
                verify(ex.getCode() > 0);
                return ex.getCode();
            } catch (const std::exception &ex) {
                problem() << "Unhandled std::exception in storage::generate_keys()" << endl;
                verify(false);
            }
            return 0;
        }

        static int generate_row_for_del(DB *dest_db, DB *src_db,
                                        DBT_ARRAY *dest_keys,
                                        const DBT *src_key, const DBT *src_val) {
            // Delete just needs keys, generate them.
            return generate_keys(dest_db, src_db, dest_keys, src_key, src_val);
        }

        static int generate_row_for_put(DB *dest_db, DB *src_db,
                                        DBT_ARRAY *dest_keys, DBT_ARRAY *dest_vals,
                                        const DBT *src_key, const DBT *src_val) {
            // Put needs keys and possibly vals (for clustering indexes.)
            const int r = generate_keys(dest_db, src_db, dest_keys, src_key, src_val);
            if (r != 0) {
                return r;
            }

            const DBT *desc = &dest_db->cmp_descriptor->dbt;
            Descriptor descriptor(reinterpret_cast<const char *>(desc->data), desc->size);
            if (dest_vals != NULL) {
                // TODO: This copies each value once, which is not good. Find a way to avoid that.
                dbt_array_clear_and_resize(dest_vals, dest_keys->size);
                for (size_t i = 0; i < dest_keys->size; i++) {
                    if (descriptor.clustering()) {
                        dbt_array_push(dest_vals, src_val->data, src_val->size);
                    } else {
                        dbt_array_push(dest_vals, NULL, 0);
                    }
                }
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

        static void tokudb_print_error(const DB_ENV * db_env, const char *db_errpfx, const char *buffer) {
            tokulog() << db_errpfx << ": " << buffer << endl;
        }

        static void lock_not_granted_callback(DB *db, uint64_t requesting_txnid,
                                              const DBT *left_key, const DBT *right_key,
                                              uint64_t blocking_txnid);

        void startup(void) {
            tokulog() << "startup" << endl;

            db_env_set_direct_io(cmdLine.directio);

            int r = db_env_set_toku_product_name("tokumx");
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }

            r = db_env_create(&env, 0);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }

            env->set_errcall(env, tokudb_print_error);
            env->set_errpfx(env, "TokuMX");

            const uint64_t cachesize = (cmdLine.cacheSize > 0
                                        ? (uint64_t) cmdLine.cacheSize
                                        : calculate_cachesize());
            const uint32_t bytes = cachesize % (1024L * 1024L * 1024L);
            const uint32_t gigabytes = cachesize >> 30;
            r = env->set_cachesize(env, gigabytes, bytes, 1);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            TOKULOG(1) << "cachesize set to " << gigabytes << " GB + " << bytes << " bytes."<< endl;

            // Use 10% the size of the cachetable for lock tree memory
            // if no value was specified on the command line.
            const uint64_t lock_memory = (cmdLine.locktreeMaxMemory > 0
                                          ? (uint64_t) cmdLine.locktreeMaxMemory
                                          : (cachesize / 10));
            r = env->set_lk_max_memory(env, lock_memory);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            TOKULOG(1) << "locktree max memory set to " << lock_memory << " bytes." << endl;

            const uint64_t lock_timeout = cmdLine.lockTimeout;
            r = env->set_lock_timeout(env, lock_timeout);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            TOKULOG(1) << "lock timeout set to " << lock_timeout << " milliseconds." << endl;

            const uint64_t loader_memory = cmdLine.loaderMaxMemory > 0 ?
                                           (uint64_t) cmdLine.loaderMaxMemory : 100 * 1024 * 1024;
            env->set_loader_memory_size(env, loader_memory);
            TOKULOG(1) << "loader memory size set to " << loader_memory << " bytes." << endl;

            r = env->set_default_bt_compare(env, dbt_key_compare);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }

            r = env->set_generate_row_callback_for_put(env, generate_row_for_put);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            
            r = env->set_generate_row_callback_for_del(env, generate_row_for_del);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }

            r = env->set_lock_timeout_callback(env, lock_not_granted_callback);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }

            env->change_fsync_log_period(env, cmdLine.logFlushPeriod);

            const int redzone_threshold = cmdLine.fsRedzone;
            r = env->set_redzone(env, redzone_threshold);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            TOKULOG(1) << "filesystem redzone set to " << redzone_threshold << " percent." << endl;

            const char *logDir = cmdLine.logDir.c_str();
            if (!mongoutils::str::equals(logDir, "")) {
                r = env->set_lg_dir(env, logDir);
                if (r != 0) {
                    handle_ydb_error_fatal(r);
                }
                TOKULOG(1) << "transaction log directory set to " << logDir << endl;
            }

            const char *tmpDir = cmdLine.tmpDir.c_str();
            if (!mongoutils::str::equals(tmpDir, "")) {
                r = env->set_tmp_dir(env, tmpDir);
                if (r != 0) {
                    handle_ydb_error_fatal(r);
                }
                TOKULOG(1) << "temporary bulk loader directory set to " << tmpDir << endl;
            }

            if (cmdLine.debug) {
                // The default number of bucket mutexes is 1 million, which is a nightmare for
                // valgrind's drd tool to keep track of.
                db_env_set_num_bucket_mutexes(32);
            }

            const int env_flags = DB_INIT_LOCK|DB_INIT_MPOOL|DB_INIT_TXN|DB_CREATE|DB_PRIVATE|DB_INIT_LOG|DB_RECOVER;
            const int env_mode = S_IRWXU|S_IRGRP|S_IROTH|S_IXGRP|S_IXOTH;
            r = env->open(env, dbpath.c_str(), env_flags, env_mode);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }

            const int checkpoint_period = cmdLine.checkpointPeriod;
            r = env->checkpointing_set_period(env, checkpoint_period);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            TOKULOG(1) << "checkpoint period set to " << checkpoint_period << " seconds." << endl;

            const int cleaner_period = cmdLine.cleanerPeriod;
            r = env->cleaner_set_period(env, cleaner_period);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            TOKULOG(1) << "cleaner period set to " << cleaner_period << " seconds." << endl;

            const int cleaner_iterations = cmdLine.cleanerIterations;
            r = env->cleaner_set_iterations(env, cleaner_iterations);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            TOKULOG(1) << "cleaner iterations set to " << cleaner_iterations << "." << endl;
        }

        void shutdown(void) {
            tokulog() << "shutdown" << endl;
            // It's possible for startup to fail before storage::startup() is called
            if (env != NULL) {
                int r = env->close(env, 0);
                if (r != 0) {
                    handle_ydb_error_fatal(r);
                }
            }
        }

        void db_remove(const string &name) {
            int r = env->dbremove(env, cc().txn().db_txn(), name.c_str(), NULL, 0);
            if (r == ENOENT) {
                uasserted(16444, "TODO: dbremove bug, should crash but won't right now");
            }
            if (r != 0) {
                handle_ydb_error(r);
            }
        }

        void db_rename(const string &oldIdxNS, const string &newIdxNS) {
            int r = env->dbrename(env, cc().txn().db_txn(), oldIdxNS.c_str(), NULL, newIdxNS.c_str(), 0);
            massert(16463, str::stream() << "tokumx dictionary rename failed: old " << oldIdxNS
                           << ", new " << newIdxNS << ", r = " << r,
                           r == 0);
        }

        void get_status(BSONObjBuilder &status) {
            uint64_t num_rows;
            uint64_t max_rows;
            uint64_t panic;
            size_t panic_string_len = 128;
            char panic_string[panic_string_len];
            fs_redzone_state redzone_state;

            int r = storage::env->get_engine_status_num_rows(storage::env, &max_rows);
            if (r != 0) {
                handle_ydb_error(r);
            }
            TOKU_ENGINE_STATUS_ROW_S mystat[max_rows];
            r = env->get_engine_status(env, mystat, max_rows, &num_rows, &redzone_state, &panic, panic_string, panic_string_len, TOKU_ENGINE_STATUS);
            if (r != 0) {
                handle_ydb_error(r);
            }
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
                case FS_STATE:
                case UINT64:
                    status.appendNumber( row->keyname, (long long) row->value.num );
                    break;
                case CHARSTR:
                    status.append( row->keyname, row->value.str );
                    break;
                case UNIXTIME:
                    {
                        time_t t = row->value.num;
                        char tbuf[26];
                        char *tstr = ctime_r(&t, tbuf);
                        verify(tstr != NULL);
                        // Remove any trailing newline.
                        size_t len = strlen(tstr);
                        if (len > 0 && tstr[len - 1] == '\n') {
                            tstr[len - 1] = '\0';
                        }
                        status.append( row->keyname, tstr );
                    }
                    break;
                case TOKUTIME:
                    status.appendNumber( row->keyname, tokutime_to_seconds(row->value.num) );
                    break;
                case PARCOUNT:
                    {
                        uint64_t v = read_partitioned_counter(row->value.parcount);
                        status.appendNumber( row->keyname, (long long) v );
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

        static BSONObj pretty_key(const DBT *key, DB *db) {
            BSONObjBuilder b;
            const Key sKey(key);
            const DBT *desc = (db != NULL && db->cmp_descriptor != NULL)
                              ? &db->cmp_descriptor->dbt
                              : NULL;
            if (desc != NULL && desc->data != NULL && desc->size > 0) {
                Descriptor descriptor(reinterpret_cast<const char *>(desc->data), desc->size);
                const BSONObj key = sKey.key();

                // Use the descriptor to match key parts with field names
                vector<const char *> fields;
                descriptor.fieldNames(fields);
                BSONObjIterator o(key);
                for (vector<const char *>::const_iterator i = fields.begin();
                     i != fields.end(); i++) {
                    b.appendAs(o.next(), *i);
                }
                // The primary key itself will have its value in sKey.key(),
                // but fields will be empty so nothing is in the bsonobj yet.
                const BSONObj pk = fields.empty() ? key : sKey.pk();
                if (!pk.isEmpty()) {
                    b.appendAs(pk.firstElement(), "$primaryKey");
                }
            } else {
                b.append("$key", string(reinterpret_cast<const char *>(key->data), key->size));
            }
            return b.obj();
        }

        static const char *get_index_name(DB *db) {
            if (db != NULL) {
                return db->get_dname(db);
            } else {
                return "$ydb_internal";
            }
        }

        static void pretty_bounds(DB *db, const DBT *left_key, const DBT *right_key,
                                  BSONArrayBuilder &bounds) {
            if (left_key->data == NULL) {
                bounds.append("-infinity");
            } else {
                bounds.append(pretty_key(left_key, db));
            }

            if (right_key->data == NULL) {
                bounds.append("+infinity");
            } else {
                bounds.append(pretty_key(right_key, db));
            }
        }

        static void lock_not_granted_callback(DB *db, uint64_t requesting_txnid,
                                              const DBT *left_key, const DBT *right_key,
                                              uint64_t blocking_txnid) {
            CurOp *op = cc().curop();
            if (op != NULL) {
                BSONObjBuilder info;
                info.append("index", get_index_name(db));
                info.appendNumber("requestingTxnid", requesting_txnid);
                info.appendNumber("blockingTxnid", blocking_txnid);
                BSONArrayBuilder bounds(info.subarrayStart("bounds"));
                pretty_bounds(db, left_key, right_key, bounds);
                bounds.done();
                op->debug().lockNotGrantedInfo = info.obj();
            }
        }

        void get_pending_lock_request_status(BSONObjBuilder &status) {
            struct iterate_lock_requests : public ExceptionSaver {
                iterate_lock_requests() { }
                static int callback(DB *db, uint64_t requesting_txnid,
                                    const DBT *left_key, const DBT *right_key,
                                    uint64_t blocking_txnid, uint64_t start_time,
                                    void *extra) {
                    iterate_lock_requests *info = reinterpret_cast<iterate_lock_requests *>(extra);
                    try {
                        if (info->array.len() + left_key->size + right_key->size > BSONObjMaxUserSize - 1024) {
                            // We're running out of space, better stop here.
                            info->array.append("too many results to return");
                            return ERANGE;
                        }
                        BSONObjBuilder status(info->array.subobjStart());
                        status.append("index", get_index_name(db));
                        status.appendNumber("requestingTxnid", requesting_txnid);
                        status.appendNumber("blockingTxnid", blocking_txnid);
                        status.appendDate("started", start_time);
                        {
                            BSONArrayBuilder bounds(status.subarrayStart("bounds"));
                            pretty_bounds(db, left_key, right_key, bounds);
                            bounds.done();
                        }
                        status.done();
                        return 0;
                    } catch (const std::exception &ex) {
                        info->saveException(ex);
                    }
                    return -1;
                }
                BSONArrayBuilder array;
            } e;
            const int r = env->iterate_pending_lock_requests(env, iterate_lock_requests::callback, &e);
            if (r != 0 && r != ERANGE) {
                e.throwException();
                handle_ydb_error(r);
            }
            status.appendArray("requests", e.array.done());
        }

        void get_live_transaction_status(BSONObjBuilder &status) {
            struct iterate_transactions : public ExceptionSaver {
                iterate_transactions() { }
                static int callback(uint64_t txnid, uint64_t client_id,
                                    iterate_row_locks_callback iterate_locks,
                                    void *locks_extra, void *extra) {
                    iterate_transactions *info = reinterpret_cast<iterate_transactions *>(extra);
                    try {
                        // We ignore client_id because txnid is sufficient for finding
                        // the associated operation in db.currentOp()
                        BSONObjBuilder status(info->array.subobjStart());
                        status.appendNumber("txnid", txnid);
                        BSONArrayBuilder locks(status.subarrayStart("rowLocks"));
                        {
                            DB *db;
                            DBT left_key, right_key;
                            while (iterate_locks(&db, &left_key, &right_key, locks_extra) == 0) {
                                if (locks.len() + left_key.size + right_key.size > BSONObjMaxUserSize - 1024) {
                                    // We're running out of space, better stop here.
                                    locks.append("too many results to return");
                                    break;
                                }
                                BSONObjBuilder row_lock(locks.subobjStart());
                                row_lock.append("index", get_index_name(db));
                                BSONArrayBuilder bounds(row_lock.subarrayStart("bounds"));
                                pretty_bounds(db, &left_key, &right_key, bounds);
                                bounds.done();
                                row_lock.done();
                            }
                            locks.done();
                        }
                        status.done();
                        if (info->array.len() > BSONObjMaxUserSize - 1024) {
                            // We're running out of space, better stop here.
                            locks.append("too many results to return");
                            return ERANGE;
                        }
                        return 0;
                    } catch (const std::exception &ex) {
                        info->saveException(ex);
                    }
                    return -1;
                }
                BSONArrayBuilder array;
            } e;
            const int r = env->iterate_live_transactions(env, iterate_transactions::callback, &e);
            if (r != 0 && r != ERANGE) {
                e.throwException();
                handle_ydb_error(r);
            }
            status.appendArray("transactions", e.array.done());

        }

        void log_flush() {
            // Flush the recovery log to disk, ensuring crash safety up until
            // the most recently committed transaction's LSN.
            int r = env->log_flush(env, NULL);
            if (r != 0) {
                handle_ydb_error(r);
            }
        }

        void checkpoint() {
            // Run a checkpoint. The zeros mean nothing (bdb-API artifacts).
            int r = env->txn_checkpoint(env, 0, 0, 0);
            if (r != 0) {
                handle_ydb_error(r);
            }
        }

        void set_log_flush_interval(uint32_t period_ms) {
            cmdLine.logFlushPeriod = period_ms;
            env->change_fsync_log_period(env, cmdLine.logFlushPeriod);
            TOKULOG(1) << "fsync log period set to " << period_ms << " milliseconds." << endl;
        }

        void set_checkpoint_period(uint32_t period_seconds) {
            cmdLine.checkpointPeriod = period_seconds;
            int r = env->checkpointing_set_period(env, period_seconds);
            if (r != 0) {
                handle_ydb_error(r);
            }
            TOKULOG(1) << "checkpoint period set to " << period_seconds << " seconds." << endl;
        }

        void set_cleaner_period(uint32_t period_seconds) {
            cmdLine.cleanerPeriod = period_seconds;
            int r = env->cleaner_set_period(env, period_seconds);
            if (r != 0) {
                handle_ydb_error(r);
            }
            TOKULOG(1) << "cleaner period set to " << period_seconds << " seconds." << endl;
        }

        void set_cleaner_iterations(uint32_t num_iterations) {
            cmdLine.cleanerPeriod = num_iterations;
            int r = env->cleaner_set_iterations(env, num_iterations);
            if (r != 0) {
                handle_ydb_error(r);
            }
            TOKULOG(1) << "cleaner iterations set to " << num_iterations << "." << endl;
        }

        void set_lock_timeout(uint64_t timeout_ms) {
            cmdLine.lockTimeout = timeout_ms;
            int r = env->set_lock_timeout(env, timeout_ms);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            TOKULOG(1) << "lock timeout set to " << timeout_ms << " milliseconds." << endl;
        }

        void set_loader_max_memory(uint64_t bytes) {
            cmdLine.loaderMaxMemory = bytes;
            env->set_loader_memory_size(env, bytes);
            TOKULOG(1) << "loader max memory set to " << bytes << "." << endl;
        }

        void handle_ydb_error(int error) {
            switch (error) {
                case ENOENT:
                    throw SystemException::Enoent();
                case ENAMETOOLONG:
                    throw UserException(16917, "Index name too long (must be shorter than the filesystem's max path)");
                case ASSERT_IDS::AmbiguousFieldNames:
                    uasserted( storage::ASSERT_IDS::AmbiguousFieldNames,
                               mongoutils::str::stream() << "Ambiguous field name found in array" );
                case ASSERT_IDS::CannotHashArrays:
                    uasserted( storage::ASSERT_IDS::CannotHashArrays,
                               "Error: hashed indexes do not currently support array values" );
                default:
                    // fall through
                    ;
            }
            if (error > 0) {
                throw SystemException(error, 16770, "You may have hit a bug. Check the error log for more details.");
            }
            switch (error) {
                case DB_LOCK_NOTGRANTED:
                    throw LockException(16759, "Lock not granted. Try restarting the transaction.");
                case DB_LOCK_DEADLOCK:
                    throw LockException(storage::ASSERT_IDS::LockDeadlock,
                                        "Deadlock detected during lock acquisition. Try restarting the transaction.");
                case DB_KEYEXIST:
                    throw UserException(ASSERT_ID_DUPKEY, "E11000 duplicate key error.");
                case DB_NOTFOUND:
                    throw UserException(16761, "Index key not found.");
                case DB_RUNRECOVERY:
                    throw DataCorruptionException(16762, "Automatic environment recovery failed.");
                case DB_BADFORMAT:
                    throw DataCorruptionException(16763, "File-format error when reading dictionary from disk.");
                case TOKUDB_BAD_CHECKSUM:
                    throw DataCorruptionException(16764, "Checksum mismatch when reading dictionary from disk.");
                case TOKUDB_NEEDS_REPAIR:
                    throw DataCorruptionException(16765, "Repair requested when reading dictionary from disk.");
                case TOKUDB_DICTIONARY_NO_HEADER:
                    throw DataCorruptionException(16766, "No header found when reading dictionary from disk.");
                case TOKUDB_MVCC_DICTIONARY_TOO_NEW:
                    throw RetryableException::MvccDictionaryTooNew();
                case TOKUDB_HUGE_PAGES_ENABLED:
                    LOG(LL_ERROR) << endl << endl
                                  << "************************************************************" << endl
                                  << "                                                            " << endl
                                  << "                        @@@@@@@@@@@                         " << endl
                                  << "                      @@'         '@@                       " << endl
                                  << "                     @@    _     _  @@                      " << endl
                                  << "                     |    (.)   (.)  |                      " << endl
                                  << "                     |             ` |                      " << endl
                                  << "                     |        >    ' |                      " << endl
                                  << "                     |     .----.    |                      " << endl
                                  << "                     ..   |.----.|  ..                      " << endl
                                  << "                      ..  '      ' ..                       " << endl
                                  << "                        .._______,.                         " << endl
                                  << "                                                            " << endl
                                  << " TokuMX will not run with transparent huge pages enabled.   " << endl
                                  << " Please disable them to continue.                           " << endl
                                  << " (echo never > /sys/kernel/mm/transparent_hugepage/enabled) " << endl
                                  << "                                                            " << endl
                                  << " The assertion failure you are about to see is intentional. " << endl
                                  << "************************************************************" << endl
                                  << endl;
                    verify(false);
                default: 
                {
                    string s = str::stream() << "Unhandled ydb error: " << error;
                    throw MsgAssertionException(16767, s);
                }
            }
        }

        NOINLINE_DECL void handle_ydb_error_fatal(int error) {
            try {
                handle_ydb_error(error);
            }
            catch (UserException &e) {
                problem() << "fatal error " << e.getCode() << ": " << e.what() << endl;
                problem() << e << endl;
                fassertFailed(e.getCode());
            }
            catch (MsgAssertionException &e) {
                problem() << "fatal error " << e.getCode() << ": " << e.what() << endl;
                problem() << e << endl;
                fassertFailed(e.getCode());
            }                
            problem() << "No storage exception thrown but one should have been thrown for error " << error << endl;
            fassertFailed(16853);
        }
    
    } // namespace storage

} // namespace mongo
