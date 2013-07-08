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

#include "mongo/db/client.h"
#include "mongo/db/cmdline.h"
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
                Key key1(dbt1);
                Key key2(dbt2);
                const Ordering &ordering(*reinterpret_cast<const Ordering *>(db->cmp_descriptor->dbt.data));
                return key1.woCompare(key2, ordering);
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
                                        ? cmdLine.cacheSize
                                        : calculate_cachesize());
            const uint32_t bytes = cachesize % (1024L * 1024L * 1024L);
            const uint32_t gigabytes = cachesize >> 30;
            r = env->set_cachesize(env, gigabytes, bytes, 1);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            TOKULOG(1) << "cachesize set to " << gigabytes << " GB + " << bytes << " bytes."<< endl;

            // Use 10% the size of the cachetable for lock tree memory
            const uint64_t lock_memory = cachesize / 10;
            r = env->set_lk_max_memory(env, lock_memory);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            tokulog() << "locktree max memory set to " << lock_memory << " bytes." << endl;

            const uint64_t lock_timeout = cmdLine.lockTimeout;
            r = env->set_lock_timeout(env, lock_timeout);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            TOKULOG(1) << "lock timeout set to " << lock_timeout << " milliseconds." << endl;

            r = env->set_default_bt_compare(env, dbt_key_compare);
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

        // set a descriptor for the given dictionary. the descriptor is
        // a serialization of the index's ordering bits.
        static void set_db_descriptor(DB *db, DB_TXN *txn, const BSONObj &key_pattern) {
            const Ordering ordering = Ordering::make(key_pattern);
            DBT dbt = make_dbt((const char *) &ordering, sizeof(Ordering));
            const int flags = DB_UPDATE_CMP_DESCRIPTOR;
            int r = db->change_descriptor(db, txn, &dbt, flags);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            TOKULOG(1) << "set db " << db << " descriptor to key pattern: " << key_pattern << endl;
        }

        static void verify_db_descriptor(DB *db, const BSONObj &key_pattern) {
            verify(db->cmp_descriptor->dbt.size == sizeof(Ordering));
            const Ordering ordering = Ordering::make(key_pattern);
            const int c = memcmp(db->cmp_descriptor->dbt.data, &ordering, sizeof(Ordering));
            if (c != 0) {
                problem() << " bad db descriptor on open, key pattern " << key_pattern << endl;
            }
            verify(c == 0);
        }

        int db_open(DB **dbp, const string &name, const BSONObj &info, bool may_create) {
            // TODO: Refactor this option setting code to someplace else. It's here because
            // the YDB api doesn't allow a db->close to be called before db->open, and we
            // would leak memory if we chose to do nothing. So we validate all the
            // options here before db_create + db->open.
            int readPageSize = 65536;
            int pageSize = 4*1024*1024;
            TOKU_COMPRESSION_METHOD compression = TOKU_ZLIB_WITHOUT_CHECKSUM_METHOD;
            BSONObj key_pattern = info["key"].Obj();
            
            BSONElement e;
            e = info["readPageSize"];
            if (e.ok() && !e.isNull()) {
                readPageSize = e.numberInt();
                uassert(16743, "readPageSize must be a number > 0.", e.isNumber () && readPageSize > 0);
                TOKULOG(1) << "db " << name << ", using read page size " << readPageSize << endl;
            }
            e = info["pageSize"];
            if (e.ok() && !e.isNull()) {
                pageSize = e.numberInt();
                uassert(16445, "pageSize must be a number > 0.", e.isNumber () && pageSize > 0);
                TOKULOG(1) << "db " << name << ", using page size " << pageSize << endl;
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
                TOKULOG(1) << "db " << name << ", using compression method \"" << str << "\"" << endl;
            }

            DB *db;
            int r = db_create(&db, env, 0);
            if (r != 0) {
                handle_ydb_error(r);
            }

            r = db->set_readpagesize(db, readPageSize);
            if (r != 0) {
                handle_ydb_error(r);
            }

            r = db->set_pagesize(db, pageSize);
            if (r != 0) {
                handle_ydb_error(r);
            }

            r = db->set_compression_method(db, compression);
            if (r != 0) {
                handle_ydb_error(r);
            }

            // If this is a non-creating open for a read-only (or non-existent)
            // transaction, we can use an alternate stack since there's nothing
            // to roll back and no locktree locks to hold.
            const bool needAltTxn = !may_create && (!cc().hasTxn() || cc().txn().readOnly());
            scoped_ptr<Client::AlternateTransactionStack> altStack(!needAltTxn ? NULL :
                                                                   new Client::AlternateTransactionStack());
            scoped_ptr<Client::Transaction> altTxn(!needAltTxn ? NULL :
                                                   new Client::Transaction(0));

            const int db_flags = may_create ? DB_CREATE : 0;
            DB_TXN *txn = cc().txn().db_txn();
            r = db->open(db, txn, name.c_str(), NULL, DB_BTREE, db_flags, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
            if (r == ENOENT) {
                verify(!may_create);
                goto exit;
            }
            if (r != 0) {
                handle_ydb_error(r);
            }
            if (altTxn.get() != NULL) {
                altTxn->commit();
            }

            if (may_create) {
                set_db_descriptor(db, txn, key_pattern);
            }
            verify_db_descriptor(db, key_pattern);
            *dbp = db;
        exit:
            return r;
        }

        void db_close(DB *db) {
            int r = db->close(db, 0);
            if (r != 0) {
                handle_ydb_error(r);
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
                        status.appendNumber( row->keyname, (long long) ctime_r(&t, tbuf) );
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

        void handle_ydb_error(int error) {
            switch (error) {
                case ENOENT:
                    throw SystemException::Enoent();
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
                    throw LockException(16760, "Deadlock detected during lock acquisition. Try restarting the transaction.");
                case DB_KEYEXIST:
                    throw UserException(ASSERT_ID_DUPKEY, "Duplicate key error.");
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
