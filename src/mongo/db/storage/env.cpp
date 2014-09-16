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
#include <string.h>
#include <sys/time.h>
#include <sys/resource.h>

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
#include "mongo/db/commands/server_status.h"
#include "mongo/db/descriptor.h"
#include "mongo/db/server_parameters.h"
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
        bool _inStartup;

        UpdateCallback *_updateCallback;

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

        static BSONObj pretty_key(const DBT *key, DB *db);

        static void runUpdateMods(DB *db, const DBT *key, const DBT *old_val, const BSONObj& updateObj, const BSONObj& query, const uint32_t fastUpdateFlags,
                                   void (*set_val)(const DBT *new_val, void *set_extra),
                                   void *set_extra) {
            BSONObj oldObj;
            if (old_val && old_val->data) {
                oldObj = BSONObj(reinterpret_cast<char *>(old_val->data));
            }
            // Apply the update mods            
            BSONObj newObj;
            bool setVal = _updateCallback->applyMods(oldObj, updateObj, query, fastUpdateFlags, newObj);
            // Set the new value
            if (setVal) {
                DBT new_val = dbt_make(newObj.objdata(), newObj.objsize());
                set_val(&new_val, set_extra);
            }
        }

        static int update_callback(DB *db, const DBT *key, const DBT *old_val, const DBT *extra,
                                   void (*set_val)(const DBT *new_val, void *set_extra),
                                   void *set_extra) {
            try {
                verify(_updateCallback != NULL);
                verify(key != NULL && extra != NULL && extra->data != NULL);
                const BSONObj msg(static_cast<char *>(extra->data));
                const char* type = msg[ "t" ].valuestrsafe();
                // right now, we only support one type of message, an updateMods
                uassert(17313, str::stream() << "unknown type of update message, type: " << type << " message: " << msg, strcmp(type, "u") == 0);
                const BSONObj updateObj = msg["o"].Obj();
                BSONElement queryElement = msg["q"];
                const BSONObj query = queryElement.ok() ? queryElement.Obj() : BSONObj();
                const uint32_t fastUpdateFlags = msg["f"].Int();
                runUpdateMods(db, key, old_val, updateObj, query, fastUpdateFlags, set_val, set_extra);
                return 0;
            } catch (const std::exception &ex) {
                problem() << "Caught exception in ydb update callback, ex: " << ex.what()
                          << "key: " << (key != NULL ? pretty_key(key, db) : BSONObj())
                          << "oldObj: " << (old_val != NULL ? BSONObj(static_cast<char *>(old_val->data)) : BSONObj())
                          << "msg: " << (extra != NULL ? BSONObj(static_cast<char *>(extra->data)) : BSONObj())
                          << endl;
                fassertFailed(17215);
            }
            return -1;
        }

        static int generate_keys(DB *dest_db, DB *src_db,
                                 DBT_ARRAY *dest_keys,
                                 const DBT *src_key, const DBT *src_val) {
            // For indexers, loaders, and any time during recovery, keys are not
            // already generated by the caller. Outside of these cases, we can
            // stop here because the keys were already generated
            //
            // See CollectionBase member functions:
            // - insertIntoIndexes
            // - deleteFromIndexes
            // - updateObject
            //
            // We know we may be in recovery if the _isStartup bit is set. We know
            // we're being called for an indexer or a loader if the dest_db has
            // a non-null app private member, because indexers and loaders must
            // utilize MultiKeyTrackers which set app private:
            //
            // See:
            // - MultiKeyTracker()
            // - ~MultiKeyTracker()
            if (!_inStartup && dest_db->app_private == NULL) {
                return 0;
            }
            try {
                const DBT *desc = &dest_db->cmp_descriptor->dbt;
                scoped_ptr<Descriptor> descriptor;
                Descriptor::getFromDBT(desc, descriptor);

                const Key sPK(src_key);
                dassert(sPK.pk().isEmpty());
                const BSONObj pk(sPK.key());
                const BSONObj obj(reinterpret_cast<const char *>(src_val->data));

                // The ydb knows that src_db does not need keys generated,
                // because the one and only key is src_key
                verify(dest_db != src_db);

                // Generate keys for a secondary index.
                BSONObjSet keys;
                descriptor->generateKeys(obj, keys);
                dbt_array_clear_and_resize(dest_keys, keys.size());
                for (BSONObjSet::const_iterator i = keys.begin(); i != keys.end(); i++) {
                    const Key sKey(*i, &pk);
                    dbt_array_push(dest_keys, sKey.buf(), sKey.size());
                }
                // Set the multiKey bool if it's provided and we generated multiple keys.
                // See CollectionBase::IndexerBase::Indexer()
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
            // We may be calling this from a crashing state, so we should use rawOut.
            rawOut(buffer);
        }

        // Called by the ydb to determine how long a txn should sleep on a lock.
        // For now, it's always the command-line specified timeout, but we could
        // make it a per-thread variable in the future.
        static uint64_t get_lock_timeout_callback(uint64_t default_timeout) {
            if (haveClient()) {
                return cc().lockTimeout();
            } else {
                return cmdLine.lockTimeout;
            }
        }

        static uint64_t get_loader_memory_size_callback(void) {
            return cmdLine.loaderMaxMemory > 0 ?
                (uint64_t) cmdLine.loaderMaxMemory : 100 * 1024 * 1024;
        }

        static void lock_not_granted_callback(DB *db, uint64_t requesting_txnid,
                                              const DBT *left_key, const DBT *right_key,
                                              uint64_t blocking_txnid);

        struct InStartup {
            InStartup() { _inStartup = true; }
            ~InStartup() { _inStartup = false; }
        };

        MONGO_EXPORT_STARTUP_SERVER_PARAMETER(numCachetableBucketMutexes, uint32_t, 0);

        void startup(TxnCompleteHooks *hooks, UpdateCallback *updateCallback) {
            InStartup is;

            setTxnCompleteHooks(hooks);
            _updateCallback = updateCallback;

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
            if (cachesize < 1ULL<<30) {
                warning() << "*****************************" << endl;
                warning() << "cacheSize set to less than 1 GB: " << cachesize << " bytes. " << endl;
                warning() << "This value may be too low to achieve good performance." << endl;
                warning() << "*****************************" << endl;
            }
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
            r = env->set_lock_timeout(env, lock_timeout, get_lock_timeout_callback);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            TOKULOG(1) << "lock timeout set to " << lock_timeout << " milliseconds." << endl;

            env->set_loader_memory_size(env, get_loader_memory_size_callback);
            TOKULOG(1) << "loader memory size set to " << get_loader_memory_size_callback() << " bytes." << endl;

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
            env->set_update(env, update_callback);

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

            if (numCachetableBucketMutexes > 0) {
                // The default number of bucket mutexes is 1 million, which is a nightmare for
                // valgrind's drd tool to keep track of.
                db_env_set_num_bucket_mutexes(numCachetableBucketMutexes);
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

        class FractalTreeEngineStatus {
            uint64_t _num_rows;
            uint64_t _max_rows;
            uint64_t _panic;
            static const size_t _panic_string_len = 128;
            char _panic_string[_panic_string_len];
            fs_redzone_state _redzone_state;
            scoped_array<TOKU_ENGINE_STATUS_ROW_S> _rows;

          public:
            FractalTreeEngineStatus()
                    : _num_rows(0),
                      _max_rows(0),
                      _panic(0),
                      _redzone_state(FS_GREEN),
                      _rows(NULL) {}

            void fetch() {
                int r = storage::env->get_engine_status_num_rows(storage::env, &_max_rows);
                if (r != 0) {
                    handle_ydb_error(r);
                }
                _rows.reset(new TOKU_ENGINE_STATUS_ROW_S[_max_rows]);
                r = env->get_engine_status(env, _rows.get(), _max_rows, &_num_rows, &_redzone_state, &_panic, _panic_string, _panic_string_len, TOKU_ENGINE_STATUS);
                if (r != 0) {
                    handle_ydb_error(r);
                }
            }

            void appendPanic(BSONObjBuilder &result) const {
                result.append("panic code", (long long) _panic);
                result.append("panic string", _panic_string);
            }

            void appendFilesystem(BSONObjBuilder &result) const {
                switch (_redzone_state) {
                    case FS_GREEN:
                        result.append("filesystem status", "OK");
                        break;
                    case FS_YELLOW:
                        result.append("filesystem status", "Getting full...");
                        break;
                    case FS_RED:
                        result.append("filesystem status", "Critically full. Engine is read-only until space is freed.");
                        break;
                    case FS_BLOCKED:
                        result.append("filesystem status", "Completely full. Free up some space now.");
                        break;
                    default:
                        {
                            StringBuilder s;
                            s << "Unknown. Code: " << (int) _redzone_state;
                            result.append("filesystem status", s.str());
                        }
                }
            }

            static void appendRow(BSONObjBuilder &result, const StringData &field, TOKU_ENGINE_STATUS_ROW row, int scale = 1) {
                switch (row->type) {
                    case FS_STATE:
                    case UINT64:
                        result.appendNumber(field, (long long) row->value.num / scale);
                        break;
                    case CHARSTR:
                        {
                            StringData s(row->value.str);
                            result.append(field, s);
                        }
                        break;
                    case UNIXTIME:
                        {
                            time_t t = row->value.num;
                            result.appendTimeT(field, t);
                        }
                        break;
                    case TOKUTIME:
                        result.appendNumber(field, tokutime_to_seconds(row->value.num));
                        break;
                    case PARCOUNT:
                        {
                            uint64_t v = read_partitioned_counter(row->value.parcount);
                            result.appendNumber(field, (long long) v / scale);
                        }
                        break;
                    case DOUBLE:
                        {
                            double v = row->value.dnum;
                            result.appendNumber(field, v);
                        }
                        break;
                    default:
                        {
                            StringBuilder s;
                            s << "Unknown type. Code: " << (int) row->type;
                            result.append(field, s.str());
                        }
                        break;                
                }
            }

            void appendInfo(BSONObjBuilder &result) {
                appendPanic(result);
                appendFilesystem(result);
                for (uint64_t i = 0; i < _num_rows; ++i) {
                    appendRow(result, _rows[i].keyname, &_rows[i]);
                }
            }

            void appendInfo(BSONObjBuilder &result, const StringData &field, const StringData &key, int scale = 1) const {
                // well, this is annoying
                for (uint64_t i = 0; i < _num_rows; ++i) {
                    if (key == _rows[i].keyname) {
                        appendRow(result, field, &_rows[i], scale);
                        break;
                    }
                }
            }

            uint64_t getInteger(const StringData &key) const {
                for (uint64_t i = 0; i < _num_rows; ++i) {
                    TOKU_ENGINE_STATUS_ROW row = &_rows[i];
                    if (key == row->keyname) {
                        switch (row->type) {
                        case FS_STATE:
                        case UINT64:
                            return row->value.num;
                        case PARCOUNT:
                            return read_partitioned_counter(row->value.parcount);
                        case DOUBLE:
                        case CHARSTR:
                        case UNIXTIME:
                        case TOKUTIME:
                            msgasserted(17289, "wrong engine status type for getInteger");
                        }
                    }
                }
                msgasserted(17290, mongoutils::str::stream() << "no such key: " << key);
            }

            double getDuration(const StringData &key) const {
                for (uint64_t i = 0; i < _num_rows; ++i) {
                    TOKU_ENGINE_STATUS_ROW row = &_rows[i];
                    if (key == row->keyname) {
                        switch (row->type) {
                        case TOKUTIME:
                            return tokutime_to_seconds(row->value.num);
                        case UNIXTIME:
                            return static_cast<double>(*reinterpret_cast<const time_t *>(&row->value.num));
                        case DOUBLE:
                        case CHARSTR:
                        case FS_STATE:
                        case UINT64:
                        case PARCOUNT:
                            msgasserted(17291, "wrong engine status type for getDuration");
                        }
                    }
                }
                msgasserted(17292, mongoutils::str::stream() << "no such key: " << key);
            }
        };

        void get_status(BSONObjBuilder &result) {
            FractalTreeEngineStatus status;
            status.fetch();
            status.appendInfo(result);
        }

        class NestedBuilder : boost::noncopyable {
          public:
            class Stack : public std::stack<BSONObjBuilder *> {
                BSONObjBuilder _bottom;
              public:
                Stack() {
                    push(&_bottom);
                }
                BSONObjBuilder &b() { return *top(); }
                operator BSONObjBuilder&() { return b(); }
            };

            NestedBuilder(Stack &stack, const StringData &name)
                    : _stack(stack), _b(_stack.b().subobjStart(name))
            {
                _stack.push(&_b);
            }

            ~NestedBuilder() {
                _stack.pop();
                _b.doneFast();
            }

          private:
            Stack &_stack;
            BSONObjBuilder _b;
        };

        class FractalTreeSSS : public ServerStatusSection {
          public:
            FractalTreeSSS() : ServerStatusSection("ft") {}
            virtual bool includeByDefault() const { return true; }

            BSONObj generateSection(const BSONElement &configElement) const {
                if (cmdLine.isMongos()) {
                    return BSONObj();
                }

                int scale = 1;
                if (configElement.isABSONObj()) {
                    BSONObj o = configElement.Obj();
                    BSONElement scaleElt = o["scale"];
                    if (scaleElt.ok()) {
                        scale = scaleElt.safeNumberLong();
                    }
                }

                NestedBuilder::Stack result;

                FractalTreeEngineStatus status;
                status.fetch();

                {
                    NestedBuilder _n1(result, "fsync");
                    status.appendInfo(result, "count", "FS_FSYNC_COUNT");
                    status.appendInfo(result, "time", "FS_FSYNC_TIME");
                }
                {
                    NestedBuilder _n1(result, "log");
                    status.appendInfo(result, "count", "LOGGER_NUM_WRITES");
                    status.appendInfo(result, "time", "LOGGER_TOKUTIME_WRITES");
                    status.appendInfo(result, "bytes", "LOGGER_BYTES_WRITTEN", scale);
                }
                {
                    NestedBuilder _n1(result, "cachetable");
                    {
                        NestedBuilder _n2(result, "size");
                        status.appendInfo(result, "current", "CT_SIZE_CURRENT", scale);
                        status.appendInfo(result, "writing", "CT_SIZE_WRITING", scale);
                        status.appendInfo(result, "limit", "CT_SIZE_LIMIT", scale);
                    }
                    {
                        NestedBuilder _n2(result, "miss");
                        uint64_t fullMisses = status.getInteger("CT_MISS");
                        // unfortunately, this is a uint64 when it's actually a tokutime...
                        double fullMisstime = tokutime_to_seconds(status.getInteger("CT_MISSTIME"));
                        uint64_t partialMisses = 0;
                        double partialMisstime = 0.0;
                        const char *partialMissKeys[] = {"FT_NUM_BASEMENTS_FETCHED_NORMAL",
                                                         "FT_NUM_BASEMENTS_FETCHED_AGGRESSIVE",
                                                         "FT_NUM_BASEMENTS_FETCHED_PREFETCH",
                                                         "FT_NUM_BASEMENTS_FETCHED_WRITE",
                                                         "FT_NUM_MSG_BUFFER_FETCHED_NORMAL",
                                                         "FT_NUM_MSG_BUFFER_FETCHED_AGGRESSIVE",
                                                         "FT_NUM_MSG_BUFFER_FETCHED_PREFETCH",
                                                         "FT_NUM_MSG_BUFFER_FETCHED_WRITE"};
                        const char *partialMisstimeKeys[] = {"FT_TOKUTIME_BASEMENTS_FETCHED_NORMAL",
                                                             "FT_TOKUTIME_BASEMENTS_FETCHED_AGGRESSIVE",
                                                             "FT_TOKUTIME_BASEMENTS_FETCHED_PREFETCH",
                                                             "FT_TOKUTIME_BASEMENTS_FETCHED_WRITE",
                                                             "FT_TOKUTIME_MSG_BUFFER_FETCHED_NORMAL",
                                                             "FT_TOKUTIME_MSG_BUFFER_FETCHED_AGGRESSIVE",
                                                             "FT_TOKUTIME_MSG_BUFFER_FETCHED_PREFETCH",
                                                             "FT_TOKUTIME_MSG_BUFFER_FETCHED_WRITE"};
                        dassert((sizeof partialMissKeys) == (sizeof partialMisstimeKeys));
                        for (size_t i = 0; i < (sizeof partialMissKeys) / (sizeof partialMissKeys[0]); ++i) {
                            partialMisses += status.getInteger(partialMissKeys[i]);
                            partialMisstime += status.getDuration(partialMisstimeKeys[i]);
                        }

                        result.b().append("count", fullMisses + partialMisses);
                        result.b().append("time", fullMisstime + partialMisstime);
                        {
                            NestedBuilder _n3(result, "full");
                            result.b().append("count", fullMisses);
                            result.b().append("time", fullMisstime);
                        }
                        {
                            NestedBuilder _n3(result, "partial");
                            result.b().append("count", partialMisses);
                            result.b().append("time", partialMisstime);
                        }
                    }
                    {
                        NestedBuilder _n2(result, "evictions");
                        {
                            NestedBuilder _n3(result, "partial");
                            {
                                NestedBuilder _n4(result, "nonleaf");
                                {
                                    NestedBuilder _n5(result, "clean");
                                    status.appendInfo(result, "count", "FT_PARTIAL_EVICTIONS_NONLEAF");
                                    status.appendInfo(result, "bytes", "FT_PARTIAL_EVICTIONS_NONLEAF_BYTES", scale);
                                }
                            }
                            {
                                NestedBuilder _n4(result, "leaf");
                                {
                                    NestedBuilder _n5(result, "clean");
                                    status.appendInfo(result, "count", "FT_PARTIAL_EVICTIONS_LEAF");
                                    status.appendInfo(result, "bytes", "FT_PARTIAL_EVICTIONS_LEAF_BYTES", scale);
                                }
                            }
                        }
                        {
                            NestedBuilder _n3(result, "full");
                            {
                                NestedBuilder _n4(result, "nonleaf");
                                {
                                    NestedBuilder _n5(result, "clean");
                                    status.appendInfo(result, "count", "FT_FULL_EVICTIONS_NONLEAF");
                                    status.appendInfo(result, "bytes", "FT_FULL_EVICTIONS_NONLEAF_BYTES", scale);
                                }
                                {
                                    NestedBuilder _n5(result, "dirty");
                                    status.appendInfo(result, "count", "FT_DISK_FLUSH_NONLEAF");
                                    status.appendInfo(result, "bytes", "FT_DISK_FLUSH_NONLEAF_UNCOMPRESSED_BYTES", scale);
                                    status.appendInfo(result, "time", "FT_DISK_FLUSH_NONLEAF_TOKUTIME");
                                }
                            }
                            {
                                NestedBuilder _n4(result, "leaf");
                                {
                                    NestedBuilder _n5(result, "clean");
                                    status.appendInfo(result, "count", "FT_FULL_EVICTIONS_LEAF");
                                    status.appendInfo(result, "bytes", "FT_FULL_EVICTIONS_LEAF_BYTES", scale);
                                }
                                {
                                    NestedBuilder _n5(result, "dirty");
                                    status.appendInfo(result, "count", "FT_DISK_FLUSH_LEAF");
                                    status.appendInfo(result, "bytes", "FT_DISK_FLUSH_LEAF_UNCOMPRESSED_BYTES", scale);
                                    status.appendInfo(result, "time", "FT_DISK_FLUSH_LEAF_TOKUTIME");
                                }
                            }
                        }
                    }
                }
                {
                    NestedBuilder _n1(result, "checkpoint");
                    status.appendInfo(result, "count", "CP_CHECKPOINT_COUNT");
                    status.appendInfo(result, "time", "CP_TIME_CHECKPOINT_DURATION");
                    status.appendInfo(result, "lastBegin", "CP_TIME_LAST_CHECKPOINT_BEGIN");
                    {
                        NestedBuilder _n2(result, "lastComplete");
                        status.appendInfo(result, "begin", "CP_TIME_LAST_CHECKPOINT_BEGIN_COMPLETE");
                        status.appendInfo(result, "end", "CP_TIME_LAST_CHECKPOINT_END");
                        status.appendInfo(result, "time", "CP_TIME_CHECKPOINT_DURATION_LAST");
                    }
                    {
                        NestedBuilder _n2(result, "begin");
                        status.appendInfo(result, "time", "CP_BEGIN_TIME");
                    }
                    {
                        NestedBuilder _n2(result, "write");
                        {
                            NestedBuilder _n3(result, "nonleaf");
                            status.appendInfo(result, "count", "FT_DISK_FLUSH_NONLEAF_FOR_CHECKPOINT");
                            status.appendInfo(result, "time", "FT_DISK_FLUSH_NONLEAF_TOKUTIME_FOR_CHECKPOINT");
                            {
                                NestedBuilder _n4(result, "bytes");
                                status.appendInfo(result, "uncompressed", "FT_DISK_FLUSH_NONLEAF_UNCOMPRESSED_BYTES_FOR_CHECKPOINT", scale);
                                status.appendInfo(result, "compressed", "FT_DISK_FLUSH_NONLEAF_BYTES_FOR_CHECKPOINT", scale);
                            }
                        }
                        {
                            NestedBuilder _n3(result, "leaf");
                            status.appendInfo(result, "count", "FT_DISK_FLUSH_LEAF_FOR_CHECKPOINT");
                            status.appendInfo(result, "time", "FT_DISK_FLUSH_LEAF_TOKUTIME_FOR_CHECKPOINT");
                            {
                                NestedBuilder _n4(result, "bytes");
                                status.appendInfo(result, "uncompressed", "FT_DISK_FLUSH_LEAF_UNCOMPRESSED_BYTES_FOR_CHECKPOINT", scale);
                                status.appendInfo(result, "compressed", "FT_DISK_FLUSH_LEAF_BYTES_FOR_CHECKPOINT", scale);
                            }
                        }
                    }
                }
                {
                    NestedBuilder _n1(result, "serializeTime");
                    {
                        NestedBuilder _n2(result, "nonleaf");
                        status.appendInfo(result, "serialize", "FT_NONLEAF_SERIALIZE_TOKUTIME");
                        status.appendInfo(result, "compress", "FT_NONLEAF_COMPRESS_TOKUTIME");
                        status.appendInfo(result, "decompress", "FT_NONLEAF_DECOMPRESS_TOKUTIME");
                        status.appendInfo(result, "deserialize", "FT_NONLEAF_DESERIALIZE_TOKUTIME");
                    }
                    {
                        NestedBuilder _n2(result, "leaf");
                        status.appendInfo(result, "serialize", "FT_LEAF_SERIALIZE_TOKUTIME");
                        status.appendInfo(result, "compress", "FT_LEAF_COMPRESS_TOKUTIME");
                        status.appendInfo(result, "decompress", "FT_LEAF_DECOMPRESS_TOKUTIME");
                        status.appendInfo(result, "deserialize", "FT_LEAF_DESERIALIZE_TOKUTIME");
                    }
                }
                {
                    NestedBuilder _n1(result, "locktree");
                    {
                        NestedBuilder _n2(result, "size");
                        status.appendInfo(result, "current", "LTM_SIZE_CURRENT", scale);
                        status.appendInfo(result, "limit", "LTM_SIZE_LIMIT", scale);
                    }
                }
                {
                    NestedBuilder _n1(result, "compressionRatio");
                    status.appendInfo(result, "leaf", "FT_DISK_FLUSH_LEAF_COMPRESSION_RATIO");
                    status.appendInfo(result, "nonleaf", "FT_DISK_FLUSH_NONLEAF_COMPRESSION_RATIO");
                    status.appendInfo(result, "overall", "FT_DISK_FLUSH_OVERALL_COMPRESSION_RATIO");
                }
                {
                    NestedBuilder _n1(result, "alerts");
                    status.appendPanic(result);
                    status.appendFilesystem(result);
                    status.appendInfo(result, "locktreeRequestsPending", "LTM_LOCK_REQUESTS_PENDING");
                    status.appendInfo(result, "checkpointFailures", "CP_CHECKPOINT_COUNT_FAIL");
                    {
                        NestedBuilder _n2(result, "longWaitEvents");
                        status.appendInfo(result, "logBufferWait", "LOGGER_WAIT_BUF_LONG");
                        {
                            NestedBuilder _n3(result, "fsync");
                            status.appendInfo(result, "count", "FS_LONG_FSYNC_COUNT");
                            status.appendInfo(result, "time", "FS_LONG_FSYNC_TIME");
                        }
                        {
                            NestedBuilder _n3(result, "cachePressure");
                            status.appendInfo(result, "count", "CT_LONG_WAIT_PRESSURE_COUNT");
                            status.appendInfo(result, "time", "CT_LONG_WAIT_PRESSURE_TIME");
                        }
                        {
                            NestedBuilder _n3(result, "checkpointBegin");
                            status.appendInfo(result, "count", "CP_LONG_BEGIN_COUNT");
                            status.appendInfo(result, "time", "CP_LONG_BEGIN_TIME");
                        }
                        {
                            NestedBuilder _n3(result, "locktreeWait");
                            status.appendInfo(result, "count", "LTM_LONG_WAIT_COUNT");
                            status.appendInfo(result, "time", "LTM_LONG_WAIT_TIME");
                        }
                        {
                            NestedBuilder _n3(result, "locktreeWaitEscalation");
                            status.appendInfo(result, "count", "LTM_LONG_WAIT_ESCALATION_COUNT");
                            status.appendInfo(result, "time", "LTM_LONG_WAIT_ESCALATION_TIME");
                        }
                    }
                }

                return result.b().obj();
            }
        } essss;

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
                b.appendElements(descriptor.fillKeyFieldNames(key));
                // The primary key itself will have its value in sKey.key(),
                // Secondary keys will have a non-empty sKey.pk(), and
                // we'll append that pk as "$primaryKey"
                const BSONObj pk = sKey.pk();
                if (!pk.isEmpty()) {
                    b.appendAs(pk.firstElement(), "$primaryKey");
                }
            } else {
                const char *data = reinterpret_cast<const char *>(key->data);
                size_t size = key->size;
                while (data[size - 1] == '\0' && size > 0) {
                    --size;
                }
                b.append("$key", string(data, size));
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

        void get_pending_lock_request_status(vector<BSONObj> &pendingLockRequests) {
            struct iterate_lock_requests : public ExceptionSaver {
                vector<BSONObj> &_reqs;
              public:
                iterate_lock_requests(vector<BSONObj> &reqs) : _reqs(reqs) { }
                static int callback(DB *db, uint64_t requesting_txnid,
                                    const DBT *left_key, const DBT *right_key,
                                    uint64_t blocking_txnid, uint64_t start_time,
                                    void *extra) {
                    iterate_lock_requests *info = reinterpret_cast<iterate_lock_requests *>(extra);
                    try {
                        BSONObjBuilder status;
                        status.append("index", get_index_name(db));
                        status.appendNumber("requestingTxnid", requesting_txnid);
                        status.appendNumber("blockingTxnid", blocking_txnid);
                        status.appendDate("started", start_time);
                        {
                            BSONArrayBuilder bounds(status.subarrayStart("bounds"));
                            pretty_bounds(db, left_key, right_key, bounds);
                            bounds.done();
                        }
                        info->_reqs.push_back(status.obj());
                        return 0;
                    } catch (const std::exception &ex) {
                        info->saveException(ex);
                    }
                    return -1;
                }
            } e(pendingLockRequests);
            const int r = env->iterate_pending_lock_requests(env, iterate_lock_requests::callback, &e);
            if (r != 0) {
                e.throwException();
                handle_ydb_error(r);
            }
        }

        void get_live_transaction_status(vector<BSONObj> &liveTransactions) {
            class iterate_transactions : public ExceptionSaver {
                vector<BSONObj> &_txns;
              public:
                iterate_transactions(vector<BSONObj> &txns) : _txns(txns) { }
                static int callback(uint64_t txnid, uint64_t client_id,
                                    iterate_row_locks_callback iterate_locks,
                                    void *locks_extra, void *extra) {
                    iterate_transactions *info = reinterpret_cast<iterate_transactions *>(extra);
                    try {
                        // We ignore client_id because txnid is sufficient for finding
                        // the associated operation in db.currentOp()
                        BSONObjBuilder status;
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
                        info->_txns.push_back(status.obj());
                        return 0;
                    } catch (const std::exception &ex) {
                        info->saveException(ex);
                    }
                    return -1;
                }
            } e(liveTransactions);
            const int r = env->iterate_live_transactions(env, iterate_transactions::callback, &e);
            if (r != 0) {
                e.throwException();
                handle_ydb_error(r);
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

        class LogFlushPeriodParameter : public ExportedServerParameter<uint32_t> {
          public:
            LogFlushPeriodParameter() : ExportedServerParameter<uint32_t>(ServerParameterSet::getGlobal(), "logFlushPeriod", &cmdLine.logFlushPeriod, true, true) {}

          protected:
            virtual Status validate(const uint32_t& period) {
                if (static_cast<int32_t>(period) < 0 || period > 500) {
                    return Status(ErrorCodes::BadValue, "logFlushPeriod must be between 0 and 500 ms");
                }
                env->change_fsync_log_period(env, period);
                return Status::OK();
            }
        } logFlushPeriod;

        class CheckpointPeriodParameter : public ExportedServerParameter<uint32_t> {
          public:
            CheckpointPeriodParameter() : ExportedServerParameter<uint32_t>(ServerParameterSet::getGlobal(), "checkpointPeriod", &cmdLine.checkpointPeriod, true, true) {}

            virtual Status validate(const uint32_t &period) {
                if (static_cast<int32_t>(period) < 0) {
                    return Status(ErrorCodes::BadValue, "checkpointPeriod must be greater than 0s");
                }
                int r = env->checkpointing_set_period(env, period);
                if (r != 0) {
                    handle_ydb_error(r);
                    return Status(ErrorCodes::InternalError, "error setting checkpointPeriod");
                }
                return Status::OK();
            }
        } checkpointPeriodParameter;

        class CleanerPeriodParameter : public ExportedServerParameter<uint32_t> {
          public:
            CleanerPeriodParameter() : ExportedServerParameter<uint32_t>(ServerParameterSet::getGlobal(), "cleanerPeriod", &cmdLine.cleanerPeriod, true, true) {}

            virtual Status validate(const uint32_t &period) {
                if (static_cast<int32_t>(period) < 0) {
                    return Status(ErrorCodes::BadValue, "cleanerPeriod must be greater than 0s");
                }
                int r = env->cleaner_set_period(env, period);
                if (r != 0) {
                    handle_ydb_error(r);
                    return Status(ErrorCodes::InternalError, "error setting cleanerPeriod");
                }
                return Status::OK();
            }
        } cleanerPeriodParameter;

        class CleanerIterationsParameter : public ExportedServerParameter<uint32_t> {
          public:
            CleanerIterationsParameter() : ExportedServerParameter<uint32_t>(ServerParameterSet::getGlobal(), "cleanerIterations", &cmdLine.cleanerIterations, true, true) {}

            virtual Status validate(const uint32_t &iterations) {
                if (static_cast<int32_t>(iterations) < 0) {
                    return Status(ErrorCodes::BadValue, "cleanerIterations must be greater than 0");
                }
                int r = env->cleaner_set_iterations(env, iterations);
                if (r != 0) {
                    handle_ydb_error(r);
                    return Status(ErrorCodes::InternalError, "error setting cleanerIterations");
                }
                return Status::OK();
            }
        } cleanerIterationsParameter;

        class CompressBuffersBeforeEvictionParameter : public ExportedServerParameter<bool> {
            bool doCompressBuffers;
          public:
            CompressBuffersBeforeEvictionParameter() : ExportedServerParameter<bool>(ServerParameterSet::getGlobal(), "compressBuffersBeforeEviction", &doCompressBuffers, true, true) {}

            virtual Status validate(const bool &newVal) {
                doCompressBuffers = newVal;
                db_env_set_compress_buffers_before_eviction(doCompressBuffers);
                return Status::OK();
            }
        } compressBuffersBeforeEvictionParameter;

        // These do not need set functions because the ydb uses a callback
        // to read cmdLine.lockTimeout / cmdLine.loaderMaxMemory 
        ExportedServerParameter<uint64_t> lockTimeoutParameter(ServerParameterSet::getGlobal(), "lockTimeout", &cmdLine.lockTimeout, true, true);
        ExportedServerParameter<BytesQuantity<uint64_t> > loaderMaxMemoryParameter(ServerParameterSet::getGlobal(), "loaderMaxMemory", &cmdLine.loaderMaxMemory, true, true);

        __attribute__((noreturn))
        static void handle_filesystem_error_nicely(int error) {
            stringstream ss;
            ss << "Error " << error << ": " << strerror(error);
            warning() << "Got the following error from the filesystem:" << endl;
            warning() << ss.str() << endl;
#if defined(RLIMIT_NOFILE)
            if (error == EMFILE) {
                struct rlimit rlnofile;
                int r = getrlimit(RLIMIT_NOFILE, &rlnofile);
                if (r == 0) {
                    warning() << "The current resource limit for this process allows only " << rlnofile.rlim_cur
                              << " open files.  Consider raising this value." << endl;
                } else {
                    r = errno;
                    warning() << "getrlimit error " << r << ": " << strerror(r) << endl;
                }
            }
#endif
            uasserted(17035, ss.str());
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
                case EACCES:
                case EMFILE:
                case ENFILE:
                case ENOSPC:
                case EPERM:
                case EROFS: {
                    handle_filesystem_error_nicely(error);
                    verify(false);  // should not reach this point
                }
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
                case TOKUDB_UPGRADE_FAILURE:
                    LOG(LL_ERROR) << endl << endl;
                    LOG(LL_ERROR) << "************************************************************" << endl;
                    LOG(LL_ERROR) << endl;
                    LOG(LL_ERROR) << " Detected an unclean shutdown during version upgrade." << endl;
                    LOG(LL_ERROR) << " Before upgrading, you must perform a clean shutdown of the" << endl;
                    LOG(LL_ERROR) << " old version of TokuMX before starting the new version." << endl;
                    LOG(LL_ERROR) << endl;
                    LOG(LL_ERROR) << " You must go back to the old version, recover, and then" << endl;
                    LOG(LL_ERROR) << " shut down cleanly before upgrading." << endl;
                    LOG(LL_ERROR) << endl;
                    LOG(LL_ERROR) << " The assertion failure you are about to see is intentional." << endl;
                    LOG(LL_ERROR) << "************************************************************" << endl;
                    // uassert(17357, "for below SystemException");
                    throw SystemException(error, 17357, "Detected an unclean shutdown during version upgrade.");
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
    
        void do_backtrace() {
            env->do_backtrace(env);
        }

    } // namespace storage

} // namespace mongo
