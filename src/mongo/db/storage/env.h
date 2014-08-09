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

#pragma once

#include "mongo/pch.h"
#include "mongo/bson/bsonobj.h"

#include <db.h>

namespace mongo {

    class Descriptor;
    class TxnCompleteHooks;

    namespace storage {

        class UpdateCallback : boost::noncopyable {
            void reportBug() {
                msgasserted(17214, "bug: update apply callback not properly installed");
            }
        public:
            virtual ~UpdateCallback() { }
            virtual bool applyMods(
                const BSONObj &oldObj,
                const BSONObj &msg,
                const BSONObj& query,
                const uint32_t fastUpdateFlags,
                BSONObj& newObj
                ) 
            {
                reportBug();
                return false;
            }
            virtual bool matchesQuery(const BSONObj &oldObj, const BSONObj &query) {
                reportBug();
                return false;
            }
        };

        extern DB_ENV *env;

        void startup(TxnCompleteHooks *hooks, UpdateCallback *updateCallback);
        void shutdown(void);

        void db_remove(const string &name);
        void db_rename(const string &old_name, const string &new_name);

        void get_status(BSONObjBuilder &status);
        void get_pending_lock_request_status(vector<BSONObj> &pendingLockRequests);
        void get_live_transaction_status(vector<BSONObj> &liveTransactions);
        void log_flush();
        void checkpoint();

        void set_log_flush_interval(uint32_t period_ms);
        void set_checkpoint_period(uint32_t period_seconds);
        void set_cleaner_period(uint32_t period_seconds);
        void set_cleaner_iterations(uint32_t num_iterations);
        void set_lock_timeout(uint64_t timeout_ms);
        void set_loader_max_memory(uint64_t bytes);

        void handle_ydb_error(int error);
        MONGO_COMPILER_NORETURN void handle_ydb_error_fatal(int error);

        void do_backtrace();

    } // namespace storage

} // namespace mongo

