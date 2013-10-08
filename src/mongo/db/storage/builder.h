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

#include <db.h>

#include "mongo/pch.h"
#include "mongo/db/client.h"
#include "mongo/db/curop.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/timer.h"

namespace mongo {

    namespace storage {

        class BuilderBase : boost::noncopyable {
        public:
            BuilderBase() { }
            void setPollMessagePrefix(const string &msg) {
                _poll_extra.msg_prefix = msg;
            }

            struct poll_function_extra : public ExceptionSaver {
                poll_function_extra() :
                    c(cc()), msg_prefix(""),
                    timer(), lastReportSeconds(0), lastReportProgress(0) {
                }
                Client &c;
                string msg_prefix;
                Timer timer;
                long long lastReportSeconds;
                double lastReportProgress;
            };
            static int poll_function(void *extra, float progress) {
                poll_function_extra *info = static_cast<poll_function_extra *>(extra);
                try {
                    killCurrentOp.checkForInterrupt(info->c); // uasserts if we should stop

                    // Report every 1% of progress, but no more than once a second.
                    if (progress > info->lastReportProgress + 0.01) {
                        long long now = info->timer.seconds();
                        if (now > info->lastReportSeconds) {
                            log() << info->msg_prefix << " "
                                  << (long long) (progress * 100) << "%." << endl;
                        }
                        info->lastReportProgress = progress;
                        info->lastReportSeconds = now;
                    }
                    return 0;
                } catch (const std::exception &ex) {
                    info->saveException(ex);
                }
                return -1;
            }

            struct error_callback_extra {
                error_callback_extra() { }
                string errmsg;
            };
            static void error_callback(DB *db, int i, int err,
                                       DBT *key, DBT *val, void *extra) {
                error_callback_extra *info = static_cast<error_callback_extra *>(extra);
                str::stream errmsg;
                errmsg << "Index build failed with code " << err << ".";
                if (err == EINVAL) {
                     errmsg << " This may be due to keys > 32kb or a document > 32mb." <<
                               " Check the error log for " <<
                               "\"Key too big ...\" or \"Row too big...\"";
                }
                info->errmsg = errmsg;
            }

        protected:
            poll_function_extra _poll_extra;
            error_callback_extra _error_extra;
        };

        // Wrapper for the ydb's DB_LOADER
        class Loader : public BuilderBase {
        public:
            Loader(DB **dbs, const int n);

            ~Loader();

            int put(DBT *key, DBT *val);

            int close();

        private:
            DB **_dbs;
            const int _n;

            DB_LOADER *_loader;
            bool _closed;
        };

        // Wrapper for the ydb's DB_INDEXER
        class Indexer : public BuilderBase {
        public:
            Indexer(DB *src_db, DB *dest_db);

            ~Indexer();

            int build();

            int close();

        private:
            DB *_dest_db;
            DB_INDEXER *_indexer;
            bool _closed;
        };

    } // namespace storage

} // namespace mongo
