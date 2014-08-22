// cloner.h - copy a database (export/import basically)

/**
 *    Copyright (C) 2011 10gen Inc.
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

#include "jsobj.h"
#include "mongo/util/progress_meter.h"

namespace mongo {

    struct CloneOptions {

        CloneOptions() {
            logForRepl = true;
            slaveOk = false;
            useReplAuth = false;
            mayBeInterrupted = false;

            syncData = true;
            syncIndexes = true;
        }
            
        string fromDB;
        set<string> collsToIgnore;

        bool logForRepl;
        bool slaveOk;
        bool useReplAuth;
        bool mayBeInterrupted;

        bool syncData;
        bool syncIndexes;
    };

    class DBClientBase;

    bool cloneFrom(
        const string& masterHost , 
        const CloneOptions& options , 
        shared_ptr<DBClientBase> conn,
        string& errmsg, /* out */
        ProgressMeter *parentProgress = NULL
        );
    
    void cloneCollection(
        shared_ptr<DBClientBase> conn,
        const string& dbname,
        const string& ns, 
        const BSONObj& query,
        bool copyIndexes,
        bool logForRepl
        );
    
    bool cloneRemotePartitionInfo(
        const string& fromdb,
        const string& todb,
        const string& fromns, 
        const string& tons, 
        DBClientBase* conn,
        string& errmsg,
        bool logForRepl
        );
} // namespace mongo
