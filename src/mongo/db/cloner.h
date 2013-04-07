// cloner.h - copy a database (export/import basically)

/**
 *    Copyright (C) 2011 10gen Inc.
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

namespace mongo {

    struct CloneOptions {

        CloneOptions() {
            logForRepl = true;
            slaveOk = false;
            useReplAuth = false;
            mayYield = true;
            mayBeInterrupted = false;

            syncData = true;
            syncIndexes = true;
        }
            
        string fromDB;
        set<string> collsToIgnore;

        bool logForRepl;
        bool slaveOk;
        bool useReplAuth;
        bool mayYield;
        bool mayBeInterrupted;

        bool syncData;
        bool syncIndexes;
    };

    bool cloneFrom(
        const string& masterHost , 
        const CloneOptions& options , 
        shared_ptr<DBClientConnection> conn,
        string& errmsg /* out */
        );
    
} // namespace mongo
