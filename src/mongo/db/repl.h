// repl.h - replication

/**
*    Copyright (C) 2008 10gen Inc.
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

/* replication data overview

   at the slave:
     local.sources { host: ..., source: ..., only: ..., syncedTo: ..., localLogTs: ..., dbsNextPass: { ... }, incompleteCloneDbs: { ... } }

   at the master:
     local.oplog.$<source>
*/

#pragma once

#include "mongo/db/dbhelpers.h"
#include "mongo/util/optime.h"
#include "mongo/db/oplog.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/db/oplogreader.h"
#include "mongo/db/cloner.h"

namespace mongo {

    class ReplSettings {
    public:
        bool fastsync;

        bool autoresync;

        int slavedelay;

        set<string> discoveredSeeds;
        mutex discoveredSeeds_mx;

        BSONObj reconfig;

        ReplSettings()
            : fastsync(),
            autoresync(false),
            slavedelay(),
            discoveredSeeds(),
            discoveredSeeds_mx("ReplSettings::discoveredSeeds") {
        }

    };

    extern ReplSettings replSettings;

    bool anyReplEnabled();
    void appendReplicationInfo(BSONObjBuilder& result, int level = 0);
} // namespace mongo
