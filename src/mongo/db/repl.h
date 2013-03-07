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

#include "mongo/base/counter.h"
#include "mongo/db/stats/timer_stats.h"

namespace mongo {

    extern TimerStats oplogInsertStats;
    extern Counter64 oplogInsertBytesStats;

} // namespace mongo
