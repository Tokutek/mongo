// repl_block.h - blocking on writes for replication

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

#pragma once

#include "mongo/pch.h"
#include "client.h"
#include "curop.h"

/**

 */
namespace mongo {

    typedef enum {
        REPL_SUCCESS = 0,
        REPL_FAIL,
        REPL_WAITING
    } OP_REPL_STATUS;


    void updateSlaveLocation( CurOp& curop, const char * oplog_ns , GTID lastGTID );

    /** @return true if op has made it to w servers */
    // TODO: FIX THIS FOR SHARDING
    bool opReplicatedEnough( GTID gtid , int w );
    OP_REPL_STATUS opReplicatedEnough( GTID gtid , BSONElement w );

    std::vector<BSONObj> getHostsWrittenTo(GTID gtid);

    void resetSlaveCache();
    unsigned getSlaveCount();
}
