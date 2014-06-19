// oplog.h - writing to and reading from oplog

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

/*

     local.oplog.$main is the default
*/

#pragma once

#include "mongo/db/clientcursor.h"
#include "mongo/db/oplogreader.h"
#include "mongo/util/optime.h"
#include "mongo/util/timer.h"

namespace mongo {

    void createOplog();
    void logToReplInfo(GTID minLiveGTID, GTID minUnappliedGTID);
    void logHighestVotedForPrimary(uint64_t hkp);

    void logOp( const char *opstr, const char *ns, const BSONObj& obj, BSONObj *patt = 0, bool fromMigrate = false );
    // Write operations to the log (local.oplog.$main)
    void logTransactionOps(GTID gtid, uint64_t timestamp, uint64_t hash, const deque<BSONObj>& ops);
    void logTransactionOpsRef(GTID gtid, uint64_t timestamp, uint64_t hash, OID& oid);
    void logOpsToOplogRef(BSONObj o);
    void deleteOplogFiles();
    
    GTID getGTIDFromOplogEntry(BSONObj o);
    bool getLastGTIDinOplog(GTID* gtid);
    bool gtidExistsInOplog(GTID gtid);
    void writeEntryToOplog(BSONObj entry, bool recordStats);
    void writeEntryToOplogRefs(BSONObj entry);
    void replicateFullTransactionToOplog(BSONObj& o, OplogReader& r, bool* bigTxn);
    void applyTransactionFromOplog(BSONObj entry);
    void rollbackTransactionFromOplog(BSONObj entry, bool purgeEntry);
    void purgeEntryFromOplog(BSONObj entry);

    // @return the age, in milliseconds, when an oplog entry expires.
    uint64_t expireOplogMilliseconds();

    uint64_t getLastPartitionAddTime();
    void addOplogPartitions();
    void trimOplogWithTS(uint64_t tsMillis);
    void trimOplogwithGTID(GTID gtid);
    void convertOplogToPartitionedIfNecessary();
}
