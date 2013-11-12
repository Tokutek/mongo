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

    void logOp( const char *opstr, const char *ns, const BSONObj& obj, BSONObj *patt = 0, bool fromMigrate = false );
    // Write operations to the log (local.oplog.$main)
    void logTransactionOps(GTID gtid, uint64_t timestamp, uint64_t hash, BSONArray& opInfo);
    void logTransactionOpsRef(GTID gtid, uint64_t timestamp, uint64_t hash, OID& oid);
    void logOpsToOplogRef(BSONObj o);
    void deleteOplogFiles();
    bool oplogFilesOpen();
    void openOplogFiles();
    
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

    // hot optimize oplog up to gtid, used by purge thread to vacuum stale entries
    void hotOptimizeOplogTo(GTID gtid, uint64_t* loops_run);
    
    /** puts obj in the oplog as a comment (a no-op).  Just for diags.
        convention is
          { msg : "text", ... }
    */

    class QueryPlan;
    
    class Sync {
    protected:
        string hn;
    public:
        Sync(const string& hostname) : hn(hostname) {}
        virtual ~Sync() {}
    };
}
