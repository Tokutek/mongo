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

#include "db.h"
#include "dbhelpers.h"
#include "clientcursor.h"
#include "../util/optime.h"
#include "../util/timer.h"

namespace mongo {

    void createOplog();
    void logToReplInfo(GTID minLiveGTID, GTID minUnappliedGTID);

    void logOp( const char *opstr, const char *ns, const BSONObj& obj, BSONObj *patt = 0, bool fromMigrate = false );
    // Write operations to the log (local.oplog.$main)
    void logTransactionOps(GTID gtid, uint64_t timestamp, uint64_t hash, BSONArray& opInfo);
    void logTransactionOpsRef(GTID gtid, uint64_t timestamp, uint64_t hash, OID& oid);
    void insertOplogRefs(BSONObj o);
    void deleteOplogFiles();
    void openOplogFiles();
    
    GTID getGTIDFromOplogEntry(BSONObj o);
    bool getLastGTIDinOplog(GTID* gtid);
    bool gtidExistsInOplog(GTID gtid);
    void writeEntryToOplog(BSONObj entry);
    void replicateTransactionToOplog(BSONObj& op);
    void replicateTransactionToOplogToFillGap(BSONObj& op);
    void applyTransactionFromOplog(BSONObj entry);
    void rollbackTransactionFromOplog(BSONObj entry);
    void purgeEntryFromOplog(BSONObj entry);
    
    /** puts obj in the oplog as a comment (a no-op).  Just for diags.
        convention is
          { msg : "text", ... }
    */

    extern int __findingStartInitialTimeout; // configurable for testing

    class QueryPlan;
    
    class Sync {
    protected:
        string hn;
    public:
        Sync(const string& hostname) : hn(hostname) {}
        virtual ~Sync() {}
    };
}
