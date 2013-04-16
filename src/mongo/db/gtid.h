/**
*    Copyright (C) 2012 Tokutek Inc.
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
#include "mongo/db/jsobj.h"

#include <db.h>

namespace mongo {


    class GTID {
        uint64_t _primarySeqNo;
        uint64_t _GTSeqNo;
        public:
        static int cmp(GTID a, GTID b);
        static uint32_t GTIDBinarySize();
        GTID();
        GTID(uint64_t primarySeqNo, uint64_t GTSeqNo);
        GTID(const char* binData);
        ~GTID(){};
        void serializeBinaryData(char* binData);
        void inc();
        void inc_primary();        
        string toString() const;
        bool isInitial() const;
    };

    struct GTIDCmp {
        bool operator()( const GTID& l, const GTID& r ) const {
            return GTID::cmp(l, r) < 0;
        }
    };

    typedef std::set<GTID, GTIDCmp> GTIDSet;

    class GTIDManager {
        boost::mutex _lock;
        
        // GTID to give out should a primary ask for one to use
        // On a secondary, this is the last GTID seen incremented
        GTID _nextLiveGTID;

        GTID _nextUnappliedGTID;

        // the minimum live GTID
        // on a primary, this is the minimum value in _liveGTIDs
        // on a secondary, this is simply _nextGTID,
        GTID _minLiveGTID;

        // the minimum unapplied GTID
        // on a primary, this is equal to the _minLiveGTID, because
        // if we are a primary, then there should be nothing 
        // committed in the opLog that is not also applied
        // on a secondary, this is the minumum GTID in the opLog
        // that has yet to be applied to the collections on the secondary
        GTID _minUnappliedGTID;

        // set of GTIDs that are live and not committed.
        // on a primary, these GTIDs have been handed out
        // by the GTIDManager to be used in the oplog, and
        // the GTIDManager has yet to get notification that 
        // the associated transaction to this GTID has been committed
        GTIDSet _liveGTIDs;

        // set of GTIDs committed to the opLog, but not applied
        // to the collections. On a primary, this should be empty
        // on a secondary, this is the set of GTIDs that are in process
        // of being applied
        GTIDSet _unappliedGTIDs;

        // in milliseconds, derived from curTimeMillis64
        uint64_t _lastTimestamp;
        uint64_t _lastHash;
        
        public:            
        GTIDManager( GTID lastGTID, uint64_t lastTime, uint64_t lastHash );
        ~GTIDManager();

        // methods for running on a primary
        
        // returns a GTID equal to _nextGTID on a primary
        // this should not be called on a secondary
        // also notes that GTID has been handed out
        void getGTIDForPrimary(GTID* gtid, uint64_t* timestamp, uint64_t* hash);

        // notification that user of GTID has completed work
        // and either committed or aborted transaction associated with
        // GTID
        void noteLiveGTIDDone(GTID gtid);

        // methods for running on a secondary

        // This function is called on a secondary when a GTID 
        // from the primary is added and committed to the opLog
        void noteGTIDAdded(GTID gtid);
        void noteApplyingGTID(GTID gtid);
        void noteGTIDApplied(GTID gtid);

        void getMins(GTID* minLiveGTID, GTID* minUnappliedGTID);
        void resetManager(GTID lastGTID, uint64_t lastTimestamp, uint64_t lastHash);

        GTID getLiveState();
        uint64_t getCurrTimestamp();
        
    };
    void addGTIDToBSON(const char* keyName, GTID gtid, BSONObjBuilder& result);
    GTID getGTIDFromBSON(const char* keyName, const BSONObj& obj);

} // namespace mongo
