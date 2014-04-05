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

#include "mongo/pch.h"
//#include "mongo/db/jsobj.h"
#include <limits>

#include "mongo/base/status.h"

namespace mongo {

    class BSONObjBuilder;

    class GTID {
        uint64_t _primarySeqNo;
        uint64_t _GTSeqNo;
    public:
        static int cmp(GTID a, GTID b);
        static uint32_t GTIDBinarySize();
        GTID();
        GTID(uint64_t primarySeqNo, uint64_t GTSeqNo){
            _primarySeqNo = primarySeqNo;
            _GTSeqNo = GTSeqNo;
        }
        GTID(const char* binData);
        ~GTID(){};
        void serializeBinaryData(char* binData) const;
        void inc();
        void setPrimaryTo(uint64_t newPrimary);
        string toString() const;
        bool isInitial() const;
        uint64_t getPrimary() const;        
        bool operator==(const GTID& other) const {
            return _primarySeqNo == other._primarySeqNo && _GTSeqNo == other._GTSeqNo;
        }
        static Status parseConciseString(const StringData &s, GTID &gtid);
        std::string toConciseString() const;
        friend class GTIDManagerTest; // for testing
    };

    static const GTID GTID_MAX(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max());

    struct GTIDCmp {
        bool operator()( const GTID& l, const GTID& r ) const {
            return GTID::cmp(l, r) < 0;
        }
    };

    typedef std::set<GTID, GTIDCmp> GTIDSet;

    typedef enum {
        VOTE_YES = 0,
        VOTE_NO,
        VOTE_VETO
    } PRIMARY_VOTE;

    class GTIDManager {
        boost::mutex _lock;

        // notified when the min live GTID changes
        boost::condition_variable _minLiveCond;

        // when a machine newly assumes primary, we want to
        // set the primary sequence number of the GTIDs
        // that are handed out to something new, but we do not want to do it
        // until we call getGTIDForPrimary. Otherwise,
        // in a system where no writes are happening, elections
        // may stall because this machine will think the last GTID is
        // some high value that has never actually been given out.
        // So, we use this value as a signal to getGTIDForPrimary
        // to reset the primary sequence number
        uint64_t _newPrimaryValue;
        
        // GTID to give out should a primary ask for one to use
        // On a secondary, this is the last GTID seen incremented
        GTID _lastLiveGTID;

        GTID _lastUnappliedGTID;

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

        uint32_t _selfID; // used for hash construction

        // specifies the highest known possible primary
        // It ought to be the minimum of _lastLiveGTID.getPrimary() and
        // whatever the last value we voted for in the election protocol
        // Note that it ONLY relates to values that this member has voted
        // for in elections, or oplog entries seen. This is completely
        // independent from ReplSetImpl::highestKnownPrimaryAcrossReplSet.
        // Also, this value controls what can and cannot be voted for in elections
        uint64_t _highestKnownPossiblePrimary;
        
    public:            
        GTIDManager( GTID lastGTID, uint64_t lastTime, uint64_t lastHash, uint32_t id, uint64_t lastVotedForPrimary );
        ~GTIDManager();

        // methods for running on a primary
        
        // returns a GTID equal to _nextGTID on a primary
        // this should not be called on a secondary
        // also notes that GTID has been handed out
        void getGTIDForPrimary(GTID* gtid, uint64_t* timestamp, uint64_t* hash);

        // notification that user of GTID has completed work
        // and either committed or aborted transaction associated with
        // GTID
        void noteLiveGTIDDone(const GTID& gtid);

        // methods for running on a secondary

        // This function is called on a secondary when a GTID 
        // from the primary is added and committed to the opLog
        void noteGTIDAdded(const GTID& gtid, uint64_t ts, uint64_t lastHash);
        void noteApplyingGTID(const GTID& gtid);
        void noteGTIDApplied(const GTID& gtid);

        void getMins(GTID* minLiveGTID, GTID* minUnappliedGTID);
        GTID getMinLiveGTID();
        bool resetManager(uint64_t newPrimary);

        GTID getLiveState();
        uint64_t getCurrTimestamp();

        void getGTIDs(
            GTID* lastLiveGTID,
            GTID* lastUnappliedGTID,
            GTID* minLiveGTID,
            GTID* minUnappliedGTID
            );
        void getLiveGTIDs(GTID* lastLiveGTID, GTID* lastUnappliedGTID);
        void verifyReadyToBecomePrimary();

        void waitForDifferentMinLive(GTID last, uint32_t millis);
        void resetAfterInitialSync(GTID last, uint64_t lastTime, uint64_t lastHash);
        void catchUnappliedToLive();

        bool rollbackNeeded(const GTID& last, uint64_t lastTime, uint64_t lastHash);
        uint64_t getHighestKnownPrimary();
        PRIMARY_VOTE acceptPossiblePrimary(uint64_t newPrimary, GTID remoteGTID);
        bool canAcknowledgeGTID();
    private:
        void handleHighestKnownPrimary();

    friend class GTIDManagerTest; // for testing
        
    };

} // namespace mongo
