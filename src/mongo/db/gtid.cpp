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

#include "mongo/pch.h"

#include "mongo/db/gtid.h"

#include "mongo/bson/util/builder.h"
#include "mongo/util/time_support.h"

namespace mongo {

    int GTID::cmp(GTID a, GTID b) {
        if (a._primarySeqNo != b._primarySeqNo) {
            return (a._primarySeqNo < b._primarySeqNo) ? -1 : 1;
        }
        if (a._GTSeqNo == b._GTSeqNo) return 0;
        return (a._GTSeqNo < b._GTSeqNo) ? -1 : 1;
    }

    uint32_t GTID::GTIDBinarySize() {
        // for _primarySeqNo and _GTSeqNo
        return 2*sizeof(uint64_t);
    }

#define SWAP64(x) \
        ((uint64_t)((((uint64_t)(x) & 0xff00000000000000ULL) >> 56) | \
                    (((uint64_t)(x) & 0x00ff000000000000ULL) >> 40) | \
                    (((uint64_t)(x) & 0x0000ff0000000000ULL) >> 24) | \
                    (((uint64_t)(x) & 0x000000ff00000000ULL) >>  8) | \
                    (((uint64_t)(x) & 0x00000000ff000000ULL) <<  8) | \
                    (((uint64_t)(x) & 0x0000000000ff0000ULL) << 24) | \
                    (((uint64_t)(x) & 0x000000000000ff00ULL) << 40) | \
                    (((uint64_t)(x) & 0x00000000000000ffULL) << 56)))

    GTID::GTID() {
        _primarySeqNo = 0;
        _GTSeqNo = 0;
    }

    GTID::GTID(const char* binData) {
        const char* pos = binData;
        uint64_t swappedPrim = *(uint64_t *)pos;
        pos += sizeof(uint64_t);
        uint64_t swappedSec = *(uint64_t *)pos;
        _primarySeqNo = SWAP64(swappedPrim);
        _GTSeqNo = SWAP64(swappedSec);
    }

    void GTID::serializeBinaryData(char* binData) const {
        char* pos = binData;
        uint64_t prim =  SWAP64(_primarySeqNo);
        memcpy(pos, &prim, sizeof(uint64_t));
        pos += sizeof(uint64_t);
        uint64_t sec =  SWAP64(_GTSeqNo);
        memcpy(pos, &sec, sizeof(uint64_t));
    }

    void GTID::inc() {
        _GTSeqNo++;
    }

    void GTID::inc_primary() {
        _primarySeqNo++;
        _GTSeqNo = 0;
    }

    string GTID::toString() const {
        stringstream ss;
        ss << "GTID(" << _primarySeqNo<< ", " << _GTSeqNo << ")";
        return ss.str();
    }

    bool GTID::isInitial() const {
        return (_primarySeqNo == 0);
    }



    
    GTIDManager::GTIDManager( GTID lastGTID, uint64_t lastTime, uint64_t lastHash, uint32_t id ) {
        _selfID = id;
        _lastLiveGTID = lastGTID;
        _minLiveGTID = _lastLiveGTID;
        _minLiveGTID.inc(); // comment this
        _incPrimary = false;

        // note that _minUnappliedGTID is not set

        _lastTimestamp = lastTime;
        _lastHash = lastHash;
    }

    GTIDManager::~GTIDManager() {
    }

    // This function is meant to only be called on a primary,
    // it assumes that we are fully up to date and are the ones
    // getting GTIDs for transactions that will be applying
    // new data to the replica set. 
    //
    // returns a GTID that is an increment of _lastGTID
    // also notes that GTID has been handed out
    void GTIDManager::getGTIDForPrimary(GTID* gtid, uint64_t* timestamp, uint64_t* hash) {
        // it is ok for this to be racy. It is used for heuristic purposes
        *timestamp = curTimeMillis64();

        boost::unique_lock<boost::mutex> lock(_lock);
        dassert(GTID::cmp(_lastLiveGTID, _lastUnappliedGTID) == 0);
        if (_incPrimary) {
            _incPrimary = false;
            _lastLiveGTID.inc_primary();
        }
        else {
            _lastLiveGTID.inc();
        }

        if (_liveGTIDs.size() == 0) {
            _minLiveGTID = _lastLiveGTID;
        }

        _lastUnappliedGTID = _lastLiveGTID;
        *gtid = _lastLiveGTID;
        _liveGTIDs.insert(*gtid);
        _lastTimestamp = *timestamp;
        *hash = (_lastHash* 131 + *timestamp) * 17 + _selfID;
        _lastHash = *hash;
    }
    
    // notification that user of GTID has completed work
    // and either committed or aborted transaction associated with
    // GTID
    //
    // THIS MUST BE DONE ON A PRIMARY
    //
    void GTIDManager::noteLiveGTIDDone(const GTID& gtid) {
        boost::unique_lock<boost::mutex> lock(_lock);
        dassert(GTID::cmp(gtid, _minLiveGTID) >= 0);
        dassert(_liveGTIDs.size() > 0);
        // remove from list of GTIDs
        _liveGTIDs.erase(gtid);
        // if what we are removing is currently the minumum live GTID
        // we need to update the minimum live GTID
        if (GTID::cmp(_minLiveGTID, gtid) == 0) {
            if (_liveGTIDs.size() == 0) {
                _minLiveGTID = _lastLiveGTID;
                _minLiveGTID.inc();
            }
            else {
                // get the minumum from _liveGTIDs and set it to _minLiveGTIDs
                _minLiveGTID = *(_liveGTIDs.begin());
            }
            // note that on a primary, which we must be, these are equivalent
            _minUnappliedGTID = _minLiveGTID;
            // notify that _minLiveGTID has changed
            _minLiveCond.notify_all();
        }
    }


    // This function is called on a secondary when a GTID 
    // from the primary is added and committed to the opLog
    void GTIDManager::noteGTIDAdded(const GTID& gtid, uint64_t ts, uint64_t lastHash) {
        boost::unique_lock<boost::mutex> lock(_lock);
        // if we are adding a GTID on a secondary, then 
        // these values must be equal
        dassert(GTID::cmp(_lastLiveGTID, _minLiveGTID) < 0);
        dassert(GTID::cmp(_lastLiveGTID, gtid) < 0);
        _lastLiveGTID = gtid;
        _minLiveGTID = _lastLiveGTID;
        _minLiveGTID.inc();

        _lastTimestamp = ts;
        _lastHash = lastHash;

        _minLiveCond.notify_all();
    }

    // called when a secondary takes an unapplied GTID it has read in the oplog
    // and starts to apply it
    void GTIDManager::noteApplyingGTID(const GTID& gtid) {
        try {
            boost::unique_lock<boost::mutex> lock(_lock);
            dassert(GTID::cmp(gtid, _minUnappliedGTID) >= 0);
            dassert(GTID::cmp(gtid, _lastUnappliedGTID) > 0);
            if (_unappliedGTIDs.size() == 0) {
                _minUnappliedGTID = gtid;
            }

            _unappliedGTIDs.insert(gtid);
            _lastUnappliedGTID = gtid;
        }
        catch (std::exception &e) {
            severe() << "exception during noteApplyingGTID, aborting system: " << e.what() << std::endl;
            ::abort();
        }
    }

    // called when a GTID has finished being applied, which means
    // we can remove it from the unappliedGTIDs set
    void GTIDManager::noteGTIDApplied(const GTID& gtid) {
        try {
            boost::unique_lock<boost::mutex> lock(_lock);
            dassert(GTID::cmp(gtid, _minUnappliedGTID) >= 0);
            dassert(_unappliedGTIDs.size() > 0);
            // remove from list of GTIDs
            _unappliedGTIDs.erase(gtid);
            // if what we are removing is currently the minumum live GTID
            // we need to update the minimum live GTID
            if (GTID::cmp(_minUnappliedGTID, gtid) == 0) {
                if (_unappliedGTIDs.size() == 0) {
                    _minUnappliedGTID = _lastUnappliedGTID;
                    _minUnappliedGTID.inc();
                }
                else {
                    // get the minumum from _liveGTIDs and set it to _minLiveGTIDs
                    _minUnappliedGTID = *(_unappliedGTIDs.begin());
                }
            }
        }
        catch (std::exception &e) {
            severe() << "exception during noteGTIDApplied, aborting system: " << e.what() << std::endl;
            ::abort();
        }
    }


    void GTIDManager::getMins(GTID* minLiveGTID, GTID* minUnappliedGTID) {
        boost::unique_lock<boost::mutex> lock(_lock);
        *minLiveGTID = _minLiveGTID;
        *minUnappliedGTID = _minUnappliedGTID;
    }

    GTID GTIDManager::getMinLiveGTID() {
        GTID minLive;
        GTID minUnapplied;
        getMins(&minLive, &minUnapplied);
        return minLive;
    }

    void GTIDManager::resetManager() {
        boost::unique_lock<boost::mutex> lock(_lock);
        dassert(_liveGTIDs.size() == 0);
        // tell the GTID Manager that the next GTID
        // we get for a primary, we increment the primary
        _incPrimary = true;

        _minLiveGTID = _lastLiveGTID;
        _minLiveGTID.inc();

        _lastUnappliedGTID = _lastLiveGTID;
        _minUnappliedGTID = _minLiveGTID;
    }
    GTID GTIDManager::getLiveState() {
        boost::unique_lock<boost::mutex> lock(_lock);
        GTID ret = _lastLiveGTID;
        return ret;
    }

    void GTIDManager::getGTIDs(
        GTID* lastLiveGTID, 
        GTID* lastUnappliedGTID, 
        GTID* minLiveGTID, 
        GTID* minUnappliedGTID
        ) 
    {
        boost::unique_lock<boost::mutex> lock(_lock);
        *lastLiveGTID = _lastLiveGTID;
        *lastUnappliedGTID = _lastUnappliedGTID;
        *minLiveGTID = _minLiveGTID;
        *minUnappliedGTID = _minUnappliedGTID;
    }
    void GTIDManager::getLiveGTIDs(GTID* lastLiveGTID, GTID* lastUnappliedGTID) {
        GTID minLiveGTID;
        GTID minUnappliedGTID;
        getGTIDs(lastLiveGTID, lastUnappliedGTID, &minLiveGTID, &minUnappliedGTID);
    }
    
    // does some sanity checks to make sure the GTIDManager
    // is in a state where it can become primary
    void GTIDManager::verifyReadyToBecomePrimary() {
        boost::unique_lock<boost::mutex> lock(_lock);
        verify(GTID::cmp(_lastLiveGTID, _lastUnappliedGTID) == 0);
        verify(GTID::cmp(_minLiveGTID, _minUnappliedGTID) == 0);
        verify(GTID::cmp(_minLiveGTID, _lastLiveGTID) > 0);
    }

    // used for Tailable cursors on the oplog. The input GTID states the last
    // GTID that was used as the min live GTID. This function returns when
    // either millis milliseconds has passed, or when it notices that the min
    // live GTID is something greater than the last min live GTID. This
    // allows tailable cursors to know when there is some new data
    // to be read
    void GTIDManager::waitForDifferentMinLive(GTID last, uint32_t millis) {
        boost::unique_lock<boost::mutex> lock(_lock);
        dassert(GTID::cmp(last, _minLiveGTID) <= 0);
        if (GTID::cmp(last, _minLiveGTID) == 0) {
            // wait on cond
            _minLiveCond.timed_wait(lock, boost::posix_time::milliseconds(millis));
        }
    }

    // after an intial sync has happened and the oplog has been updated
    // with operations up to a certain point in the oplog stream,
    // and these operations have been applied, we set the values
    // of the GTIDManager to reflect the state of the oplog so that
    // we can proceed with replication.
    void GTIDManager::resetAfterInitialSync(GTID last, uint64_t lastTime, uint64_t lastHash) {
        boost::unique_lock<boost::mutex> lock(_lock);
        verify(_liveGTIDs.size() == 0);
        verify(_unappliedGTIDs.size() == 0);
        _lastLiveGTID = last;
        _minLiveGTID = _lastLiveGTID;
        _minLiveGTID.inc(); // comment this

        _lastUnappliedGTID = _lastLiveGTID;
        _minUnappliedGTID = _minLiveGTID;

        _lastTimestamp = lastTime;
        _lastHash = lastHash;
    }

    uint64_t GTIDManager::getCurrTimestamp() {
        boost::unique_lock<boost::mutex> lock(_lock);
        uint64_t ret = _lastTimestamp;
        return ret;        
    }

    void GTIDManager::catchUnappliedToLive() {
        boost::unique_lock<boost::mutex> lock(_lock);
        verify(_liveGTIDs.size() == 0);
        verify(_unappliedGTIDs.size() == 0);
        _lastUnappliedGTID = _lastLiveGTID;
        _minUnappliedGTID = _minLiveGTID;
    }
    
    bool GTIDManager::rollbackNeeded(
        const GTID& last, 
        uint64_t lastTime, 
        uint64_t lastHash
        ) 
    {
        return !((GTID::cmp(last, _lastLiveGTID) == 0) && 
                 lastTime == _lastTimestamp && 
                 lastHash == _lastHash);
    }
} // namespace mongo
