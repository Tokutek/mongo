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

#include "gtid.h"

#include "mongo/pch.h"

namespace mongo {

    // TODO: Is there a faster comparison function for this?
    int GTID::cmp(GTID a, GTID b) {
        if (a._primarySeqNo != b._primarySeqNo) {
            return (a._primarySeqNo < b._primarySeqNo) ? -1 : 1;
        }
        if (a._GTSeqNo == b._GTSeqNo) return 0;
        return (a._GTSeqNo < b._GTSeqNo) ? -1 : 1;
    }

    GTID::GTID() {
        _primarySeqNo = 0;
        _GTSeqNo = 0;
    }

    GTID::GTID(uint64_t primarySeqNo, uint64_t GTSeqNo) {
        _primarySeqNo = primarySeqNo;
        _GTSeqNo = GTSeqNo;
    }

    GTID::GTID(BSONObj b) {
        _primarySeqNo = static_cast<uint64_t>(b["p"].Long());
        _GTSeqNo = static_cast<uint64_t>(b["t"].Long());
    }

    // This is doing a malloc. Would be nice to find a way to do
    // this without a malloc.
    BSONObj GTID::getBSON() {
        BSONObjBuilder b;
        b.append("p", static_cast<long long>(_primarySeqNo));
        b.append("t", static_cast<long long>(_GTSeqNo));
        return b.obj();
    }

    void GTID::inc() {
        _GTSeqNo++;
    }

    void GTID::inc_primary() {
        _primarySeqNo++;
        _GTSeqNo = 0;
    }
    
    GTIDManager::GTIDManager( GTID lastGTID ) {
        _nextGTID = lastGTID;
        _nextGTID.inc();
        _minLiveGTID = _nextGTID;
    }

    GTIDManager::~GTIDManager() {
    }

    // returns a GTID that is an increment of _lastGTID
    // also notes that GTID has been handed out
    GTID GTIDManager::getGTID() {
        GTID ret;
        _lock.lock();
        ret = _nextGTID;
        _liveGTIDs.insert(ret);
        _nextGTID.inc();
        _lock.unlock();
        return ret;
    }
    
    // notification that user of GTID has completed work
    // and either committed or aborted transaction associated with
    // GTID
    void GTIDManager::noteGTIDDone(GTID gtid) {
        _lock.lock();
        dassert(GTID::cmp(gtid, _minLiveGTID) >= 0);
        dassert(_liveGTIDs.size() > 0);
        // remove from list of GTIDs
        _liveGTIDs.erase(gtid);
        // if what we are removing is currently the minumum live GTID
        // we need to update the minimum live GTID
        if (GTID::cmp(_minLiveGTID, gtid) == 0) {
            if (_liveGTIDs.size() == 0) {
                _minLiveGTID = _nextGTID;
            }
            else {
                // get the minumum from _liveGTIDs and set it to _minLiveGTIDs
                _minLiveGTID = *(_liveGTIDs.begin());
            }
        }
        _lock.unlock();
    }

} // namespace mongo
