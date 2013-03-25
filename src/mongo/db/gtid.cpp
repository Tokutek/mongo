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
        b.appendNumber("p", _primarySeqNo);
        b.appendNumber("t", _GTSeqNo);
        return b.obj();
    }

    void GTID::inc() {
        _GTSeqNo++;
    }

    void GTID::inc_primary() {
        _primarySeqNo++;
    }

} // namespace mongo
