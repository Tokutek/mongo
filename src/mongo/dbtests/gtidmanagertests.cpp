/**
 *    Copyright (C) 2010 10gen Inc.
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

#include "pch.h"
#include "dbtests.h"
#include "mongo/db/gtid.h"

namespace mongo {
    class GTIDManagerTest {
    public:
        GTIDManagerTest() {}

        // simple test of GTIDs
        void GTIDtest() {
            GTID gtid1(1,0);
            GTID gtid2(0,1);
            ASSERT(!gtid1.isInitial());
            ASSERT(gtid2.isInitial());
            ASSERT(GTID::cmp(gtid1, gtid2) > 0);
            gtid1.inc();
            gtid2.inc_primary();
            ASSERT(gtid1._primarySeqNo == 1);
            ASSERT(gtid1._GTSeqNo == 1);
            ASSERT(gtid2._primarySeqNo == 1);
            ASSERT(gtid2._GTSeqNo == 0);
        }

        void testGTIDManager() {
            GTID lastGTID(1,1);
            GTIDManager mgr(lastGTID, 0, 0, 0);
            
            // make sure initialization is what we expect
            ASSERT(GTID::cmp(mgr._lastLiveGTID, lastGTID) == 0);
            ASSERT(GTID::cmp(mgr._minLiveGTID, lastGTID) > 0);
            lastGTID.inc();
            ASSERT(GTID::cmp(mgr._minLiveGTID, lastGTID) == 0);
            mgr.catchUnappliedToLive();
            ASSERT(GTID::cmp(mgr._lastLiveGTID, mgr._lastUnappliedGTID) == 0);
            ASSERT(GTID::cmp(mgr._minLiveGTID, mgr._minUnappliedGTID) == 0);
            GTID resetGTID(2,2);
            mgr.resetAfterInitialSync(resetGTID, 1, 1);
            mgr.verifyReadyToBecomePrimary();
            ASSERT(GTID::cmp(mgr._lastLiveGTID, resetGTID) == 0);
            ASSERT(GTID::cmp(mgr._lastLiveGTID, mgr._lastUnappliedGTID) == 0);
            ASSERT(GTID::cmp(mgr._minLiveGTID, mgr._minUnappliedGTID) == 0);
            resetGTID.inc();
            ASSERT(GTID::cmp(mgr._minLiveGTID, resetGTID) == 0);

            // now test that it works as primary
            GTID currLast = mgr.getLiveState();
            GTID currMin = mgr._minLiveGTID;
            ASSERT(GTID::cmp(currLast, mgr._lastLiveGTID) == 0);

            uint64_t ts;
            uint64_t hash;
            GTID gtid;
            mgr.getGTIDForPrimary(&gtid, &ts, &hash);
            cerr << gtid.toString() << endl;
            cerr << currMin.toString() <<endl;
            ASSERT(GTID::cmp(gtid, currMin) == 0);
            ASSERT(GTID::cmp(gtid, mgr._minLiveGTID) == 0);
            ASSERT(GTID::cmp(gtid, mgr._lastLiveGTID) == 0);
            mgr.noteLiveGTIDDone(gtid);
            ASSERT(GTID::cmp(gtid, mgr._lastLiveGTID) == 0);
            ASSERT(GTID::cmp(gtid, mgr._minLiveGTID) < 0);

            // simple test of resetManager
            currLast = mgr._lastLiveGTID;
            currMin = mgr._minLiveGTID;
            mgr.resetManager();
            mgr.verifyReadyToBecomePrimary();
            // make sure that lastLive and minLive not changed yet
            ASSERT(GTID::cmp(currMin, mgr._minLiveGTID) == 0);
            ASSERT(GTID::cmp(currLast, mgr._lastLiveGTID) == 0);
            ASSERT(mgr._incPrimary);
            // now make sure that primary has increased
            mgr.getGTIDForPrimary(&gtid, &ts, &hash);
            ASSERT(!mgr._incPrimary);
            ASSERT(gtid._primarySeqNo > currLast._primarySeqNo);
            mgr.noteLiveGTIDDone(gtid);
            mgr.verifyReadyToBecomePrimary();

            // now test that min is properly maintained
            currLast = mgr._lastLiveGTID;
            currMin = mgr._minLiveGTID;
            GTID gtid1, gtid2, gtid3, gtid4, gtid5;
            mgr.getGTIDForPrimary(&gtid1, &ts, &hash);
            mgr.getGTIDForPrimary(&gtid2, &ts, &hash);
            mgr.getGTIDForPrimary(&gtid3, &ts, &hash);
            mgr.getGTIDForPrimary(&gtid4, &ts, &hash);
            ASSERT(GTID::cmp(gtid1, gtid2) < 0);
            ASSERT(GTID::cmp(gtid2, gtid3) < 0);
            ASSERT(GTID::cmp(gtid3, gtid4) < 0);
            ASSERT(GTID::cmp(mgr._lastLiveGTID, gtid4) == 0);
            ASSERT(GTID::cmp(mgr._minLiveGTID, gtid1) == 0);
            // finish 2, nothing should change
            mgr.noteLiveGTIDDone(gtid2);
            ASSERT(GTID::cmp(mgr._lastLiveGTID, gtid4) == 0);
            ASSERT(GTID::cmp(mgr._minLiveGTID, gtid1) == 0);
            // finish 1, min should jump to 3
            mgr.noteLiveGTIDDone(gtid1);
            ASSERT(GTID::cmp(mgr._lastLiveGTID, gtid4) == 0);
            ASSERT(GTID::cmp(mgr._minLiveGTID, gtid3) == 0);
            // get 5, _lastLive should change
            mgr.getGTIDForPrimary(&gtid5, &ts, &hash);
            ASSERT(GTID::cmp(gtid4, gtid5) < 0);
            ASSERT(GTID::cmp(mgr._lastLiveGTID, gtid5) == 0);
            ASSERT(GTID::cmp(mgr._minLiveGTID, gtid3) == 0);
            
            // finish 3 and 4, should both jump to 5
            mgr.noteLiveGTIDDone(gtid3);
            mgr.noteLiveGTIDDone(gtid4);
            ASSERT(GTID::cmp(mgr._lastLiveGTID, gtid5) == 0);
            ASSERT(GTID::cmp(mgr._minLiveGTID, gtid5) == 0);
            // finish 5, min should jump up
            mgr.noteLiveGTIDDone(gtid5);
            ASSERT(GTID::cmp(mgr._lastLiveGTID, gtid5) == 0);
            ASSERT(GTID::cmp(mgr._minLiveGTID, gtid5) > 0);
            mgr.verifyReadyToBecomePrimary();

            GTID currLastUnapplied = mgr._lastUnappliedGTID;
            GTID currMinUnapplied = mgr._minUnappliedGTID;
            
            gtid5.inc();
            gtid5.inc();
            gtid5.inc();
            GTID gtidOther = gtid5;
            gtidOther.inc();

            GTID gtidUnapplied1 = gtid5;
            // now let's do a test for secondaries
            mgr.noteGTIDAdded(gtidUnapplied1, ts, hash);
            ASSERT(GTID::cmp(mgr._lastLiveGTID, gtidUnapplied1) == 0);
            ASSERT(GTID::cmp(mgr._minLiveGTID, gtidOther) == 0);
            gtid5.inc();
            gtidOther.inc();
            GTID gtidUnapplied2 = gtid5;
            mgr.noteGTIDAdded(gtidUnapplied2, ts, hash);
            ASSERT(GTID::cmp(mgr._lastLiveGTID, gtidUnapplied2) == 0);
            ASSERT(GTID::cmp(mgr._minLiveGTID, gtidOther) == 0);
            // verify unapplied values not changed
            ASSERT(GTID::cmp(mgr._lastUnappliedGTID, currLastUnapplied) == 0);
            ASSERT(GTID::cmp(mgr._minUnappliedGTID, currMinUnapplied) == 0);
            gtid5.inc();
            GTID gtidUnapplied3 = gtid5;
            mgr.noteGTIDAdded(gtidUnapplied3, ts, hash);
            gtid5.inc();
            GTID gtidUnapplied4 = gtid5;
            mgr.noteGTIDAdded(gtidUnapplied4, ts, hash);
            // at this point, we have 4 GTIDs that have been added, but
            // yet to be applied
            mgr.noteApplyingGTID(gtidUnapplied1);
            mgr.noteApplyingGTID(gtidUnapplied2);
            ASSERT(GTID::cmp(mgr._lastUnappliedGTID, gtidUnapplied2) == 0);
            ASSERT(GTID::cmp(mgr._minUnappliedGTID, gtidUnapplied1) == 0);
            mgr.noteGTIDApplied(gtidUnapplied2);
            ASSERT(GTID::cmp(mgr._minUnappliedGTID, gtidUnapplied1) == 0);
            mgr.noteApplyingGTID(gtidUnapplied3);
            mgr.noteApplyingGTID(gtidUnapplied4);
            ASSERT(GTID::cmp(mgr._lastUnappliedGTID, gtidUnapplied4) == 0);
            ASSERT(GTID::cmp(mgr._minUnappliedGTID, gtidUnapplied1) == 0);
            mgr.noteGTIDApplied(gtidUnapplied3);
            mgr.noteGTIDApplied(gtidUnapplied1);
            ASSERT(GTID::cmp(mgr._minUnappliedGTID, gtidUnapplied4) == 0);
            mgr.noteGTIDApplied(gtidUnapplied4);
            ASSERT(GTID::cmp(mgr._lastUnappliedGTID, gtidUnapplied4) == 0);
            ASSERT(GTID::cmp(mgr._minUnappliedGTID, gtidUnapplied4) > 0);
        }

        void run() {
            GTIDtest();
            testGTIDManager();
        }
    };
}

namespace GTIDManagerTests {
    class All : public Suite {
    public:
        All() : Suite( "GTIDManager" ) {
        }

        void setupTests() {
            add<GTIDManagerTest>();
        }

    } all;
}
