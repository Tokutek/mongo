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
            GTID gtid2(0,0);
            ASSERT(!gtid1.isInitial());
            ASSERT(gtid2.isInitial());
            ASSERT(GTID::cmp(gtid1, gtid2) > 0);
            gtid1.inc();
            ASSERT(gtid1._primarySeqNo == 1);
            ASSERT(gtid1._GTSeqNo == 1);
        }

        void testGTIDManager() {
            GTID lastGTID(1,1);
            GTIDManager mgr(lastGTID, 0, 0, 0, 0);
            
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
            uint64_t currHkp = mgr.getHighestKnownPrimary();
            // just a sanity check, that hkp is 2
            ASSERT(currHkp == 2);
            ASSERT(mgr._newPrimaryValue == 0);
            ASSERT(!mgr.resetManager(1));
            ASSERT(!mgr.resetManager(2));
            ASSERT(mgr.resetManager(4));
            mgr.verifyReadyToBecomePrimary();
            // make sure that lastLive and minLive not changed yet
            ASSERT(GTID::cmp(currMin, mgr._minLiveGTID) == 0);
            ASSERT(GTID::cmp(currLast, mgr._lastLiveGTID) == 0);
            // now make sure that primary has increased
            ASSERT(mgr._newPrimaryValue == 4);
            mgr.getGTIDForPrimary(&gtid, &ts, &hash);
            ASSERT(mgr._newPrimaryValue == 0);

            ASSERT(gtid._primarySeqNo > currLast._primarySeqNo);
            ASSERT(gtid._primarySeqNo == 4);
            ASSERT(gtid._GTSeqNo == 0);

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

        void simulateElectionRelatedStuff() {
            GTIDManager mgr(GTID(1,1), 0, 0, 0, 0);
            // make sure initialization is what we expect
            mgr.catchUnappliedToLive();
            GTID resetGTID(2,2);
            mgr.resetAfterInitialSync(resetGTID, 1, 1);

            // first simulation
            ASSERT(GTID::cmp(mgr._lastLiveGTID, GTID(2,2)) == 0);
            GTID p;
            uint64_t ts;
            uint64_t hash;
            mgr.getGTIDForPrimary(&p, &ts, &hash);
            ASSERT(GTID::cmp(p, GTID(2,3)) == 0);
            ASSERT(GTID::cmp(mgr._lastLiveGTID, GTID(2,3)) == 0);
            ASSERT(mgr.getHighestKnownPrimary() == 2);
            mgr.noteLiveGTIDDone(p);
            // simulate a normal GTID coming in that bumps up the highestKnownPrimary
            mgr.noteGTIDAdded(GTID(5,0), 2, 2);
            ASSERT(mgr.getHighestKnownPrimary() == 5);
            mgr.noteApplyingGTID(GTID(5,0));
            mgr.noteGTIDApplied(GTID(5,0));
            // normal GTID coming that does not bump up hkp
            mgr.noteGTIDAdded(GTID(5,2), 2, 2);
            ASSERT(mgr.getHighestKnownPrimary()== 5);
            mgr.noteApplyingGTID(GTID(5,2));
            mgr.noteGTIDApplied(GTID(5,2));

            //
            // test canAcknowledgeGTID and acceptPossiblePrimary
            //

            ASSERT(mgr.canAcknowledgeGTID()); // can acknowledge
            // test possibilities for acceptPossiblePrimary
            // new primary too low
            ASSERT(mgr.acceptPossiblePrimary(3, GTID(5,5)) == VOTE_NO);
            ASSERT(mgr.acceptPossiblePrimary(5, GTID(5,5)) == VOTE_NO);
            ASSERT(mgr.canAcknowledgeGTID());
            // GTID too low
            ASSERT(mgr.acceptPossiblePrimary(6, GTID(5,1)) == VOTE_VETO);
            ASSERT(mgr.canAcknowledgeGTID());
            // both parameters no good
            // in such case, low GTID takes precedence, and we veto
            ASSERT(mgr.acceptPossiblePrimary(5, GTID(5,1)) == VOTE_VETO);
            ASSERT(mgr.canAcknowledgeGTID());
            // boundary case, where it is equal to _lastLiveGTID
            ASSERT(GTID::cmp(GTID(5,2), mgr._lastLiveGTID) == 0);
            ASSERT(mgr.acceptPossiblePrimary(6, GTID(5,2)) == VOTE_YES);
            ASSERT(!mgr.canAcknowledgeGTID());
            ASSERT(mgr.getHighestKnownPrimary()== 6);
            ASSERT(mgr.acceptPossiblePrimary(8, GTID(5,2)) == VOTE_YES);
            ASSERT(!mgr.canAcknowledgeGTID());
            ASSERT(mgr.getHighestKnownPrimary()== 8);
            mgr.noteGTIDAdded(GTID(6,2), 2, 2);
            ASSERT(!mgr.canAcknowledgeGTID());
            mgr.noteGTIDAdded(GTID(7,2), 2, 2);
            ASSERT(!mgr.canAcknowledgeGTID());
            mgr.noteGTIDAdded(GTID(8,2), 2, 2);
            ASSERT(mgr.canAcknowledgeGTID());
            mgr.noteApplyingGTID(GTID(6,2));
            mgr.noteGTIDApplied(GTID(6,2));
            mgr.noteApplyingGTID(GTID(7,2));
            mgr.noteGTIDApplied(GTID(7,2));
            mgr.noteApplyingGTID(GTID(8,2));
            mgr.noteGTIDApplied(GTID(8,2));
            ASSERT(mgr.getHighestKnownPrimary()== 8);
            
            GTIDManager one(GTID(5,5), 0, 0, 0, 4);
            ASSERT(one.getHighestKnownPrimary() == 5);
            GTIDManager two(GTID(2,5), 0, 0, 0, 4);
            ASSERT(two.getHighestKnownPrimary() == 4);

        }

        void run() {
            GTIDtest();
            testGTIDManager();
            simulateElectionRelatedStuff();
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
