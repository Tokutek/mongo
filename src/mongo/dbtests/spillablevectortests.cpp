/*
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

#include "mongo/db/jsobj.h"
#include "mongo/db/txn_context.h"

namespace SpillableVectorTests {

    class Base : public stack<shared_ptr<SpillableVector> > {
      protected:
        void push_new(void (*callback)(BSONObj &), size_t maxSize) {
            SpillableVector *parent = empty() ? NULL : top().get();
            shared_ptr<SpillableVector> new_vec(new SpillableVector(callback, maxSize, parent));
            push(new_vec);
        }

      public:
        class WithSpillableVector : boost::noncopyable {
            Base &_b;
          public:
            WithSpillableVector(Base *b, void (*callback)(BSONObj &), size_t maxSize) : _b(*b) {
                _b.push_new(callback, maxSize);
            }
            ~WithSpillableVector() {
                _b.pop();
            }
        };
    };

    class InMemory : Base {
      public:
        static void callback(BSONObj &) {
            FAIL("shouldn't call callback when we're in memory");
        }
        void run() {
            WithSpillableVector v(this, callback, 1<<10);
            for (int i = 0; i < 10; ++i) {
                top()->append(BSON("i" << i));
            }
            BSONObjBuilder b;
            top()->getObjectsOrRef(b);
            BSONObj obj = b.done();
            BSONElement arrElt = obj["a"];
            ASSERT_TRUE(arrElt.ok());
            vector<BSONElement> arr = arrElt.Array();
            for (int i = 0; i < 10; ++i) {
                BSONObj o = arr[i].Obj();
                ASSERT_EQUALS(i, o["i"].numberLong());
            }
        }
    };

    class NotInMemory : Base {
        static int calls;
      public:
        static void callback(BSONObj &o) {
            ASSERT_TRUE(o["a"].ok());
            vector<BSONElement> vec = o["a"].Array();
            ASSERT_EQUALS(50, (int) vec.size());
            calls++;
        }
        void run() {
            calls = 0;
            BSONObj sample = BSON("i" << 1);
            size_t oneObject = sample.objsize();
            // This calculation is very sloppy and is entirely guesswork.
            // I think the raw BSON overhead is 4 bytes, maybe?  The _id is a subdocument so that gets doubled.
            // The header is three 3-byte field names, a 12-byte OID, and an 8-byte int, so 29 bytes.  Call it 32.
            // Each object also has some overhead for the array it's in, let's say another 4 bytes, per object
            // We want to fit 50 objects per stored reference object.  Add 1 for fencepost errors, I guess.
            WithSpillableVector v(this, callback, 8 + (oneObject + 4) * 50 + 32 + 1);
            for (int i = 0; i < 100; ++i) {
                top()->append(BSON("i" << i));
            }
            BSONObjBuilder b;
            top()->getObjectsOrRef(b);
            BSONObj obj = b.done();
            ASSERT_FALSE(obj["a"].ok());
            ASSERT_TRUE(obj["refOID"].ok());
            ASSERT_EQUALS(2, calls); // two chunks
        }
    };
    int NotInMemory::calls = 0;

    class ParentSpillsFirst : Base {
        static int maxSeq;
        static bool parentCheckComplete;
      public:
        static void callback(BSONObj &o) {
            ASSERT_TRUE(o["_id"].ok());
            BSONObj idObj = o["_id"].Obj();
            ASSERT_TRUE(idObj["seq"].ok());
            long long seq = idObj["seq"].numberLong();
            if (maxSeq > seq) {
                // We are going backwards, so this must be the parent object.
                ASSERT_TRUE(o["a"].ok());
                vector<BSONElement> arr = o["a"].Array();
                ASSERT_EQUALS(1, (int) arr.size());
                BSONObj obj = arr[0].Obj();
                ASSERT_TRUE(obj["i"].ok());
                ASSERT_EQUALS(0, obj["i"].numberInt());
                parentCheckComplete = true;
            }
            else if (maxSeq >= 0) {
                ASSERT_LESS_THAN(maxSeq, seq);
                maxSeq = seq;
            }
            else {
                maxSeq = seq;
            }
        }
        void run() {
            maxSeq = -1;
            parentCheckComplete = false;
            WithSpillableVector parent(this, callback, 100);
            top()->append(BSON("i" << 0));

            {
                ASSERT_EQUALS(-1, maxSeq);
                WithSpillableVector child(this, callback, 100);
                for (int i = 0; i < 50; ++i) {
                    top()->append(BSON("i" << 100 + i));
                }
                ASSERT_LESS_THAN(1, maxSeq);
                top()->transfer();
            }
            BSONObjBuilder b;
            top()->getObjectsOrRef(b);
            BSONObj obj = b.done();
            ASSERT_FALSE(obj["a"].ok());
            ASSERT_TRUE(obj["refOID"].ok());
            ASSERT_LESS_THAN(2, maxSeq);
            ASSERT_TRUE(parentCheckComplete);
        }
    };
    int ParentSpillsFirst::maxSeq = -1;
    bool ParentSpillsFirst::parentCheckComplete = false;

    class ParentForcesChildSpill : Base {
        static int lastI;
      public:
        static void callback(BSONObj &o) {
            ASSERT_TRUE(o["a"].ok());
            vector<BSONElement> arr = o["a"].Array();
            for (vector<BSONElement>::const_iterator it = arr.begin(); it != arr.end(); ++it) {
                BSONObj obj = it->Obj();
                ASSERT_TRUE(obj["i"].ok());
                int i = obj["i"].numberInt();
                ASSERT_LESS_THAN(lastI, i);
                lastI = i;
            }
        }
        void run() {
            lastI = -1;
            WithSpillableVector parent(this, callback, 100);
            for (int i = 0; i < 50; ++i) {
                top()->append(BSON("i" << i));
            }
            {
                WithSpillableVector child(this, callback, 100);
                top()->append(BSON("i" << 100));
                ASSERT_GREATER_THAN(100, lastI);
                top()->transfer();
            }
            BSONObjBuilder b;
            top()->getObjectsOrRef(b);
            ASSERT_EQUALS(100, lastI);
        }
    };
    int ParentForcesChildSpill::lastI = -1;

    class ConcurrentVectorsGetDifferentOids {
        Base b1;
        Base b2;
        static OID oid1, oid2;
      public:
        static void callback1(BSONObj &o) {
            ASSERT_TRUE(o["_id"].ok());
            BSONObj idObj = o["_id"].Obj();
            ASSERT_TRUE(idObj["oid"].ok());
            OID oid = idObj["oid"].OID();
            static OID nullOID;
            if (oid1 == nullOID) {
                oid1 = oid;
            }
            else {
                ASSERT_EQUALS(oid1, oid);
            }
            ASSERT_NOT_EQUALS(oid2, oid);
        }
        static void callback2(BSONObj &o) {
            ASSERT_TRUE(o["_id"].ok());
            BSONObj idObj = o["_id"].Obj();
            ASSERT_TRUE(idObj["oid"].ok());
            OID oid = idObj["oid"].OID();
            static OID nullOID;
            if (oid2 == nullOID) {
                oid2 = oid;
            }
            else {
                ASSERT_EQUALS(oid2, oid);
            }
            ASSERT_NOT_EQUALS(oid1, oid);
        }
        void run() {
            Base::WithSpillableVector v1(&b1, callback1, 1);
            Base::WithSpillableVector v2(&b2, callback2, 1);
            for (int i = 0; i < 10; ++i) {
                b1.top()->append(BSON("i" << i));
                b2.top()->append(BSON("i" << i));
            }
        }
    };
    OID ConcurrentVectorsGetDifferentOids::oid1;
    OID ConcurrentVectorsGetDifferentOids::oid2;

    class ChildAbort : Base {
      public:
        static void callback(BSONObj &) {
            FAIL("shouldn't call callback when we're in memory");
        }
        void run() {
            WithSpillableVector v(this, callback, 1<<10);
            for (int i = 0; i < 10; ++i) {
                top()->append(BSON("i" << i));
            }
            {
                WithSpillableVector v(this, callback, 1<<10);
                for (int i = 0; i < 10; ++i) {
                    top()->append(BSON("j" << i));
                }
            }
            BSONObjBuilder b;
            top()->getObjectsOrRef(b);
            BSONObj obj = b.done();
            BSONElement arrElt = obj["a"];
            ASSERT_TRUE(arrElt.ok());
            vector<BSONElement> arr = arrElt.Array();
            ASSERT_EQUALS(10, (int) arr.size());
            int i = 0;
            for (vector<BSONElement>::const_iterator it = arr.begin(); it != arr.end(); ++it, ++i) {
                BSONObj o = it->Obj();
                ASSERT_FALSE(o.hasField("j"));
                ASSERT_TRUE(o.hasField("i"));
                ASSERT_EQUALS(i, o["i"].numberLong());
            }
        }
    };

    class All : public Suite {
      public:
        All() : Suite("spillablevector") {}
        void setupTests() {
            add<InMemory>();
            add<NotInMemory>();
            add<ParentSpillsFirst>();
            add<ParentForcesChildSpill>();
            add<ConcurrentVectorsGetDifferentOids>();
            add<ChildAbort>();
        }
    } all;

}
