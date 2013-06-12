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
    };

    class InMemory : Base {
      public:
        static void callback(BSONObj &) {
            ASSERT(false);  // shouldn't be calling callback
        }
        void run() {
            push_new(callback, 1<<10);
            SpillableVector &vec = *top();
            for (int i = 0; i < 10; ++i) {
                vec.append(BSON("i" << i));
            }
            BSONObjBuilder b;
            vec.getObjectsOrRef(b);
            BSONObj obj = b.done();
            BSONElement arrElt = obj["a"];
            ASSERT(arrElt.ok());
            vector<BSONElement> arr = arrElt.Array();
            for (int i = 0; i < 10; ++i) {
                BSONObj o = arr[i].Obj();
                ASSERT_EQUALS(i, o["i"].numberLong());
            }
        }
    };

    class All : public Suite {
      public:
        All() : Suite("spillablevector") {}
        void setupTests() {
            add<InMemory>();
        }
    } all;

}
