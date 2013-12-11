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

#include "mongo/client/dbclientinterface.h"
#include "mongo/client/dbclientcursor.h"
#include "mongo/db/jsobj.h"

namespace mongo {

    /**
     * SpillableVector holds a vector of BSONObjs, and if that vector gets too big (>= maxSize), it
     * starts spilling those objects to a backing collection, to be referenced later.
     *
     * SpillableVector labels each of the spilled entries with an OID and a sequence number in the
     * _id field.  Each entry in the collection is of the form (where n is from a sequence of
     * integers starting with 0 for each new OID):
     *
     *     {
     *         _id: {"oid": ObjectID("..."),
     *               "seq": n},
     *         a: [
     *              ..., // user objects are here
     *            ]
     *     }
     *
     * SpillableVector supports getObjectsOrRef(), which appends to a given BSONObjBuilder either a
     * reference to the OID used to spill objects, or a BSONArray containing the spilled objects.
     *
     * SpillableVector also supports transfer(), which appends its objects to a parent
     * SpillableVector.
     */
    class SpillableVector : boost::noncopyable {
        void (*_writeObjToRef)(BSONObj &);
        vector<BSONObj> _vec;
        size_t _curSize;
        const size_t _maxSize;
        SpillableVector *_parent;
        OID _oid;

        bool _curObjInited;
        BufBuilder _buf;
        long long _seq;
        long long *_curSeqNo;
        scoped_ptr<BSONObjBuilder> _curObjBuilder;
        scoped_ptr<BSONArrayBuilder> _curArrayBuilder;
      public:
        SpillableVector(void (*writeObjToRef)(BSONObj &), size_t maxSize, SpillableVector *parent);

        /** @return true iff there have been no objects appended yet. */
        bool empty() const {
            bool isEmpty = _curSize == 0;
            if (isEmpty) {
                dassert(_vec.empty());
            }
            return isEmpty;
        }

        void append(const BSONObj &o);
        void getObjectsOrRef(BSONObjBuilder &b);
        void transfer();

      private:
        bool spilling() const {
            return _curSize >= _maxSize;
        }
        void initCurObj();
        void finish();
        void spillCurObj();
        void spillOneObject(BSONObj obj);
        void spillAllObjects();
    };

    /**
     * SpillableVectorIterator is a simple way to use a SpillableVector that's already been written.
     *
     * If the document didn't need to be spilled, it'll just have a BSONArray named "a", and
     * SpillableVectorIterator will just iterate over that array.
     *
     * If the document was spilled, it'll have an OID named "refOID", and SpillableVectorIterator
     * will use conn and refNs to look up everything with _id.oid matching refOID and will iterate
     * over each of the returned documents in order, thus reconstructing the originally appended
     * objects.
     *
     * Usage is similar to BSONObjIterator except that it returns BSONObjs rather than BSONElements.
     *
     *     {
     *         // obj was originally created with SpillableVector::getObjectsOrRef
     *         for (SpillableVectorIterator it(obj, conn, ns); it.more(); ++it) {
     *             BSONObj o = *it;
     *             // o is something that was appended to the SpillableVector
     *         }
     *     }
     */
    class SpillableVectorIterator {

        class SpilledIterator {
            scoped_ptr<DBClientCursor> _cursor;
            scoped_ptr<BSONObjIterator> _iter;
            long long _seq;

            void nextObj();

          public:
            SpilledIterator(DBClientBase &conn, const string &refNs, OID oid);
            bool more();
            bool moreInCurrentBatch();
            BSONObj next();
            BSONObj operator*();
        };

        scoped_ptr<BSONObjIterator> _objIter;
        scoped_ptr<SpilledIterator> _spilledIter;

      public:
        SpillableVectorIterator(const BSONObj &obj, DBClientBase &conn, const StringData &refNs);

        bool more() {
            if (_objIter) {
                return _objIter->more();
            } else {
                dassert(_spilledIter);
                return _spilledIter->more();
            }
        }

        bool moreInCurrentBatch() {
            if (_objIter) {
                return true;
            } else {
                dassert(_spilledIter);
                return _spilledIter->moreInCurrentBatch();
            }
        }

        BSONObj next() {
            if (_objIter) {
                return _objIter->next().Obj();
            } else {
                dassert(_spilledIter);
                return _spilledIter->next();
            }
        }

        void operator++() { next(); }
        void operator++(int) { next(); }
        BSONObj operator*() {
            if (_objIter) {
                return (*(*_objIter)).Obj();
            } else {
                dassert(_spilledIter);
                return *(*_spilledIter);
            }
        }
    };

} // namespace mongo
