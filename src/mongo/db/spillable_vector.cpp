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

#include "mongo/db/spillable_vector.h"

#include "mongo/pch.h"

#include "mongo/util/mongoutils/str.h"

namespace mongo {

    SpillableVector::SpillableVector(void (*writeObjToRef)(BSONObj &), size_t maxSize, SpillableVector *parent)
            : _writeObjToRef(writeObjToRef),
              _vec(),
              _curSize(0),
              _maxSize(maxSize),
              _parent(parent),
              _oid(_parent == NULL ? OID::gen() : _parent->_oid),
              _curObjInited(false),
              _buf(),
              _seq(0),
              _curSeqNo(_parent == NULL ? &_seq : _parent->_curSeqNo),
              _curObjBuilder(),
              _curArrayBuilder()
    {}

    void SpillableVector::append(const BSONObj &o) {
        BSONObj obj = o.getOwned();
        bool wasSpilling = spilling();
        _curSize += obj.objsize();
        if (!wasSpilling && spilling()) {
            spillAllObjects();
        }
        if (spilling()) {
            spillOneObject(obj);
        }
        else {
            _vec.push_back(obj.getOwned());
        }
    }

    void SpillableVector::getObjectsOrRef(BSONObjBuilder &b) {
        finish();
        dassert(_parent == NULL);
        if (spilling()) {
            b.append("refOID", _oid);
        }
        else {
            b.append("a", _vec);
        }
    }

    void SpillableVector::transfer() {
        finish();
        dassert(_parent != NULL);
        if (!spilling()) {
            // If your parent is spilling, or is about to start spilling, we'll take care of
            // spilling these in a moment.
            _parent->_vec.insert(_parent->_vec.end(), _vec.begin(), _vec.end());
        }
        _parent->_curSize += _curSize;
        if (_parent->spilling()) {
            // If your parent wasn't spilling before, this will spill their objects and yours in the
            // right order.  If they were already spilling, this will just spill your objects now.
            _parent->spillAllObjects();
        }
    }

    void SpillableVector::finish() {
        if (spilling()) {
            spillCurObj();
        }
    }

    void SpillableVector::initCurObj() {
        _curObjBuilder.reset(new BSONObjBuilder(_buf));
        _curObjBuilder->append("_id", BSON("oid" << _oid << "seq" << (*_curSeqNo)++));
        _curArrayBuilder.reset(new BSONArrayBuilder(_curObjBuilder->subarrayStart("a")));
    }

    void SpillableVector::spillCurObj() {
        if (_curArrayBuilder->arrSize() == 0) {
            return;
        }
        _curArrayBuilder->doneFast();
        BSONObj curObj = _curObjBuilder->done();
        _writeObjToRef(curObj);
    }

    void SpillableVector::spillOneObject(BSONObj obj) {
        if (!_curObjInited) {
            initCurObj();
            _curObjInited = true;
        }
        if (_curObjBuilder->len() + obj.objsize() >= (long long) _maxSize) {
            spillCurObj();
            _buf.reset();
            initCurObj();
        }
        _curArrayBuilder->append(obj);
    }

    void SpillableVector::spillAllObjects() {
        if (_parent != NULL) {
            // Your parent must spill anything they have before you do, to get the sequence numbers
            // right.
            _parent->spillAllObjects();
        }
        for (vector<BSONObj>::iterator it = _vec.begin(); it != _vec.end(); ++it) {
            spillOneObject(*it);
        }
        _vec.clear();
    }

    SpillableVectorIterator::SpillableVectorIterator(const BSONObj &obj, DBClientBase &conn, const StringData &refNs) {
        if (obj.hasField("a")) {
            _objIter.reset(new BSONObjIterator(obj["a"].Obj()));
        } else {
            massert(17226, mongoutils::str::stream() << "spilled vector object " << obj << " doesn't have a proper 'refOID' or 'a' field", obj.hasField("refOID"));
            _spilledIter.reset(new SpilledIterator(conn, refNs.toString(), obj["refOID"].OID()));
        }
    }

    SpillableVectorIterator::SpilledIterator::SpilledIterator(DBClientBase &conn, const string &refNs, OID oid)
            : _cursor(conn.query(refNs.c_str(), QUERY("_id.oid" << oid).hint(BSON("_id" << 1))).release()), _seq(0) {}

    void SpillableVectorIterator::SpilledIterator::nextObj() {
        verify(_cursor->more());
        BSONObj obj = _cursor->nextSafe();
        BSONElement seqElt = obj.getFieldDotted("_id.seq");
        verify(seqElt.Long() == _seq);
        ++_seq;
        _iter.reset(new BSONObjIterator(obj["a"].Obj()));
    }

    bool SpillableVectorIterator::SpilledIterator::more() {
        if (_iter && _iter->more()) {
            return true;
        } else {
            return _cursor->more();
        }
    }

    bool SpillableVectorIterator::SpilledIterator::moreInCurrentBatch() {
        if (_iter && _iter->more()) {
            return true;
        } else {
            return _cursor->moreInCurrentBatch();
        }
    }

    BSONObj SpillableVectorIterator::SpilledIterator::next() {
        if (_iter && _iter->more()) {
            return _iter->next().Obj();
        } else {
            nextObj();
            return next();
        }
    }

    BSONObj SpillableVectorIterator::SpilledIterator::operator*() {
        if (_iter && _iter->more()) {
            return (*(*_iter)).Obj();
        } else {
            nextObj();
            return *(*this);
        }
    }

} // namespace mongo
