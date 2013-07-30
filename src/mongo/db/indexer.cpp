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

#include "mongo/db/namespace_details.h"

namespace mongo {

    NamespaceDetails::Indexer::Indexer(NamespaceDetails *d, const BSONObj &info) :
        _d(d), _info(info), _isSecondaryIndex(_d->_nIndexes > 0) {

        Lock::assertWriteLocked(_d->_ns);

        const StringData &name = _info["name"].Stringdata();
        const BSONObj &keyPattern = _info["key"].Obj();

        uassert(16449, str::stream() << "dropDups is not supported and is likely to remain "
                       << "unsupported for some time because it deletes arbitrary data",
                       !info["dropDups"].trueValue());

        uassert(12588, "cannot add index with a hot index build in progress",
                       !_d->_indexBuildInProgress);

        uassert(12523, "no index name specified",
                        info["name"].ok());

        uassert(16753, str::stream() << "index with name " << name << " already exists",
                       _d->findIndexByName(name) < 0);

        uassert(16754, str::stream() << "index already exists with diff name " <<
                       name << " " << keyPattern.toString(),
                       _d->findIndexByKeyPattern(keyPattern) < 0);

        uassert(12505, str::stream() << "add index fails, too many indexes for " <<
                       name << " key:" << keyPattern.toString(),
                       _d->nIndexes() < NIndexesMax);

        // Note this ns in the rollback so if this transaction aborts, we'll
        // close this ns, forcing the next user to reload in-memory metadata.
        NamespaceIndexRollback &rollback = cc().txn().nsIndexRollback();
        rollback.noteNs(_d->_ns);

        shared_ptr<IndexDetails> idx(new IndexDetails(_info));
        try {
            // Throws if we're creating an invalid index, which needs to
            // be checked now since the ns might be created by this txn
            // and a bad index would mean the create should be rolled back.
            idx->getSpec();
        } catch (...) {
            idx->close();
            throw;
        }
        _d->_indexes.push_back(idx);
        _d->_indexBuildInProgress = true;
    }

    NamespaceDetails::Indexer::~Indexer() {
        if (_d->_indexBuildInProgress) {
            _d->_indexes[_d->_nIndexes]->close();
            _d->_indexes.pop_back();
            _d->_indexBuildInProgress = false;
            verify(_d->_nIndexes == (int) _d->_indexes.size());
        }
    }

    void NamespaceDetails::Indexer::build() {
        Lock::assertAtLeastReadLocked(_d->_ns);

        if (_isSecondaryIndex) {
            IndexDetails &idx = *_d->_indexes[_d->_nIndexes];
            // The primary key doesn't need to be built - there's no data.
            _d->buildIndex(idx);
        } 
    }

    void NamespaceDetails::Indexer::commit() {
        Lock::assertWriteLocked(_d->_ns);

        // Bumping the index count "commits" this index to the set.
        // Setting _indexBuildInProgress to false prevents us from
        // rolling back the index creation in the destructor.
        _d->_indexBuildInProgress = false;
        _d->_nIndexes++;

        // The first index we create should be the pk index, when we first create the collection.
        // Therefore the collection's NamespaceDetails should not already exist in the NamespaceIndex
        // unless we are building a secondary index (and therefore the collection already exists)
        if (!_isSecondaryIndex) {
            massert(16435, "first index should be pk index", _info["key"].Obj() == _d->_pk);
        }
        nsindex(_d->_ns)->update_ns(_d->_ns, _d->serialize(), _isSecondaryIndex);
        NamespaceDetailsTransient::get(_d->_ns).addedIndex();
    }

} // namespace mongo
