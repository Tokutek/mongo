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

#include "mongo/db/d_concurrency.h"
#include "mongo/db/index.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_details.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/stringutils.h"

namespace mongo {

    NamespaceDetails::Indexer::Indexer(NamespaceDetails *d, const BSONObj &info) :
        _d(d), _isSecondaryIndex(_d->_nIndexes > 0), _multiKey(false) {

        Lock::assertWriteLocked(_d->_ns);

        const StringData &name = info["name"].Stringdata();
        const BSONObj &keyPattern = info["key"].Obj();

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

        // The first index we create should be the pk index, when we first create the collection.
        if (!_isSecondaryIndex) {
            massert(16435, "first index should be pk index", keyPattern == _d->_pk);
        }

        // Note this ns in the rollback so if this transaction aborts, we'll
        // close this ns, forcing the next user to reload in-memory metadata.
        NamespaceIndexRollback &rollback = cc().txn().nsIndexRollback();
        rollback.noteNs(_d->_ns);

        shared_ptr<IndexDetails> idx(new IndexDetails(info));
        try {
            // Throws if we're creating an invalid index, which needs to
            // be checked now since the ns might be created by this txn
            // and a bad index would mean the create should be rolled back.
            idx->getSpec();
        } catch (...) {
            idx->close();
            throw;
        }

        // The primary key doesn't need to be built - there's no data.
        if (_isSecondaryIndex) {
            // Give the underlying DB a pointer to the multikey bool, which 
            // will be set during index creation if multikeys are generated.
            // see storage::generate_keys()
            idx->db()->app_private = &_multiKey;

            _indexer.reset( new storage::Indexer( _d->getPKIndex().db(), idx->db()) );
        }
        _d->_indexes.push_back(idx);
        _d->_indexBuildInProgress = true;
    }

    NamespaceDetails::Indexer::~Indexer() {
        Lock::assertWriteLocked(_d->_ns);

        // Destroy any indexer that may exist.
        _indexer.reset();
        if (_d->_indexBuildInProgress) {
            shared_ptr<IndexDetails> idx = _d->_indexes.back();
            idx->db()->app_private = NULL;
            _d->_indexes.pop_back();
            _d->_indexBuildInProgress = false;
            verify(_d->_nIndexes == (int) _d->_indexes.size());
            try {
                idx->close();
            } catch (const DBException &e) {
                TOKULOG(0) << "Caught DBException exception while destroying Indexer: "
                           << e.getCode() << ", " << e.what() << endl;
            } catch (...) {
                TOKULOG(0) << "Caught generic exception while destroying Indexer." << endl;
            }
        }
    }

    void NamespaceDetails::Indexer::build() {
        Lock::assertAtLeastReadLocked(_d->_ns);
        if (_indexer.get() != NULL) {
            const int r = _indexer->build();
            if (r != 0) {
                storage::handle_ydb_error(r);
            }

            // If the index is unique, check all adjacent keys for a duplicate.
            IndexDetails &idx = *_d->_indexes[_d->_nIndexes];
            if (idx.unique()) {
                _d->checkIndexUniqueness(idx);
            }
        } 
    }

    void NamespaceDetails::Indexer::commit() {
        Lock::assertWriteLocked(_d->_ns);

        // Bumping the index count "commits" this index to the set.
        // Setting _indexBuildInProgress to false prevents us from
        // rolling back the index creation in the destructor.
        _d->_indexes[_d->_nIndexes]->db()->app_private = NULL;
        _d->_indexBuildInProgress = false;
        _d->_nIndexes++;
        if (_indexer.get() != NULL) {
            const int r = _indexer->close();
            if (r != 0) {
                storage::handle_ydb_error(r);
            }
        }

        if (_multiKey) {
            _d->setIndexIsMultikey(_d->_nIndexes - 1);
        }
        nsindex(_d->_ns)->update_ns(_d->_ns, _d->serialize(), _isSecondaryIndex);
        _d->resetTransient();
    }

} // namespace mongo
