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

#include "mongo/base/string_data.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/client.h"
#include "mongo/db/curop.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/index.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_details.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/stringutils.h"

namespace mongo {

    NamespaceDetails::Indexer::Indexer(NamespaceDetails *d, const BSONObj &info) :
        _d(d), _info(info), _isSecondaryIndex(_d->_nIndexes > 0) {
        if (!cc().creatingSystemUsers()) {
            std::string sourceNS = info["ns"].String();
            uassert(16548,
                    mongoutils::str::stream() << "not authorized to create index on " << sourceNS,
                    cc().getAuthorizationManager()->checkAuthorization(sourceNS,
                                                                       ActionType::ensureIndex));
        }
    }

    NamespaceDetails::Indexer::~Indexer() {
        Lock::assertWriteLocked(_d->_ns);

        if (_d->_indexBuildInProgress) {
            verify(_idx.get() == _d->_indexes.back().get());
            // Pop back the index from the index vector. We still
            // have a shared pointer (_idx), so it won't close here.
            _d->_indexes.pop_back();
            _d->_indexBuildInProgress = false;
            verify(_d->_nIndexes == (int) _d->_indexes.size());
            // If we catch any exceptions, eat them. We can only enter this block
            // if we're already propogating an exception (ie: not under normal
            // operation) so it's okay to just print to the log and continue.
            try {
                _idx->close();
            } catch (const DBException &e) {
                TOKULOG(0) << "Caught DBException exception while destroying Indexer: "
                           << e.getCode() << ", " << e.what() << endl;
            } catch (...) {
                TOKULOG(0) << "Caught generic exception while destroying Indexer." << endl;
            }
        }
    }

    void NamespaceDetails::Indexer::prepare() {
        Lock::assertWriteLocked(_d->_ns);

        const StringData &name = _info["name"].Stringdata();
        const BSONObj &keyPattern = _info["key"].Obj();

        uassert(16922, str::stream() << "dropDups is not supported and is likely to remain "
                       << "unsupported for some time because it deletes arbitrary data",
                       !_info["dropDups"].trueValue());

        uassert(12588, "cannot add index with a hot index build in progress",
                       !_d->_indexBuildInProgress);

        uassert(12523, "no index name specified",
                        _info["name"].ok());

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
            massert(16923, "first index should be pk index", keyPattern == _d->_pk);
        }

        // Note this ns in the rollback so if this transaction aborts, we'll
        // close this ns, forcing the next user to reload in-memory metadata.
        NamespaceIndexRollback &rollback = cc().txn().nsIndexRollback();
        rollback.noteNs(_d->_ns);

        // Store the index in the _indexes array so that others know an
        // index with this name / key pattern exists and is being built.
        _idx = IndexDetails::make(_info);
        _d->_indexes.push_back(_idx);
        _d->_indexBuildInProgress = true;

        _prepare();
    }

    void NamespaceDetails::Indexer::commit() {
        Lock::assertWriteLocked(_d->_ns);

        _commit();

        // Bumping the index count "commits" this index to the set.
        // Setting _indexBuildInProgress to false prevents us from
        // rolling back the index creation in the destructor.
        _d->_indexBuildInProgress = false;
        _d->_nIndexes++;

        // Pass true for includeHotIndex to serialize()
        nsindex(_d->_ns)->update_ns(_d->_ns, _d->serialize(true), _isSecondaryIndex);
        _d->resetTransient();
    }

    NamespaceDetails::HotIndexer::HotIndexer(NamespaceDetails *d, const BSONObj &info) :
        NamespaceDetails::Indexer(d, info) {
    }

    void NamespaceDetails::HotIndexer::_prepare() {
        verify(_idx.get() != NULL);
        // The primary key doesn't need to be built - there's no data.
        if (_isSecondaryIndex) {
            // Give the underlying DB a pointer to the multikey bool, which 
            // will be set during index creation if multikeys are generated.
            // see storage::generate_keys()
            _multiKeyTracker.reset(new MultiKeyTracker(_idx->db()));
            _indexer.reset(new storage::Indexer(_d->getPKIndex().db(), _idx->db()));
            _indexer->setPollMessagePrefix(str::stream() << "Hot index build progress: "
                                                         << _d->_ns << ", key "
                                                         << _idx->keyPattern()
                                                         << ":");
        }
    }

    void NamespaceDetails::HotIndexer::build() {
        Lock::assertAtLeastReadLocked(_d->_ns);

        if (_indexer.get() != NULL) {
            const int r = _indexer->build();
            if (r != 0) {
                storage::handle_ydb_error(r);
            }

            // If the index is unique, check all adjacent keys for a duplicate.
            if (_idx->unique()) {
                _d->checkIndexUniqueness(*_idx.get());
            }
        } 
    }

    void NamespaceDetails::HotIndexer::_commit() {
        if (_indexer.get() != NULL) {
            const int r = _indexer->close();
            if (r != 0) {
                storage::handle_ydb_error(r);
            }
            if (_multiKeyTracker->isMultiKey()) {
                _d->setIndexIsMultikey(_d->idxNo(*_idx.get()));
            }
        }
    }

    NamespaceDetails::ColdIndexer::ColdIndexer(NamespaceDetails *d, const BSONObj &info) :
        NamespaceDetails::Indexer(d, info) {
    }

    void NamespaceDetails::ColdIndexer::build() {
        Lock::assertWriteLocked(_d->_ns);
        if (_isSecondaryIndex) {
            IndexDetails::Builder builder(*_idx);

            const int indexNum = _d->idxNo(*_idx);
            for (shared_ptr<Cursor> cursor(BasicCursor::make(_d));
                 cursor->ok(); cursor->advance()) {
                BSONObj pk = cursor->currPK();
                BSONObj obj = cursor->current();
                BSONObjSet keys;
                _idx->getKeysFromObject(obj, keys);
                if (keys.size() > 1) {
                    _d->setIndexIsMultikey(indexNum);
                }
                for (BSONObjSet::const_iterator ki = keys.begin(); ki != keys.end(); ++ki) {
                    builder.insertPair(*ki, &pk, obj);
                }
                killCurrentOp.checkForInterrupt(); // uasserts if we should stop
            }

            builder.done();

            // If the index is unique, check all adjacent keys for a duplicate.
            if (_idx->unique()) {
                _d->checkIndexUniqueness(*_idx);
            }
        }
    }

} // namespace mongo
