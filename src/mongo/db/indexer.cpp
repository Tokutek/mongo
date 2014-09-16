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
#include "mongo/db/cursor.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/index.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/collection.h"
#include "mongo/db/collection_map.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/progress_meter.h"
#include "mongo/util/stringutils.h"

namespace mongo {

    CollectionBase::IndexerBase::IndexerBase(CollectionBase *cl, const BSONObj &info) :
        _cl(cl), _info(info), _isSecondaryIndex(_cl->_nIndexes > 0) {
        if (!cc().creatingSystemUsers() &&
            !cc().upgradingDiskFormatVersion()) {
            std::string sourceNS = info["ns"].String();
            uassert(16548,
                    mongoutils::str::stream() << "not authorized to create index on " << sourceNS,
                    cc().getAuthorizationManager()->checkAuthorization(sourceNS,
                                                                       ActionType::ensureIndex));
        }
    }

    CollectionBase::IndexerBase::~IndexerBase() {
        Lock::assertWriteLocked(_cl->_ns);

        if (_idx && _cl->_indexBuildInProgress) {
            verify(_idx.get() == _cl->_indexes.back().get());
            // Pop back the index from the index vector. We still
            // have a shared pointer (_idx), so it won't close here.
            _cl->_indexes.pop_back();
            _cl->_indexBuildInProgress = false;
            verify(_cl->_nIndexes == (int) _cl->_indexes.size());
            // If we catch any exceptions, eat them. We can only enter this block
            // if we're already propogating an exception (ie: not under normal
            // operation) so it's okay to just print to the log and continue.
            try {
                _idx->close();
            } catch (const DBException &e) {
                TOKULOG(0) << "Caught DBException exception while destroying IndexerBase: "
                           << e.getCode() << ", " << e.what() << endl;
            } catch (...) {
                TOKULOG(0) << "Caught generic exception while destroying IndexerBase." << endl;
            }
        } else {
            // the indexer is destructing before it got a chance to actually
            // build anything, which is the case if prepare() throws before
            // creating the indexer and setting _cl->_indexBuildInProgress, etc.
        }
    }

    void CollectionBase::IndexerBase::prepare() {
        Lock::assertWriteLocked(_cl->_ns);

        const BSONObj &keyPattern = _info["key"].Obj();

        // The first index we create should be the pk index, when we first create the collection.
        if (!_isSecondaryIndex) {
            massert(16923, "first index should be pk index", keyPattern == _cl->_pk);
        }

        // Store the index in the _indexes array so that others know an
        // index with this name / key pattern exists and is being built.
        _idx = IndexInterface::make(_info);
        _cl->_indexes.push_back(_idx);
        _cl->_indexBuildInProgress = true;

        _prepare();
    }

    void CollectionBase::IndexerBase::commit() {
        Lock::assertWriteLocked(_cl->_ns);

        _commit();

        // Bumping the index count "commits" this index to the set.
        // Setting _indexBuildInProgress to false prevents us from
        // rolling back the index creation in the destructor.
        _cl->_indexBuildInProgress = false;
        _cl->_nIndexes++;
    }

    CollectionBase::HotIndexer::HotIndexer(CollectionBase *cl, const BSONObj &info) :
        CollectionBase::IndexerBase(cl, info) {
    }

    void CollectionBase::HotIndexer::_prepare() {
        verify(_idx.get() != NULL);
        // The primary key doesn't need to be built - there's no data.
        if (_isSecondaryIndex) {
            // Give the underlying DB a pointer to the multikey bool, which 
            // will be set during index creation if multikeys are generated.
            // see storage::generate_keys()
            _multiKeyTracker.reset(new MultiKeyTracker(_idx->db()));
            _indexer.reset(new storage::Indexer(_cl->getPKIndexBase().db(), _idx->db(),
                                                str::stream() << "Background index build progress for "
                                                              << _cl->_ns << ", key "
                                                              << _idx->keyPattern()));
        }
    }

    void CollectionBase::HotIndexer::build() {
        if (_indexer.get() != NULL) {
            const int r = _indexer->build();
            if (r != 0) {
                storage::handle_ydb_error(r);
            }

            // If the index is unique, check all adjacent keys for a duplicate.
            if (_idx->unique()) {
                _cl->checkIndexUniqueness(*_idx.get());
            }
        } 
    }

    void CollectionBase::HotIndexer::_commit() {
        if (_indexer.get() != NULL) {
            const int r = _indexer->close();
            if (r != 0) {
                storage::handle_ydb_error(r);
            }
            if (_multiKeyTracker->isMultiKey()) {
                bool indexBitChanged;
                _cl->setIndexIsMultikey(_cl->idxNo(*_idx.get()), &indexBitChanged);
            }
        }
    }

    CollectionBase::ColdIndexer::ColdIndexer(CollectionBase *cl, const BSONObj &info) :
        CollectionBase::IndexerBase(cl, info) {
    }

    void CollectionBase::ColdIndexer::build() {
        Lock::assertWriteLocked(_cl->_ns);
        if (_isSecondaryIndex) {
            IndexInterface::Builder builder(*_idx);

            IndexDetails::Stats idxStats = _cl->getPKIndex().getStats();
            ProgressMeter pm(idxStats.count, 3, 1000, "estimated documents",
                             mongoutils::str::stream() << "Foreground index build progress (collect phase) for "
                                                       << _cl->_ns << ", key "
                                                       << _idx->keyPattern());

            for (shared_ptr<Cursor> cursor(Cursor::make(_cl, 1, false));
                 cursor->ok(); cursor->advance()) {
                BSONObj pk = cursor->currPK();
                BSONObj obj = cursor->current();
                BSONObjSet keys;
                _idx->getKeysFromObject(obj, keys);
                if (keys.size() > 1) {
                    bool indexBitChanged;
                    _cl->setIndexIsMultikey(_cl->idxNo(*_idx.get()), &indexBitChanged);
                }
                for (BSONObjSet::const_iterator ki = keys.begin(); ki != keys.end(); ++ki) {
                    builder.insertPair(*ki, &pk, obj);
                }
                if (pm.hit() && cc().curop()) {
                    std::string status = pm.toString();
                    cc().curop()->setMessage(status.c_str());
                }
                killCurrentOp.checkForInterrupt(); // uasserts if we should stop
            }

            pm.finished();
            builder.done();

            // If the index is unique, check all adjacent keys for a duplicate.
            if (_idx->unique()) {
                _cl->checkIndexUniqueness(*_idx);
            }
        }
    }

} // namespace mongo
