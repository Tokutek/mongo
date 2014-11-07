/**
 *    Copyright (C) 2008 10gen Inc.
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

#include "mongo/db/curop.h"
#include "mongo/db/cursor.h"
#include "mongo/db/kill_current_op.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/collection.h"
#include "mongo/db/storage/exception.h"

namespace mongo {

    //
    // The centralized factories for creating cursors over collections.
    //

    shared_ptr<Cursor> Cursor::make(CollectionData *cd, const int direction, const bool countCursor) {
        return cd->makeCursor(direction, countCursor);
    }

    shared_ptr<Cursor> Cursor::make(Collection *cl, const int direction,
                                    const bool countCursor) {
        if (cl != NULL) {
            CollectionData* cd = cl->as<CollectionData>();
            return Cursor::make(cd, direction, countCursor);
        } else {
            return shared_ptr<Cursor>(new DummyCursor(direction));
        }
    }

    shared_ptr<Cursor> Cursor::make(CollectionData *cd, const IndexDetails &idx,
                                    const int direction,
                                    const bool countCursor) {
        return cd->makeCursor(idx, direction, countCursor);
    }

    shared_ptr<Cursor> Cursor::make(Collection *cl, const IndexDetails &idx,
                                    const int direction,
                                    const bool countCursor) {
        CollectionData* cd = cl->as<CollectionData>();
        return Cursor::make(cd, idx, direction, countCursor);
    }

    shared_ptr<Cursor> Cursor::make(CollectionData *cd, const IndexDetails &idx,
                                    const BSONObj &startKey, const BSONObj &endKey,
                                    const bool endKeyInclusive,
                                    const int direction, const int numWanted,
                                    const bool countCursor) {
        return cd->makeCursor(idx, startKey, endKey, endKeyInclusive, direction, numWanted, countCursor);
    }

    shared_ptr<Cursor> Cursor::make(Collection *cl, const IndexDetails &idx,
                                    const BSONObj &startKey, const BSONObj &endKey,
                                    const bool endKeyInclusive,
                                    const int direction, const int numWanted,
                                    const bool countCursor) {
        CollectionData* cd = cl->as<CollectionData>();
        return Cursor::make(cd, idx, startKey, endKey, endKeyInclusive, 
                            direction, numWanted, countCursor);
    }

    shared_ptr<Cursor> Cursor::make(CollectionData *cd, const IndexDetails &idx,
                                    const shared_ptr<FieldRangeVector> &bounds,
                                    const int singleIntervalLimit,
                                    const int direction, const int numWanted,
                                    const bool countCursor) {
        return cd->makeCursor(idx, bounds, singleIntervalLimit, direction, numWanted, countCursor);
    }

    shared_ptr<Cursor> Cursor::make(Collection *cl, const IndexDetails &idx,
                                    const shared_ptr<FieldRangeVector> &bounds,
                                    const int singleIntervalLimit,
                                    const int direction, const int numWanted,
                                    const bool countCursor) {
        CollectionData* cd = cl->as<CollectionData>();
        return Cursor::make(cd, idx, bounds, singleIntervalLimit, direction, numWanted, countCursor);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    bool ScanCursor::reverseMinMaxBoundsOrder(const Ordering &ordering, const int direction) {
        // Only the first field's direction matters, because this function is only called
        // to possibly reverse bounds ordering with min/max key, which is single field.
        const bool ascending = !ordering.descending(1);
        const bool forward = direction > 0;
        // We need to reverse the order if exactly one of the query or the index are descending.  If
        // both are descending, the normal order is fine.
        return ascending != forward;
    }

    const BSONObj &ScanCursor::startKey(const BSONObj &keyPattern, const int direction) {
        // Scans intuitively start at minKey, but may need to be reversed to maxKey.
        return reverseMinMaxBoundsOrder(Ordering::make(keyPattern), direction) ? maxKey : minKey;
    }

    const BSONObj &ScanCursor::endKey(const BSONObj &keyPattern, const int direction) {
        // Scans intuitively end at maxKey, but may need to be reversed to minKey.
        return reverseMinMaxBoundsOrder(Ordering::make(keyPattern), direction) ? minKey : maxKey;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    IndexScanCursor::IndexScanCursor( CollectionData *cl, const IndexDetails &idx,
                                      int direction, int numWanted ) :
        IndexCursor( cl, idx,
                     ScanCursor::startKey(idx.keyPattern(), direction),
                     ScanCursor::endKey(idx.keyPattern(), direction),
                     true, direction, numWanted ) {
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    BasicCursor::BasicCursor( CollectionData *cl, int direction ) :
        IndexScanCursor( cl, cl->getPKIndex(), direction ) {
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    IndexCountCursor::IndexCountCursor( CollectionData *cl, const IndexDetails &idx,
                                        const BSONObj &startKey, const BSONObj &endKey,
                                        const bool endKeyInclusive ) :
        IndexCursor(cl, idx, startKey, endKey, endKeyInclusive, 1, 0),
        _bufferedRowCount(0),
        _exhausted(false),
        _endSKeyPrefix(_endKey, NULL) {
        TOKULOG(3) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
        checkAssumptionsAndInit();
    }

    IndexCountCursor::IndexCountCursor( CollectionData *cl, const IndexDetails &idx,
                                        const shared_ptr< FieldRangeVector > &bounds ) :
        // This will position the cursor correctly using bounds, which will do the right
        // thing based on bounds->start/endKey() and bounds->start/endKeyInclusive()
        IndexCursor(cl, idx, bounds, false, 1, 0),
        _bufferedRowCount(0),
        _exhausted(false),
        _endSKeyPrefix(_endKey, NULL) {
        TOKULOG(3) << toString() << ": constructor: bounds " << prettyIndexBounds() << endl;
        dassert(_startKey == bounds->startKey());
        dassert(_endKey == bounds->endKey());
        dassert(_endKeyInclusive == bounds->endKeyInclusive());
        checkAssumptionsAndInit();
    }

    void IndexCountCursor::checkAssumptionsAndInit() {
        verify(forward()); // only need to count forward
        verify(_prelock); // should be no case where we decide not to prelock/prefetch
        if ( ok() ) {
            // ok() at this point means the IndexCursor constructor found
            // a single matching row. We note that with a buffered row
            // count of 1.
            _bufferedRowCount = 1;
        }
    }

    int IndexCountCursor::count_cursor_getf(const DBT *key, const DBT *val, void *extra) {
        struct count_cursor_getf_extra *info = reinterpret_cast<struct count_cursor_getf_extra *>(extra);
        try {
            // Initializes a storage key that will ignore the appended primary key.
            // This is useful because we only want to compare the secondary key prefix.
            const storage::Key sKey(reinterpret_cast<char *>(key->data), false);
            const int c = sKey.woCompare(info->endSKeyPrefix);
            TOKULOG(5) << "count getf got " << sKey.key() << ", c = " << c << endl;
            if (c > 0 || (c == 0 && !info->endKeyInclusive)) {
                // out of bounds, count is finished.
                dassert(!info->exhausted);
                info->exhausted = true;
                return 0;
            }
            info->bufferedRowCount++;
            return TOKUDB_CURSOR_CONTINUE;
        } catch (const std::exception &ex) {
            info->saveException(ex);
        }
        return -1;
    }

    bool IndexCountCursor::countMoreRows() {
        // no rows buffered for count, that's why we're counting more here.
        verify(_bufferedRowCount == 0);
        // should not have already tried + failed at counting more rows.
        verify(!_exhausted);

        DBC *cursor = _cursor->dbc();
        struct count_cursor_getf_extra extra(_bufferedRowCount, _exhausted,
                                             _endSKeyPrefix, _ordering, _endKeyInclusive);
        const int r = cursor->c_getf_next(cursor, getf_flags(), count_cursor_getf, &extra);
        if (r != 0 && r != DB_NOTFOUND) {
            extra.throwException();
            storage::handle_ydb_error(r);
        }
        return _bufferedRowCount > 0;
    }

    bool IndexCountCursor::advance() {
        killCurrentOp.checkForInterrupt();
        if ( ok() ) {
            dassert(_bufferedRowCount > 0);
            _ok = --_bufferedRowCount > 0;
            if ( !ok() && !_exhausted ) {
                _ok = countMoreRows();
            }
            if ( ok() ) {
                ++_nscanned;
            }
        }
        return ok();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    IndexScanCountCursor::IndexScanCountCursor( CollectionData *cl, const IndexDetails &idx ) :
        IndexCountCursor( cl, idx,
                          ScanCursor::startKey(idx.keyPattern(), 1), 
                          ScanCursor::endKey(idx.keyPattern(), 1),
                          true ) {
        verify(forward());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Partitioned Cursors (over the _id index)
    PartitionedCursor::PartitionedCursor(
        const bool distributed,
        shared_ptr<SinglePartitionCursorGenerator> subCursorGenerator,
        shared_ptr<PartitionedCursorIDGenerator> subPartitionIDGenerator,
        const bool multiKey
        ) :
        _distributed(distributed),
        _subCursorGenerator(subCursorGenerator),
        _partitionIDGenerator(subPartitionIDGenerator),
        _multiKey(multiKey),
        _prevNScanned(0),
        _tailable(false)
    {
        initializeSubCursor();
    }

    void PartitionedCursor::getNextSubCursor() {
        _partitionIDGenerator->advanceIndex();
        shared_ptr<Cursor> oldCursor = _currentCursor;
        _currentCursor = _subCursorGenerator->makeSubCursor(_partitionIDGenerator->getCurrentPartitionIndex());
        if (oldCursor) {
            if (_matcher) {
                _currentCursor->setMatcher(_matcher);
            }
            _prevNScanned += oldCursor->nscanned();
            _currentCursor->setKeyFieldsOnly(_keyFieldsOnly);
        }
    }

    void PartitionedCursor::initializeSubCursor() {
        TOKULOG(3) << "Query: " << cc().querySettings().getQuery() << " sort: " << cc().querySettings().sortRequired() << endl;
        uint64_t currPartition = _partitionIDGenerator->getCurrentPartitionIndex();
        _currentCursor = _subCursorGenerator->makeSubCursor(currPartition);
        while (!_currentCursor->ok() && !_partitionIDGenerator->lastIndex()) {
            getNextSubCursor();
            // because we are called from a constructor,
            // we don't need to check to see if we are tailable
        }
    }

    bool PartitionedCursor::advance(){
        bool ret = _currentCursor->advance();
        while (!_currentCursor->ok() && !_partitionIDGenerator->lastIndex()) {
            // just making sure that advance() outside of this loop returned false
            // That is the only wany that _currentCursor->ok() should be false
            dassert(!ret);
            getNextSubCursor();
            // if we are iterating over the last partition and we are tailable,
            // we set tailable on the current cursor. addPartition and dropPartition
            // invalidate cursors, so we don't need to worry about
            // partitions being added or dropped in the lifetime of
            // a cursor
            if (_tailable && _partitionIDGenerator->lastIndex()) {
                _currentCursor->setTailable();
            }
            ret = _currentCursor->ok();
        }
        return ret;
    }

    void PartitionedCursor::setTailable() {
        _tailable = true;
        if (_partitionIDGenerator->lastIndex()) {
            _currentCursor->setTailable();
        }
    }

    SortedPartitionedCursor::SortedPartitionedCursor(
        const BSONObj idxPattern,
        const int direction,
        shared_ptr<SinglePartitionCursorGenerator> subCursorGenerator,
        shared_ptr<PartitionedCursorIDGenerator> subPartitionIDGenerator,
        const bool multiKey
        ) :
        _direction(direction),
        _ordering(Ordering::make(idxPattern)),
        _subCursorGenerator(subCursorGenerator),
        _partitionIDGenerator(subPartitionIDGenerator),
        _multiKey(multiKey),
        _comparator(_direction, _ordering)
    {
        // create each sub cursor in _cursors
        uint64_t curr = _partitionIDGenerator->getCurrentPartitionIndex();
        shared_ptr<Cursor> currentCursor = _subCursorGenerator->makeSubCursor(curr);
        _cursors.push_back(SPCSingleCursor(currentCursor, curr));
        while (!_partitionIDGenerator->lastIndex()) {
            _partitionIDGenerator->advanceIndex();
            curr = _partitionIDGenerator->getCurrentPartitionIndex();
            currentCursor = _subCursorGenerator->makeSubCursor(curr);
            _cursors.push_back(SPCSingleCursor(currentCursor, curr));
        }

        // now that we have a vector of cursors, make a heap out of it
        std::make_heap(_cursors.begin(), _cursors.end(), _comparator);
    }
    
    bool SortedPartitionedCursor::advance() {
        std::pop_heap(
            _cursors.begin(),
            _cursors.end(),
            _comparator
            );
        shared_ptr<Cursor> currentCursor = _cursors.back().first;
        massert(17340, "cursor should be ok", currentCursor->ok());
        currentCursor->advance();
        std::push_heap(_cursors.begin(), _cursors.end(), _comparator);
        return ok();
    }

    shared_ptr<Cursor> RangePartitionCursorGenerator::_makeSubCursor(uint64_t partitionIndex) {
        shared_ptr<CollectionData> currColl = _pc->getPartition(partitionIndex);
        // an optimization for a future day may be
        // if we know that the entire partition falls between startKey
        // and endKey, then we can use  a table scan cursor
        return Cursor::make(
            currColl.get(),
            currColl->idx(_idxNo),
            _startKey,
            _endKey,
            _endKeyInclusive,
            _direction,
            _numWanted,
            _countCursor
            );
    }

    shared_ptr<Cursor> BoundsPartitionCursorGenerator::_makeSubCursor(uint64_t partitionIndex) {
        shared_ptr<CollectionData> currColl = _pc->getPartition(partitionIndex);
        // I am not sure this case is currently possible
        // because the index is just a simple _id index
        // Look at coverage tools to see if this is dead
        // code
        return Cursor::make(
            currColl.get(),
            currColl->idx(_idxNo),
            _bounds,
            _singleIntervalLimit,
            _direction,
            _numWanted,
            _countCursor
            );
    }

    shared_ptr<Cursor> SinglePartitionCursorGenerator::makeSubCursor(uint64_t partitionIndex) {
        try {
            return _makeSubCursor(partitionIndex);
        } catch (storage::RetryableException::MvccDictionaryTooNew) {
            // this is how we make a dummy cursor, as it's only constructor is private
            return Cursor::make(NULL);
        }
    }

    shared_ptr<Cursor> ExhaustivePartitionCursorGenerator::_makeSubCursor(uint64_t partitionIndex) {
        shared_ptr<CollectionData> currColl = _pc->getPartition(partitionIndex);
        if (_cursorOverPartitionKey) {
            return Cursor::make(
                currColl.get(),
                _direction,
                _countCursor
                );
        }
        else {
            return Cursor::make(
                currColl.get(),
                currColl->idx(_idxNo),
                _direction,
                _countCursor
                );
        }
    }

    PartitionedCursorIDGeneratorImpl::PartitionedCursorIDGeneratorImpl(
        PartitionedCollection* pc,
        const int direction
        ) :
        _startPartition(direction > 0 ? 0 : pc->numPartitions() - 1),
        _endPartition(direction > 0 ? pc->numPartitions() - 1 : 0),
        _direction(direction)
    {
        _currPartition = _startPartition;
        sanityCheckPartitionEndpoints();
    }

    PartitionedCursorIDGeneratorImpl::PartitionedCursorIDGeneratorImpl(
        PartitionedCollection* pc,
        const BSONObj &startKey,
        const BSONObj &endKey,
        const int direction
        ) :
        _startPartition(pc->partitionWithPK(startKey)),
        _endPartition(pc->partitionWithPK(endKey)),
        _direction(direction)
    {
        _currPartition = _startPartition;
        sanityCheckPartitionEndpoints();
    }

    PartitionedCursorIDGeneratorImpl::PartitionedCursorIDGeneratorImpl(
        PartitionedCollection* pc,
        const shared_ptr<FieldRangeVector> &bounds,
        const int direction
        ) :
        _startPartition(pc->partitionWithPK(bounds->startKey())),
        _endPartition(pc->partitionWithPK(bounds->endKey())),
        _direction(direction)
    {
        _currPartition = _startPartition;
        sanityCheckPartitionEndpoints();
    }

    void PartitionedCursorIDGeneratorImpl::sanityCheckPartitionEndpoints() {
        // sanity check
        if (_direction > 0) {
            massert(17341, str::stream() << "bad _endPartition " << _endPartition << " and _startPartition " << _startPartition, _endPartition >= _startPartition);
        }
        else {
            massert(17342, str::stream() << "bad _endPartition " << _endPartition << " and _startPartition " << _startPartition, _startPartition >= _endPartition);
        }
    }

    uint64_t PartitionedCursorIDGeneratorImpl::getCurrentPartitionIndex() {
        return _currPartition;
    }

    void PartitionedCursorIDGeneratorImpl::advanceIndex() {
        massert(17343, "cannot advanceIndex, at end", !lastIndex());
        if (_direction > 0) {
            _currPartition++;
        }
        else {
            _currPartition--;
        }
    }
    
    bool PartitionedCursorIDGeneratorImpl::lastIndex() {
        return (_currPartition == _endPartition);
    }

    FilteredPartitionIDGeneratorImpl::FilteredPartitionIDGeneratorImpl(
        PartitionedCollection* pc,
        const char* ns,
        const ShardKeyPattern key,
        const int direction
        ):
        _partitionsToRead(pc->numPartitions(), false),
        _direction(direction)
    {
        // initialize the bitmap to be all false
        uint64_t numPartitions = pc->numPartitions();
        uint64_t minPartitionToRead = numPartitions;
        uint64_t maxPartitionToRead = 0;
        // This code was pattern-matched/taken from sharding code
        // Specifically, from ChunkManager::getShardsForQuery
        OrRangeGenerator org(ns, cc().querySettings().getQuery(), false);
        do {
            boost::scoped_ptr<FieldRangeSetPair> frsp (org.topFrsp());

            // special case if most-significant field isn't in query
            // only have this because ChunkManager::getShardsForQuery
            // has it as well
            FieldRange range = frsp->shardKeyRange(key.key().firstElementFieldName());
            if ( range.universal() ) {
                std::fill(_partitionsToRead.begin(), _partitionsToRead.end(), true);
                minPartitionToRead = 0;
                maxPartitionToRead = numPartitions-1;
                break;
            }
            
            if ( frsp->matchPossibleForSingleKeyFRS( key.key() ) ) {
                BoundList ranges = key.keyBounds( frsp->getSingleKeyFRS() );
                for ( BoundList::const_iterator it=ranges.begin(); it != ranges.end(); ++it ){
                    TOKULOG(3) << "Bounds for partitions: first: " << it->first << " second " << it->second << endl;
                    uint64_t first = pc->partitionWithRow(it->first);
                    uint64_t second = pc->partitionWithRow(it->second);
                    uint64_t min = first < second ? first : second;
                    uint64_t max = first < second ? second : first;
                    TOKULOG(3) << "Setting partitions " << min << " through " << max << " to be read" <<endl;
                    std::fill(_partitionsToRead.begin() + min, _partitionsToRead.begin() + max + 1, true);
                    if (min < minPartitionToRead) {
                        minPartitionToRead = min;
                    }
                    if (max > maxPartitionToRead) {
                        maxPartitionToRead = max;
                    }
                }
            }
            if (!org.orRangesExhausted()) {
                org.popOrClauseSingleKey();
            }
        } while (!org.orRangesExhausted());
        // at this point, we have set all of the appropriate
        // entries in _partitionsToRead to true
        // Now we need to set up _currPartition
        // and _endPartition so that partition id's can be generated
        if (_direction > 0) {
            _currPartition = minPartitionToRead;
            _endPartition = maxPartitionToRead;
        }
        else {
            _currPartition = maxPartitionToRead;
            _endPartition = minPartitionToRead;
        }
        massert(17344, "bad _endPartition", _partitionsToRead[_endPartition]);
        massert(17345, "bad _currPartition", _partitionsToRead[_currPartition]);
    }

    uint64_t FilteredPartitionIDGeneratorImpl::getCurrentPartitionIndex() {
        return _currPartition;
    }
    
    void FilteredPartitionIDGeneratorImpl::advanceIndex() {
        massert(17346, "cannot advanceIndex, at end", !lastIndex());
        if (_direction > 0) {
            _currPartition++;
            while (!_partitionsToRead[_currPartition]) {
                _currPartition++;
            }
        }
        else {
            _currPartition--;
            while (!_partitionsToRead[_currPartition]) {
                _currPartition--;
            }
        }
    }
    
    bool FilteredPartitionIDGeneratorImpl::lastIndex() {
        return (_currPartition == _endPartition);
    }
    
} // namespace mongo
