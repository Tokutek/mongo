/**
 *    Copyright (C) 2013 10gen Inc.
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

#include "mongo/db/exec/fetch.h"

#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/pdfile.h"
#include "mongo/util/fail_point_service.h"

namespace mongo {

    // Some fail points for testing.
    MONGO_FP_DECLARE(fetchInMemoryFail);
    MONGO_FP_DECLARE(fetchInMemorySucceed);

    FetchStage::FetchStage(WorkingSet* ws, PlanStage* child, Matcher* matcher)
        : _ws(ws), _child(child), _matcher(matcher), _idBeingPagedIn(WorkingSet::INVALID_ID) { }

    FetchStage::~FetchStage() { }

    bool FetchStage::isEOF() {
        if (WorkingSet::INVALID_ID != _idBeingPagedIn) {
            // We asked our parent for a page-in but he didn't get back to us.  We still need to
            // return the result that _idBeingPagedIn refers to.
            return false;
        }

        return _child->isEOF();
    }

    bool recordInMemory(const char* data) {
        if (MONGO_FAIL_POINT(fetchInMemoryFail)) {
            return false;
        }

        if (MONGO_FAIL_POINT(fetchInMemorySucceed)) {
            return true;
        }

        return Record::likelyInPhysicalMemory(data);
    }

    PlanStage::StageState FetchStage::work(WorkingSetID* out) {
        if (isEOF()) { return PlanStage::IS_EOF; }

        // If we asked our parent for a page-in last time work(...) was called, finish the fetch.
        if (WorkingSet::INVALID_ID != _idBeingPagedIn) {
            return fetchCompleted(out);
        }

        // If we're here, we're not waiting for a DiskLoc to be fetched.  Get another to-be-fetched
        // result from our child.
        WorkingSetID id;
        StageState status = _child->work(&id);

        if (PlanStage::ADVANCED == status) {
            WorkingSetMember* member = _ws->get(id);

            // If there's an obj there, there is no fetching to perform.
            if (member->hasObj()) {
                return returnIfMatches(member, id, out);
            }

            // We need a valid loc to fetch from and this is the only state that has one.
            verify(WorkingSetMember::LOC_AND_IDX == member->state);
            verify(member->hasLoc());

            Record* record = member->loc.rec();
            const char* data = record->dataNoThrowing();

            if (!recordInMemory(data)) {
                // member->loc points to a record that's NOT in memory.  Pass a fetch request up.
                verify(WorkingSet::INVALID_ID == _idBeingPagedIn);
                _idBeingPagedIn = id;
                *out = id;
                return PlanStage::NEED_FETCH;
            }
            else {
                // Don't need index data anymore as we have an obj.
                member->keyData.clear();
                member->obj = BSONObj(data);
                member->state = WorkingSetMember::LOC_AND_UNOWNED_OBJ;
                return returnIfMatches(member, id, out);
            }
        }
        else {
            // NEED_TIME/YIELD, ERROR, IS_EOF
            return status;
        }
    }

    void FetchStage::prepareToYield() { _child->prepareToYield(); }

    void FetchStage::recoverFromYield() { _child->recoverFromYield(); }

    void FetchStage::invalidate(const DiskLoc& dl) {
        _child->invalidate(dl);

        // If we're holding on to an object that we're waiting for the runner to page in...
        if (WorkingSet::INVALID_ID != _idBeingPagedIn) {
            WorkingSetMember* member = _ws->get(_idBeingPagedIn);
            verify(member->hasLoc());
            // The DiskLoc is about to perish so we force a fetch of the data.
            if (member->loc == dl) {
                // This is a fetch inside of a write lock (that somebody else holds) but the other
                // holder is likely operating on this object so this shouldn't have to hit disk.
                WorkingSetCommon::fetchAndInvalidateLoc(member);
            }
        }
    }

    PlanStage::StageState FetchStage::fetchCompleted(WorkingSetID* out) {
        WorkingSetMember* member = _ws->get(_idBeingPagedIn);

        // The DiskLoc we're waiting to page in was invalidated (forced fetch).  Test for
        // matching and maybe pass it up.
        if (member->state == WorkingSetMember::OWNED_OBJ) {
            WorkingSetID memberID = _idBeingPagedIn;
            _idBeingPagedIn = WorkingSet::INVALID_ID;
            return returnIfMatches(member, memberID, out);
        }

        // Assume that the caller has fetched appropriately.
        // TODO: Do we want to double-check the runner?  Not sure how reliable likelyInMemory is
        // on all platforms.
        verify(member->hasLoc());
        verify(!member->hasObj());

        // Make the (unowned) object.
        Record* record = member->loc.rec();
        const char* data = record->dataNoThrowing();
        member->obj = BSONObj(data);

        // Don't need index data anymore as we have an obj.
        member->keyData.clear();
        member->state = WorkingSetMember::LOC_AND_UNOWNED_OBJ;
        verify(!member->obj.isOwned());

        // Return the obj if it passes our filter.
        WorkingSetID memberID = _idBeingPagedIn;
        _idBeingPagedIn = WorkingSet::INVALID_ID;
        return returnIfMatches(member, memberID, out);
    }

    PlanStage::StageState FetchStage::returnIfMatches(WorkingSetMember* member,
                                                      WorkingSetID memberID,
                                                      WorkingSetID* out) {
        if (NULL == _matcher || _matcher->matches(member)) {
            *out = memberID;
            return PlanStage::ADVANCED;
        }
        else {
            _ws->free(memberID);
            return PlanStage::NEED_TIME;
        }
    }

}  // namespace mongo
