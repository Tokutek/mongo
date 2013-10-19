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

#include "mongo/db/exec/plan_stage.h"
#include "mongo/db/exec/working_set.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/pdfile.h"

namespace mongo {

    /**
     * A placeholder for a full-featured plan runner.  Calls work() on a plan until a result is
     * produced.  Stops when the plan is EOF or if the plan errors.
     *
     * TODO: Yielding policy
     * TODO: Graceful error handling
     * TODO: Stats, diagnostics, instrumentation, etc.
     */
    class SimplePlanRunner {
    public:
        SimplePlanRunner() { }

        WorkingSet* getWorkingSet() { return &_workingSet; }

        /**
         * Takes ownership of root.
         */
        void setRoot(PlanStage* root) {
            verify(root);
            _root.reset(root);
        }

        bool getNext(BSONObj* objOut) {
            for (;;) {
                WorkingSetID id;
                PlanStage::StageState code = _root->work(&id);

                if (PlanStage::ADVANCED == code) {
                    WorkingSetMember* member = _workingSet.get(id);
                    uassert(16912, "Couldn't fetch obj from query plan",
                            WorkingSetCommon::fetch(member));
                    *objOut = member->obj;
                    _workingSet.free(id);
                    return true;
                }
                else if (code == PlanStage::NEED_TIME) {
                    // TODO: Occasionally yield.  For now, we run until we get another result.
                }
                else if (PlanStage::NEED_FETCH == code) {
                    // id has a loc and refers to an obj we need to fetch.
                    WorkingSetMember* member = _workingSet.get(id);

                    // This must be true for somebody to request a fetch and can only change when an
                    // invalidation happens, which is when we give up a lock.  Don't give up the
                    // lock between receiving the NEED_FETCH and actually fetching(?).
                    verify(member->hasLoc());

                    // Actually bring record into memory.
                    Record* record = member->loc.rec();
                    record->touch();

                    // Record should be in memory now.  Log if it's not.
                    if (!Record::likelyInPhysicalMemory(record->dataNoThrowing())) {
                        OCCASIONALLY {
                            warning() << "Record wasn't in memory immediately after fetch: "
                                      << member->loc.toString() << endl;
                        }
                    }

                    // Note that we're not freeing id.  Fetch semantics say that we shouldn't.
                }
                else {
<<<<<<< HEAD
                    // IS_EOF, ERROR, NEED_YIELD.  We just stop here.
=======
                    // IS_EOF, FAILURE.  We just stop here.
>>>>>>> b8f0ec5... SERVER-10026 fetch limit skip or
                    return false;
                }
            }
        }

    private:
        WorkingSet _workingSet;
        scoped_ptr<PlanStage> _root;
    };

}  // namespace mongo
