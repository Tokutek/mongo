/**
 *    Copyright (C) 2011 10gen Inc.
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

#include "mongo/db/query_plan_selection_policy.h"

#include "mongo/db/query_optimizer_internal.h"
#include "mongo/db/namespace_details.h"

namespace mongo {

    QueryPlanSelectionPolicy::Any QueryPlanSelectionPolicy::__any;
    const QueryPlanSelectionPolicy& QueryPlanSelectionPolicy::any() { return __any; }
    
    bool QueryPlanSelectionPolicy::IndexOnly::permitPlan( const QueryPlan& plan ) const {
        return !plan.willScanTable();
    }
    QueryPlanSelectionPolicy::IndexOnly QueryPlanSelectionPolicy::__indexOnly;
    const QueryPlanSelectionPolicy& QueryPlanSelectionPolicy::indexOnly() { return __indexOnly; }
    
} // namespace mongo
