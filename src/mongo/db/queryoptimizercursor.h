/**
 *    Copyright (C) 2011 10gen Inc.
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

#include "mongo/db/cursor.h"
#include "mongo/db/query_plan_selection_policy.h"

namespace mongo {
    
    class CandidatePlanCharacter;
    class ExplainQueryInfo;
    class FieldRangeSet;
    
    /**
     * Adds functionality to Cursor for running multiple plans, running out of order plans,
     * utilizing covered indexes, and generating explain output.
     */
    class QueryOptimizerCursor : public Cursor {
    public:
        
        /** Candidate plans for the query before it begins running. */
        virtual CandidatePlanCharacter initialCandidatePlans() const = 0;
        /** FieldRangeSet for the query before it begins running. */
        virtual const FieldRangeSet *initialFieldRangeSet() const = 0;

        /** @return true if the plan for the current iterate is out of order. */
        virtual bool currentPlanScanAndOrderRequired() const = 0;

        /** @return true when there may be multiple plans running and some are in order. */
        virtual bool runningInitialInOrderPlan() const = 0;
        /**
         * @return true when some query plans may have been excluded due to plan caching, for a
         * non-$or query.
         */
        virtual bool hasPossiblyExcludedPlans() const = 0;

        /**
         * @return true when both in order and out of order candidate plans were available, and
         * an out of order candidate plan completed iteration.
         */
        virtual bool completePlanOfHybridSetScanAndOrderRequired() const = 0;

        /** Clear recorded indexes for the current clause's query patterns. */
        virtual void clearIndexesForPatterns() = 0;
        /** Stop returning results from out of order plans and do not allow them to complete. */
        virtual void abortOutOfOrderPlans() = 0;

        /** Note match information for the current iterate, to generate explain output. */
        virtual void noteIterate( bool match, bool loadedDocument, bool chunkSkip ) = 0;
        /** @return explain output for the query run by this cursor. */
        virtual shared_ptr<ExplainQueryInfo> explainQueryInfo() const = 0;
    };

    class ParsedQuery;
    class QueryPlanSummary;

    /**
     * @return a cursor interface to the query optimizer.  The implementation may utilize a
     * single query plan or interleave results from multiple query plans before settling on a
     * single query plan.  Note that the schema of currKey() documents, indexKeyPattern(), the
     * matcher(), and the isMultiKey() nature of the cursor may change over the course of
     * iteration.
     *
     * @param query - Query used to select indexes and populate matchers; not copied if unowned
     * (see bsonobj.h).
     *
     * @param order - Required ordering spec for documents produced by this cursor, empty object
     * default indicates no order requirement.  If no index exists that satisfies the required
     * sort order, an empty shared_ptr is returned unless parsedQuery is also provided.  This is
     * not copied if unowned.
     *
     * @param planPolicy - A policy for selecting query plans - see queryoptimizercursor.h
     *
     * @param requestMatcher - Set to true to request that the returned Cursor provide a
     * matcher().  If false, the cursor's matcher() may return NULL if the Cursor can perform
     * accurate query matching internally using a non Matcher mechanism.  One case where a
     * Matcher might be requested even though not strictly necessary to select matching
     * documents is if metadata about matches may be requested using MatchDetails.  NOTE This is
     * a hint that the Cursor use a Matcher, but the hint may be ignored.  In some cases the
     * returned cursor may not provide a matcher even if 'requestMatcher' is true.
     *
     * @param parsedQuery - Additional query parameters, as from a client query request.
     *
     * @param requireOrder - If false, the resulting cursor may return results in an order
     * inconsistent with the @param order spec.  See queryoptimizercursor.h for information on
     * handling these results properly.
     *
     * @param singlePlanSummary - Query plan summary information that may be provided when a
     * cursor running a single plan is returned.
     *
     * This is a work in progress.  Partial list of features not yet implemented through this
     * interface:
     * 
     * - covered indexes
     * - in memory sorting
     */
    shared_ptr<Cursor> getOptimizedCursor( const StringData& ns,
                                           const BSONObj& query,
                                           const BSONObj& order = BSONObj(),
                                           const QueryPlanSelectionPolicy& planPolicy =
                                               QueryPlanSelectionPolicy::any(),
                                           bool requestMatcher = true,
                                           const shared_ptr<const ParsedQuery>& parsedQuery =
                                               shared_ptr<const ParsedQuery>(),
                                           bool requireOrder = true,
                                           QueryPlanSummary* singlePlanSummary = NULL );

    /**
     * @return a single cursor that may work well for the given query.  A $or style query will
     * produce a single cursor, not a MultiCursor.
     * It is possible no cursor is returned if the sort is not supported by an index.  Clients are responsible
     * for checking this if they are not sure an index for a sort exists, and defaulting to a non-sort if
     * no suitable indices exist.
     */
    shared_ptr<Cursor> getBestGuessCursor( const StringData& ns,
                                           const BSONObj &query,
                                           const BSONObj &sort );
    
} // namespace mongo
