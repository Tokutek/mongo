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
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

/**
 * This file contains tests for mongo/db/query/query_planner.cpp
 */

#include "mongo/db/query/query_planner.h"
#include "mongo/db/query/query_solution.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/json.h"
#include "mongo/db/jsobj.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"

using namespace mongo;

namespace {

    // TODO: ALBERTO
    bool albertoWasHere = false;

    static const char* ns = "somebogusns";

    //
    // Equality
    //

    // Test the most basic index-using functionality.
    TEST(QueryPlannerTest, EqualityIndexScan) {
        // Get the canonical query.
        BSONObj queryObj = BSON("x" << 5);
        CanonicalQuery* cq;
        ASSERT(CanonicalQuery::canonicalize(ns, queryObj, &cq).isOK());

        // This is our index.  It's the simplest index we can use.
        vector<BSONObj> indices;
        indices.push_back(BSON("x" << 1));

        // If we can't get this right...
        vector<QuerySolution*> solns;
        QueryPlanner::plan(*cq, indices, &solns);

        if (albertoWasHere) {
            ASSERT_EQUALS(size_t(2), solns.size());

            IndexScanNode* ixnode;
            if (STAGE_IXSCAN == solns[0]->root->getType()) {
                ASSERT_EQUALS(STAGE_COLLSCAN, solns[1]->root->getType());
                ixnode = static_cast<IndexScanNode*>(solns[0]->root.get());
            }
            else {
                ASSERT_EQUALS(STAGE_COLLSCAN, solns[0]->root->getType());
                ASSERT_EQUALS(STAGE_IXSCAN, solns[1]->root->getType());
                ixnode = static_cast<IndexScanNode*>(solns[1]->root.get());
            }

            cout << ixnode->bounds.toString() << endl;
            // TODO: Check bounds.
        }
    }

    TEST(QueryPlannerTest, EqualityIndexScanWithTrailingFields) {
        BSONObj queryObj = BSON("x" << 5);

        CanonicalQuery* cq;
        ASSERT(CanonicalQuery::canonicalize(ns, queryObj, &cq).isOK());

        vector<BSONObj> indices;
        indices.push_back(BSON("x" << 1 << "y" << 1));

        vector<QuerySolution*> solns;
        QueryPlanner::plan(*cq, indices, &solns);

        if (albertoWasHere) {
            // Available index is prefixed by our equality, use it.
            ASSERT_EQUALS(size_t(2), solns.size());

            IndexScanNode* ixnode;
            if (STAGE_IXSCAN == solns[0]->root->getType()) {
                ASSERT_EQUALS(STAGE_COLLSCAN, solns[1]->root->getType());
                ixnode = static_cast<IndexScanNode*>(solns[0]->root.get());
            }
            else {
                ASSERT_EQUALS(STAGE_COLLSCAN, solns[0]->root->getType());
                ASSERT_EQUALS(STAGE_IXSCAN, solns[1]->root->getType());
                ixnode = static_cast<IndexScanNode*>(solns[1]->root.get());
            }

            cout << ixnode->bounds.toString() << endl;
            // TODO: Check bounds.
        }
    }

    // TODO: Check compound indices.
    // TODO: Check diff. index directions for ranges below.

    //
    // <
    //

    TEST(QueryPlannerTest, LessThan) {
        BSONObj queryObj = BSON("x" << BSON("$lt" << 5));

        CanonicalQuery* cq;
        ASSERT(CanonicalQuery::canonicalize(ns, queryObj, &cq).isOK());

        vector<BSONObj> indices;
        indices.push_back(BSON("x" << 1));

        vector<QuerySolution*> solns;
        QueryPlanner::plan(*cq, indices, &solns);

        if (albertoWasHere) {
            // Available index is prefixed by our equality, use it.
            ASSERT_EQUALS(size_t(2), solns.size());

            IndexScanNode* ixnode;
            if (STAGE_IXSCAN == solns[0]->root->getType()) {
                ASSERT_EQUALS(STAGE_COLLSCAN, solns[1]->root->getType());
                ixnode = static_cast<IndexScanNode*>(solns[0]->root.get());
            }
            else {
                ASSERT_EQUALS(STAGE_COLLSCAN, solns[0]->root->getType());
                ASSERT_EQUALS(STAGE_IXSCAN, solns[1]->root->getType());
                ixnode = static_cast<IndexScanNode*>(solns[1]->root.get());
            }

            cout << ixnode->bounds.toString() << endl;
            // TODO: Check bounds.
        }
    }

    //
    // <=
    //

    TEST(QueryPlannerTest, LessThanEqual) {
        BSONObj queryObj = BSON("x" << BSON("$lte" << 5));

        CanonicalQuery* cq;
        ASSERT(CanonicalQuery::canonicalize(ns, queryObj, &cq).isOK());

        vector<BSONObj> indices;
        indices.push_back(BSON("x" << 1));

        vector<QuerySolution*> solns;
        QueryPlanner::plan(*cq, indices, &solns);

        if (albertoWasHere) {
            // Available index is prefixed by our equality, use it.
            ASSERT_EQUALS(size_t(2), solns.size());

            IndexScanNode* ixnode;
            if (STAGE_IXSCAN == solns[0]->root->getType()) {
                ASSERT_EQUALS(STAGE_COLLSCAN, solns[1]->root->getType());
                ixnode = static_cast<IndexScanNode*>(solns[0]->root.get());
            }
            else {
                ASSERT_EQUALS(STAGE_COLLSCAN, solns[0]->root->getType());
                ASSERT_EQUALS(STAGE_IXSCAN, solns[1]->root->getType());
                ixnode = static_cast<IndexScanNode*>(solns[1]->root.get());
            }
            cout << ixnode->bounds.toString() << endl;
            // TODO: Check bounds.
        }
    }

    //
    // >
    //

    TEST(QueryPlannerTest, GreaterThan) {
        BSONObj queryObj = BSON("x" << BSON("$gt" << 5));

        CanonicalQuery* cq;
        ASSERT(CanonicalQuery::canonicalize(ns, queryObj, &cq).isOK());

        vector<BSONObj> indices;
        indices.push_back(BSON("x" << 1));

        vector<QuerySolution*> solns;
        QueryPlanner::plan(*cq, indices, &solns);

        if (albertoWasHere) {
            // Available index is prefixed by our equality, use it.
            ASSERT_EQUALS(size_t(2), solns.size());

            IndexScanNode* ixnode;
            if (STAGE_IXSCAN == solns[0]->root->getType()) {
                ASSERT_EQUALS(STAGE_COLLSCAN, solns[1]->root->getType());
                ixnode = static_cast<IndexScanNode*>(solns[0]->root.get());
            }
            else {
                ASSERT_EQUALS(STAGE_COLLSCAN, solns[0]->root->getType());
                ASSERT_EQUALS(STAGE_IXSCAN, solns[1]->root->getType());
                ixnode = static_cast<IndexScanNode*>(solns[1]->root.get());
            }

            cout << ixnode->bounds.toString() << endl;
            // TODO: Check bounds.
        }
    }

    //
    // >=
    //

    TEST(QueryPlannerTest, GreaterThanEqual) {
        BSONObj queryObj = BSON("x" << BSON("$gte" << 5));

        CanonicalQuery* cq;
        ASSERT(CanonicalQuery::canonicalize(ns, queryObj, &cq).isOK());

        vector<BSONObj> indices;
        indices.push_back(BSON("x" << 1));

        vector<QuerySolution*> solns;
        QueryPlanner::plan(*cq, indices, &solns);

        if (albertoWasHere) {
            // Available index is prefixed by our equality, use it.
            ASSERT_EQUALS(size_t(2), solns.size());

            IndexScanNode* ixnode;
            if (STAGE_IXSCAN == solns[0]->root->getType()) {
                ASSERT_EQUALS(STAGE_COLLSCAN, solns[1]->root->getType());
                ixnode = static_cast<IndexScanNode*>(solns[0]->root.get());
            }
            else {
                ASSERT_EQUALS(STAGE_COLLSCAN, solns[0]->root->getType());
                ASSERT_EQUALS(STAGE_IXSCAN, solns[1]->root->getType());
                ixnode = static_cast<IndexScanNode*>(solns[1]->root.get());
            }

            cout << ixnode->bounds.toString() << endl;
            // TODO: Check bounds.
        }
    }

}  // namespace
