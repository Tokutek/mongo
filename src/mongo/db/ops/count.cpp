// count.cpp

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

#include "mongo/db/ops/count.h"
#include "mongo/db/client.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/collection.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/util/elapsed_tracker.h"

namespace mongo {

    /**
     * Specialized Cursor creation rules that the count operator provides to the query
     * processing system.  These rules limit the performance overhead when counting index keys
     * matching simple predicates.  See SERVER-1752.
     */
    class CountPlanPolicies : public QueryPlanSelectionPolicy {

        virtual string name() const { return "CountPlanPolicies"; }

        virtual bool requestMatcher() const {
            // Avoid using a Matcher when a Cursor will exactly match a query.
            return false;
        }

        virtual bool requestCountingCursor() const {
            // Request use of an IntervalBtreeCursor when the index bounds represent a single
            // btree interval.  This Cursor implementation is optimized for performing counts
            // between two endpoints.
            return true;
        }

    } _countPlanPolicies;

    long long runCount( const char *ns, const BSONObj &cmd, string &err, int &errCode ) {
        Collection *cl = getCollection( ns );
        if (cl == NULL) {
            err = "ns missing";
            return -1;
        }
        BSONObj query = cmd.getObjectField("query");

        long long count = 0;
        long long skip = cmd["skip"].numberLong();
        long long limit = cmd["limit"].numberLong();

        if ( limit < 0 ) {
            limit  = -limit;
        }

        Client::WithOpSettings wos(OpSettings().setQueryCursorMode(DEFAULT_LOCK_CURSOR).setBulkFetch(true));

        Lock::assertAtLeastReadLocked(ns);
        try {
            for (shared_ptr<Cursor> cursor = getOptimizedCursor( ns, query, BSONObj(), _countPlanPolicies );
                 cursor->ok() ; cursor->advance() ) {
                if ( cursor->currentMatches() && !cursor->getsetdup( cursor->currPK() ) ) {
                    if ( skip > 0 ) {
                        --skip;
                    }
                    else {
                        ++count;
                        if ( limit > 0 && count >= limit ) {
                            break;
                        }
                    }
                }
            }
            return count;
        }
        catch ( const DBException &e ) {
            err = e.toString();
            errCode = e.getCode();
            count = -2;
        }
        catch ( const std::exception &e ) {
            err = e.what();
            errCode = 0;
            count = -2;
        }
        if ( count == -2 ) {
            // Historically we have returned zero in many count assertion cases - see SERVER-2291.
            log() << "Count with ns: " << ns << " and query: " << query
                  << " failed with exception: " << err << " code: " << errCode
                  << endl;
        }
        return count;
    }

} // namespace mongo
