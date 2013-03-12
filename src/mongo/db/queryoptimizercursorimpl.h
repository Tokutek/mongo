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

#include "mongo/db/queryutil.h"
#include "mongo/db/queryoptimizercursor.h"
#include "mongo/db/querypattern.h"

namespace mongo {

    class MultiCursor;
    class MultiPlanScanner;
    class QueryPlanRunner;
    class QueryPlanSelectionPolicy;
    struct QueryPlanSummary;
    
    /** Dup tracking class, optimizing one common case with small set and few initial reads. */
    class SmallDupSet {
    public:
        SmallDupSet() : _accesses() {
            _vec.reserve( 250 );
        }
        /** @return true if @param 'pk' already added to the set, false if adding to the set in this call. */
        bool getsetdup( const BSONObj &pk ) {
            access();
            return vec() ? getsetdupVec( pk ) : getsetdupSet( pk );
        }
        /** @return true when @param pk in the set. */
        bool getdup( const BSONObj &pk ) {
            access();
            return vec() ? getdupVec( pk ) : getdupSet( pk );
        }            
    private:
        void access() {
            ++_accesses;
            mayUpgrade();
        }
        void mayUpgrade() {
            if ( vec() && _accesses > 500 ) {
                _set.insert( _vec.begin(), _vec.end() );
            }
        }
        bool vec() const {
            return _set.size() == 0;
        }
        bool getsetdupVec( const BSONObj &pk ) {
            if ( getdupVec( pk ) ) {
                return true;
            }
            _vec.push_back( pk.getOwned() );
            return false;
        }
        bool getdupVec( const BSONObj &pk ) const {
            for( vector<BSONObj>::const_iterator i = _vec.begin(); i != _vec.end(); ++i ) {
                if ( *i == pk ) {
                    return true;
                }
            }
            return false;
        }
        bool getsetdupSet( const BSONObj &pk ) {
            pair<set<BSONObj>::iterator, bool> p = _set.insert(pk.getOwned());
            return !p.second;
        }
        bool getdupSet( const BSONObj &pk ) {
            return _set.count( pk ) > 0;
        }
        vector<BSONObj> _vec;
        set<BSONObj> _set;
        long long _accesses;
    };

    /**
     * This cursor runs a MultiPlanScanner iteratively and returns results from
     * the scanner's cursors as they become available.  Once the scanner chooses
     * a single plan, this cursor becomes a simple wrapper around that single
     * plan's cursor (called the 'takeover' cursor).
     */
    class QueryOptimizerCursorImpl : public QueryOptimizerCursor {
    public:
        static QueryOptimizerCursorImpl* make( auto_ptr<MultiPlanScanner>& mps,
                                               const QueryPlanSelectionPolicy& planPolicy,
                                               bool requireOrder,
                                               bool explain );
        
        virtual bool ok();
        
        virtual BSONObj current();
        
        virtual BSONObj currPK() const;

        BSONObj _currPK() const;
        
        virtual bool advance();
        
        virtual BSONObj currKey() const;
        
        virtual BSONObj indexKeyPattern() const;
        
        virtual bool supportGetMore() { return true; }

        virtual string toString() const { return "QueryOptimizerCursor"; }
        
        virtual bool getsetdup(const BSONObj &pk);
        
        /** Matcher needs to know if the the cursor being forwarded to is multikey. */
        virtual bool isMultiKey() const;
        
        // TODO fix
        virtual bool modifiedKeys() const { return true; }

        virtual bool capped() const;

        virtual long long nscanned() const;

        virtual CoveredIndexMatcher *matcher() const;

        virtual bool currentMatches( MatchDetails* details = 0 );
        
        virtual CandidatePlanCharacter initialCandidatePlans() const {
            return _initialCandidatePlans;
        }
        
        virtual const FieldRangeSet* initialFieldRangeSet() const;
        
        virtual bool currentPlanScanAndOrderRequired() const;
        
        virtual const Projection::KeyOnly* keyFieldsOnly() const;
        
        virtual bool runningInitialInOrderPlan() const;

        virtual bool hasPossiblyExcludedPlans() const;

        virtual bool completePlanOfHybridSetScanAndOrderRequired() const {
            return _completePlanOfHybridSetScanAndOrderRequired;
        }
        
        virtual void clearIndexesForPatterns();
        
        virtual void abortOutOfOrderPlans();

        virtual void noteIterate( bool match, bool loadedDocument, bool chunkSkip );
        
        virtual shared_ptr<ExplainQueryInfo> explainQueryInfo() const {
            return _explainQueryInfo;
        }
        
    private:
        
        QueryOptimizerCursorImpl( auto_ptr<MultiPlanScanner>& mps,
                                  const QueryPlanSelectionPolicy& planPolicy,
                                  bool requireOrder );
        
        void init( bool explain );

        /**
         * Advances the QueryPlanSet::Runner.
         * @param force - advance even if the current query op is not valid.  The 'force' param should only be specified
         * when there are plans left in the runner.
         */
        bool _advance( bool force );

        /** Forward an exception when the runner errs out. */
        void rethrowOnError( const shared_ptr< QueryPlanRunner >& runner );
        
        void assertOk() const {
            massert( 14809, "Invalid access for cursor that is not ok()", !_currPK().isEmpty() );
        }

        /** Insert and check for dups before takeover occurs */
        bool getsetdupInternal(const BSONObj& pk);

        /** Just check for dups - after takeover occurs */
        bool getdupInternal(const BSONObj& pk);
        
        bool _requireOrder;
        auto_ptr<MultiPlanScanner> _mps;
        CandidatePlanCharacter _initialCandidatePlans;
        shared_ptr<QueryPlanRunner> _originalRunner;
        QueryPlanRunner* _currRunner;
        bool _completePlanOfHybridSetScanAndOrderRequired;
        shared_ptr<MultiCursor> _takeover;
        long long _nscanned;
        // Using a SmallDupSet seems a bit hokey, but I've measured a 5% performance improvement
        // with ~100 document non multi key scans.
        SmallDupSet _dups;
        shared_ptr<ExplainQueryInfo> _explainQueryInfo;
    };
    
    /**
     * Helper class for generating a simple Cursor or QueryOptimizerCursor from a set of query
     * parameters.  This class was refactored from a single function call and is not expected to
     * outlive its constructor arguments.
     */
    class CursorGenerator {
    public:
        CursorGenerator( const StringData& ns,
                        const BSONObj &query,
                        const BSONObj &order,
                        const QueryPlanSelectionPolicy &planPolicy,
                        bool requestMatcher,
                        const shared_ptr<const ParsedQuery> &parsedQuery,
                        bool requireOrder,
                        QueryPlanSummary *singlePlanSummary );
        
        shared_ptr<Cursor> generate();
        
    private:
        bool snapshot() const { return _parsedQuery && _parsedQuery->isSnapshot(); }
        bool explain() const { return _parsedQuery && _parsedQuery->isExplain(); }
        BSONObj min() const { return _parsedQuery ? _parsedQuery->getMin() : BSONObj(); }
        BSONObj max() const { return _parsedQuery ? _parsedQuery->getMax() : BSONObj(); }
        bool hasFields() const { return _parsedQuery && _parsedQuery->getFieldPtr(); }
        
        bool isOrderRequired() const { return _requireOrder; }
        bool mayShortcutQueryOptimizer() const {
            return min().isEmpty() && max().isEmpty() && !hasFields() && _argumentsHint.isEmpty();
        }
        BSONObj hint() const;
        
        void setArgumentsHint();
        shared_ptr<Cursor> shortcutCursor() const;
        void setMultiPlanScanner();
        shared_ptr<Cursor> singlePlanCursor();
        
        const StringData _ns;
        BSONObj _query;
        BSONObj _order;
        const QueryPlanSelectionPolicy &_planPolicy;
        bool _requestMatcher;
        shared_ptr<const ParsedQuery> _parsedQuery;
        bool _requireOrder;
        QueryPlanSummary *_singlePlanSummary;
        
        BSONObj _argumentsHint;
        auto_ptr<MultiPlanScanner> _mps;
    };
    
} // namespace mongo
