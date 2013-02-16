// @file queryoptimizercursorimpl.cpp - A cursor interleaving multiple candidate cursors.

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


#include "pch.h"
#include "queryoptimizercursorimpl.h"
#include "clientcursor.h"
#include "indexcursor.h"
#include "explain.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/queryoptimizer.h"

namespace mongo {
    
    extern bool useHints;
    
    static const int OutOfOrderDocumentsAssertionCode = 14810;
        
    QueryPlanSelectionPolicy::Any QueryPlanSelectionPolicy::__any;
    const QueryPlanSelectionPolicy &QueryPlanSelectionPolicy::any() { return __any; }
    
    bool QueryPlanSelectionPolicy::IndexOnly::permitPlan( const QueryPlan &plan ) const {
        return !plan.willScanTable();
    }
    QueryPlanSelectionPolicy::IndexOnly QueryPlanSelectionPolicy::__indexOnly;
    const QueryPlanSelectionPolicy &QueryPlanSelectionPolicy::indexOnly() { return __indexOnly; }
    
    bool QueryPlanSelectionPolicy::IdElseNatural::permitPlan( const QueryPlan &plan ) const {
        return !plan.indexed() || plan.index()->isIdIndex();
    }
    BSONObj QueryPlanSelectionPolicy::IdElseNatural::planHint( const char *ns ) const {
        NamespaceDetails *nsd = nsdetails( ns );
        if ( !nsd || !nsd->haveIdIndex() ) {
            return BSON( "$hint" << BSON( "$natural" << 1 ) );
        }
        return BSON( "$hint" << nsd->idx( nsd->findIdIndex() ).indexName() );
    }
    QueryPlanSelectionPolicy::IdElseNatural QueryPlanSelectionPolicy::__idElseNatural;
    const QueryPlanSelectionPolicy &QueryPlanSelectionPolicy::idElseNatural() {
        return __idElseNatural;
    }

    /**
     * A QueryOp implementation utilized by the QueryOptimizerCursor
     */
    class QueryOptimizerCursorOp : public QueryOp {
    public:
        /**
         * @param aggregateNscanned - shared long long counting total nscanned for
         * query ops for all cursors.
         */
        QueryOptimizerCursorOp( long long &aggregateNscanned, const QueryPlanSelectionPolicy &selectionPolicy,
                               const bool &requireOrder, bool alwaysCountMatches, int cumulativeCount = 0 ) :
        _matchCounter( aggregateNscanned, cumulativeCount ),
        _countingMatches(),
        _mustAdvance(),
        _capped(),
        _selectionPolicy( selectionPolicy ),
        _requireOrder( requireOrder ),
        _alwaysCountMatches( alwaysCountMatches ) {
        }
        
        virtual void init() {
            checkCursorOrdering();
            if ( !_selectionPolicy.permitPlan( queryPlan() ) ) {
                throw MsgAssertionException( 9011,
                                            str::stream()
                                            << "Plan not permitted by query plan selection policy '"
                                            << _selectionPolicy.name()
                                            << "'" );
            }
            
            _c = queryPlan().newCursor();
            // The basic and btree cursors used by this implementation do not supply their own
            // matchers, and a matcher from a query plan will be used instead.
            verify( !_c->matcher() );

            // The query plan must have a matcher.  The matcher's constructor performs some aspects
            // of query validation that should occur as part of this class's init() if not handled
            // already.
            fassert( 16249, queryPlan().matcher() );

            _capped = _c->capped();

            // TODO This violates the current Cursor interface abstraction, but for now it's simpler to keep our own set of
            // dups rather than avoid poisoning the cursor's dup set with unreturned documents.  Deduping documents
            // matched in this QueryOptimizerCursorOp will run against the takeover cursor.
            _matchCounter.setCheckDups( countMatches() && _c->isMultiKey() );
            // TODO ok if cursor becomes multikey later?

            _matchCounter.updateNscanned( _c->nscanned() );
        }
        
        virtual long long nscanned() const {
            return _c ? _c->nscanned() : _matchCounter.nscanned();
        }
        
        virtual void next() {
            checkCursorOrdering();

            mayAdvance();
            
            if ( countMatches() && _matchCounter.enoughCumulativeMatchesToChooseAPlan() ) {
                setStop();
                if ( _explainPlanInfo ) _explainPlanInfo->notePicked();
                return;
            }
            if ( !_c || !_c->ok() ) {
                if ( _explainPlanInfo && _c ) _explainPlanInfo->noteDone( *_c );
                setComplete();
                return;
            }
            
            _mustAdvance = true;
        }
        virtual QueryOp *createChild() const {
            return new QueryOptimizerCursorOp( _matchCounter.aggregateNscanned(), _selectionPolicy, _requireOrder, _alwaysCountMatches, _matchCounter.cumulativeCount() );
        }
        BSONObj currKey() const { return _c ? _c->currKey() : BSONObj(); }
        BSONObj currPK() const { return _c ? _c->currPK() : BSONObj(); }
        bool currentMatches( MatchDetails *details ) {
            if ( !_c || !_c->ok() ) {
                _matchCounter.setMatch( false );
                return false;
            }
            
            MatchDetails myDetails;
            if ( !details && _explainPlanInfo ) {
                details = &myDetails;
            }

            bool match = queryPlan().matcher()->matchesCurrent( _c.get(), details );
            // Cache the match, so we can count it in mayAdvance().
            bool newMatch = _matchCounter.setMatch( match );

            if ( _explainPlanInfo ) {
                bool countableMatch = newMatch && _matchCounter.wouldCountMatch( _c->currPK() );
                _explainPlanInfo->noteIterate( countableMatch,
                                              countableMatch || details->hasLoadedRecord(),
                                              *_c );
            }

            return match;
        }
        virtual bool mayRecordPlan() const {
            return complete() && ( !stopRequested() || _matchCounter.enoughMatchesToRecordPlan() );
        }
        shared_ptr<Cursor> cursor() const { return _c; }
        virtual shared_ptr<ExplainPlanInfo> generateExplainInfo() {
            if ( !_c ) {
                return QueryOp::generateExplainInfo();
            }
            _explainPlanInfo.reset( new ExplainPlanInfo() );
            _explainPlanInfo->notePlan( *_c, queryPlan().scanAndOrderRequired(),
                                        queryPlan().keyFieldsOnly() );
            return _explainPlanInfo;
        }
        shared_ptr<ExplainPlanInfo> explainInfo() const { return _explainPlanInfo; }
        
        virtual const Projection::KeyOnly *keyFieldsOnly() const {
            return queryPlan().keyFieldsOnly().get();
        }
        
    private:
        void mayAdvance() {
            if ( !_c ) {
                return;
            }
            if ( countingMatches() ) {
                // Check match if not yet known.
                if ( !_matchCounter.knowMatch() ) {
                    currentMatches( 0 );
                }
                _matchCounter.countMatch( currPK() );
            }
            if ( _mustAdvance ) {
                _c->advance();
                handleCursorAdvanced();
            }
            _matchCounter.updateNscanned( _c->nscanned() );
        }
        bool countingMatches() {
            if ( _countingMatches ) {
                return true;
            }
            if ( countMatches() ) {
                // Only count matches after the first call to next(), which occurs before the first
                // result is returned.
                _countingMatches = true;
            }
            return false;
        }
        bool countMatches() const {
            return _alwaysCountMatches || !queryPlan().scanAndOrderRequired();
        }

        void handleCursorAdvanced() {
            _mustAdvance = false;
            _matchCounter.resetMatch();
        }
        void checkCursorOrdering() {
            if ( _requireOrder && queryPlan().scanAndOrderRequired() ) {
                throw MsgAssertionException( OutOfOrderDocumentsAssertionCode, "order spec cannot be satisfied with index" );
            }
        }

        CachedMatchCounter _matchCounter;
        bool _countingMatches;
        bool _mustAdvance;
        bool _capped;
        shared_ptr<Cursor> _c;
        ClientCursor::Holder _cc;
        const QueryPlanSelectionPolicy &_selectionPolicy;
        const bool &_requireOrder; // TODO don't use a ref for this, but signal change explicitly
        shared_ptr<ExplainPlanInfo> _explainPlanInfo;
        bool _alwaysCountMatches;
    };
    
    /**
     * This cursor runs a MultiPlanScanner iteratively and returns results from
     * the scanner's cursors as they become available.  Once the scanner chooses
     * a single plan, this cursor becomes a simple wrapper around that single
     * plan's cursor (called the 'takeover' cursor).
     */
    class QueryOptimizerCursorImpl : public QueryOptimizerCursor {
    public:
        static QueryOptimizerCursorImpl *make( auto_ptr<MultiPlanScanner> &mps,
                                              const QueryPlanSelectionPolicy &planPolicy,
                                              bool requireOrder,
                                              bool explain ) {
            auto_ptr<QueryOptimizerCursorImpl> ret( new QueryOptimizerCursorImpl( mps, planPolicy,
                                                                                 requireOrder ) );
            ret->init( explain );
            return ret.release();
        }
        
        virtual bool ok() { return _takeover ? _takeover->ok() : !_currPK().isEmpty(); }
        
        virtual BSONObj current() {
            if ( _takeover ) {
                return _takeover->current();
            }
            assertOk();
            BSONObj currObj;
            bool found = nsdetails(_mps->ns())->findById(currPK(), currObj, false);
            verify(found);
            return currObj;
        }
        
        virtual BSONObj currPK() const { return _takeover ? _takeover->currPK() : _currPK(); }
        
        BSONObj _currPK() const {
            dassert( !_takeover );
            return _currOp ? _currOp->currPK() : BSONObj();
        }
        
        virtual bool advance() {
            return _advance( false );
        }
        
        virtual BSONObj currKey() const {
            if ( _takeover ) {
             	return _takeover->currKey();   
            }
            assertOk();
            return _currOp->currKey();
        }
        
        virtual BSONObj indexKeyPattern() {
            if ( _takeover ) {
                return _takeover->indexKeyPattern();
            }
            assertOk();
            return _currOp->cursor()->indexKeyPattern();
        }
        
        virtual bool supportGetMore() { return true; }

        virtual string toString() const { return "QueryOptimizerCursor"; }
        
        virtual bool getsetdup(const BSONObj &pk) {
            if ( _takeover ) {
                if ( _dups.getdup( pk ) ) {
                    return true;   
                }
             	return _takeover->getsetdup( pk );   
            }
            assertOk();
            return _dups.getsetdup( pk );
        }
        
        /** Matcher needs to know if the the cursor being forwarded to is multikey. */
        virtual bool isMultiKey() const {
            if ( _takeover ) {
                return _takeover->isMultiKey();
            }
            assertOk();
            return _currOp->cursor()->isMultiKey();
        }
        
        // TODO fix
        virtual bool modifiedKeys() const { return true; }

        /** Initial capped wrapping cases (before takeover) are handled internally by a component ClientCursor. */
        virtual bool capped() const { return _takeover ? _takeover->capped() : false; }

        virtual long long nscanned() const { return _takeover ? _takeover->nscanned() : _nscanned; }

        virtual shared_ptr<CoveredIndexMatcher> matcherPtr() const {
            if ( _takeover ) {
                return _takeover->matcherPtr();
            }
            assertOk();
            return _currOp->queryPlan().matcher();
        }

        virtual CoveredIndexMatcher *matcher() const {
            if ( _takeover ) {
                return _takeover->matcher();
            }
            assertOk();
            return _currOp->queryPlan().matcher().get();
        }

        virtual bool currentMatches( MatchDetails *details = 0 ) {
            if ( _takeover ) {
                return _takeover->currentMatches( details );
            }
            assertOk();
            return _currOp->currentMatches( details );
        }
        
        virtual CandidatePlanCharacter initialCandidatePlans() const {
            return _initialCandidatePlans;
        }
        
        virtual const FieldRangeSet *initialFieldRangeSet() const {
            if ( _takeover ) {
                return 0;
            }
            assertOk();
            return &_currOp->queryPlan().multikeyFrs();
        }
        
        virtual bool currentPlanScanAndOrderRequired() const {
            if ( _takeover ) {
                return _takeover->queryPlan().scanAndOrderRequired();
            }
            assertOk();
            return _currOp->queryPlan().scanAndOrderRequired();
        }
        
        virtual const Projection::KeyOnly *keyFieldsOnly() const {
            if ( _takeover ) {
                return _takeover->keyFieldsOnly();
            }
            assertOk();
            return _currOp->keyFieldsOnly();
        }
        
        virtual bool runningInitialInOrderPlan() const {
            if ( _takeover ) {
                return false;
            }
            assertOk();
            return _mps->haveInOrderPlan();
        }

        virtual bool hasPossiblyExcludedPlans() const {
            if ( _takeover ) {
                return false;
            }
            assertOk();
            return _mps->hasPossiblyExcludedPlans();
        }

        virtual bool completePlanOfHybridSetScanAndOrderRequired() const {
            return _completePlanOfHybridSetScanAndOrderRequired;
        }
        
        virtual void clearIndexesForPatterns() {
            if ( !_takeover ) {
                _mps->clearIndexesForPatterns();
            }
        }
        
        virtual void abortOutOfOrderPlans() {
            _requireOrder = true;
        }

        virtual void noteIterate( bool match, bool loadedDocument, bool chunkSkip ) {
            if ( _explainQueryInfo ) {
                _explainQueryInfo->noteIterate( match, loadedDocument, chunkSkip );
            }
            if ( _takeover ) {
                _takeover->noteIterate( match, loadedDocument );
            }
        }
        
        virtual shared_ptr<ExplainQueryInfo> explainQueryInfo() const {
            return _explainQueryInfo;
        }
        
    private:
        
        QueryOptimizerCursorImpl( auto_ptr<MultiPlanScanner> &mps,
                                 const QueryPlanSelectionPolicy &planPolicy,
                                 bool requireOrder ) :
        _requireOrder( requireOrder ),
        _mps( mps ),
        _initialCandidatePlans( _mps->possibleInOrderPlan(), _mps->possibleOutOfOrderPlan() ),
        _originalOp( new QueryOptimizerCursorOp( _nscanned, planPolicy, _requireOrder,
                                                !_initialCandidatePlans.hybridPlanSet() ) ),
        _currOp(),
        _completePlanOfHybridSetScanAndOrderRequired(),
        _nscanned() {
        }
        
        void init( bool explain ) {
            _mps->initialOp( _originalOp );
            if ( explain ) {
                _explainQueryInfo = _mps->generateExplainInfo();
            }
            shared_ptr<QueryOp> op = _mps->nextOp();
            rethrowOnError( op );
            if ( !op->complete() ) {
                _currOp = dynamic_cast<QueryOptimizerCursorOp*>( op.get() );
            }
        }

        /**
         * Advances the QueryPlanSet::Runner.
         * @param force - advance even if the current query op is not valid.  The 'force' param should only be specified
         * when there are plans left in the runner.
         */
        bool _advance( bool force ) {
            if ( _takeover ) {
                return _takeover->advance();
            }

            if ( !force && !ok() ) {
                return false;
            }

            _currOp = 0;
            shared_ptr<QueryOp> op = _mps->nextOp();
            rethrowOnError( op );

            // Avoiding dynamic_cast here for performance.  Soon we won't need to
            // do a cast at all.
            QueryOptimizerCursorOp *qocop = (QueryOptimizerCursorOp*)( op.get() );

            if ( !op->complete() ) {
                // The 'qocop' will be valid until we call _mps->nextOp() again.  We return 'current' values from this op.
                _currOp = qocop;
            }
            else if ( op->stopRequested() ) {
                if ( qocop->cursor() ) {
                    _takeover.reset( new MultiCursor( _mps,
                                                     qocop->cursor(),
                                                     op->queryPlan().matcher(),
                                                     qocop->explainInfo(),
                                                     *op,
                                                     _nscanned - qocop->cursor()->nscanned() ) );
                }
            }
            else {
                if ( _initialCandidatePlans.hybridPlanSet() ) {
                    _completePlanOfHybridSetScanAndOrderRequired =
                            op->queryPlan().scanAndOrderRequired();
                }
            }

            return ok();
        }
        /** Forward an exception when the runner errs out. */
        void rethrowOnError( const shared_ptr< QueryOp > &op ) {
            if ( op->error() ) {
                throw MsgAssertionException( op->exception() );   
            }
        }
        
        void assertOk() const {
            // TODO: We should be calling ok() here, but for some reason that's not const.
            // So instead we copy the implementation.
            massert( 14809, "Invalid access for cursor that is not ok()", !currPK().isEmpty() );
        }

        bool _requireOrder;
        auto_ptr<MultiPlanScanner> _mps;
        CandidatePlanCharacter _initialCandidatePlans;
        shared_ptr<QueryOptimizerCursorOp> _originalOp;
        QueryOptimizerCursorOp *_currOp;
        bool _completePlanOfHybridSetScanAndOrderRequired;
        shared_ptr<MultiCursor> _takeover;
        long long _nscanned;
        // Using a SmallDupSet seems a bit hokey, but I've measured a 5% performance improvement
        // with ~100 document non multi key scans.
        SmallDupSet _dups;
        shared_ptr<ExplainQueryInfo> _explainQueryInfo;
    };
    
    shared_ptr<Cursor> newQueryOptimizerCursor( auto_ptr<MultiPlanScanner> mps,
                                               const QueryPlanSelectionPolicy &planPolicy,
                                               bool requireOrder, bool explain ) {
        try {
            shared_ptr<QueryOptimizerCursorImpl> ret
                    ( QueryOptimizerCursorImpl::make( mps, planPolicy, requireOrder, explain ) );
            return ret;
        } catch( const AssertionException &e ) {
            if ( e.getCode() == OutOfOrderDocumentsAssertionCode ) {
                // If no indexes follow the requested sort order, return an
                // empty pointer.  This is legacy behavior based on bestGuessCursor().
                return shared_ptr<Cursor>();
            }
            throw;
        }
        return shared_ptr<Cursor>();
    }
    
    shared_ptr<Cursor>
    NamespaceDetailsTransient::getCursor( const char *ns,
                                         const BSONObj &query,
                                         const BSONObj &order,
                                         const QueryPlanSelectionPolicy &planPolicy,
                                         bool *simpleEqualityMatch,
                                         const shared_ptr<const ParsedQuery> &parsedQuery,
                                         bool requireOrder,
                                         QueryPlanSummary *singlePlanSummary ) {

        CursorGenerator generator( ns, query, order, planPolicy, simpleEqualityMatch, parsedQuery,
                                   requireOrder, singlePlanSummary );
        return generator.generate();
    }
    
    CursorGenerator::CursorGenerator( const char *ns,
                                     const BSONObj &query,
                                     const BSONObj &order,
                                     const QueryPlanSelectionPolicy &planPolicy,
                                     bool *simpleEqualityMatch,
                                     const shared_ptr<const ParsedQuery> &parsedQuery,
                                     bool requireOrder,
                                     QueryPlanSummary *singlePlanSummary ) :
    _ns( ns ),
    _query( query ),
    _order( order ),
    _planPolicy( planPolicy ),
    _simpleEqualityMatch( simpleEqualityMatch ),
    _parsedQuery( parsedQuery ),
    _requireOrder( requireOrder ),
    _singlePlanSummary( singlePlanSummary ) {
        // Initialize optional return variables.
        if ( _simpleEqualityMatch ) {
            *_simpleEqualityMatch = false;
        }
        if ( _singlePlanSummary ) {
            *_singlePlanSummary = QueryPlanSummary();
        }
    }
    
    void CursorGenerator::setArgumentsHint() {
        if ( useHints && _parsedQuery ) {
            _argumentsHint = _parsedQuery->getHint();
        }
        
        if ( snapshot() ) {
            NamespaceDetails *d = nsdetails( _ns );
            if ( d ) {
                int i = d->findIdIndex();
                if( i < 0 ) {
                    if ( strstr( _ns , ".system." ) == 0 )
                        log() << "warning: no _id index on $snapshot query, ns:" << _ns << endl;
                }
                else {
                    /* [dm] the name of an _id index tends to vary, so we build the hint the hard
                     way here. probably need a better way to specify "use the _id index" as a hint.
                     if someone is in the query optimizer please fix this then!
                     */
                    _argumentsHint = BSON( "$hint" << d->idx(i).indexName() );
                }
            }
        }
    }
    
    shared_ptr<Cursor> CursorGenerator::shortcutCursor() const {
        if ( !mayShortcutQueryOptimizer() ) {
            return shared_ptr<Cursor>();
        }
        
        if ( _planPolicy.permitOptimalNaturalPlan() && _query.isEmpty() && _order.isEmpty() ) {
            // Table-scan
            NamespaceDetails *d = nsdetails(_ns);
            return shared_ptr<Cursor>(new BasicCursor(d));
        }
        if ( _planPolicy.permitOptimalIdPlan() && isSimpleIdQuery( _query ) ) {
            Database *database = cc().database();
            verify( database );
            NamespaceDetails *d = database->namespaceIndex.details( _ns );
            if ( d ) {
                int idxNo = d->findIdIndex();
                if ( idxNo >= 0 ) {
                    IndexDetails& i = d->idx( idxNo );
                    BSONObj key = i.getKeyFromQuery( _query );
                    return shared_ptr<Cursor>( IndexCursor::make( d, idxNo, i, key, key, true,
                                                                 1 ) );
                }
            }
        }
        
        return shared_ptr<Cursor>();
    }
    
    void CursorGenerator::setMultiPlanScanner() {
        _mps.reset( MultiPlanScanner::make( _ns, _query, _order, _parsedQuery, hint(),
                                           explain() ? QueryPlanGenerator::Ignore :
                                                QueryPlanGenerator::Use,
                                           min(), max() ) );
    }
    
    shared_ptr<Cursor> CursorGenerator::singlePlanCursor() {
        const QueryPlan *singlePlan = _mps->singlePlan();
        if ( !singlePlan || ( isOrderRequired() && singlePlan->scanAndOrderRequired() ) ) {
            return shared_ptr<Cursor>();
        }
        if ( !_planPolicy.permitPlan( *singlePlan ) ) {
            return shared_ptr<Cursor>();
        }
        
        if ( _singlePlanSummary ) {
            *_singlePlanSummary = singlePlan->summary();
        }
        shared_ptr<Cursor> single = singlePlan->newCursor();
        if ( !_query.isEmpty() && !single->matcher() ) {
            single->setMatcher( singlePlan->matcher() );
        }
        if ( singlePlan->keyFieldsOnly() ) {
            single->setKeyFieldsOnly( singlePlan->keyFieldsOnly() );
        }
        if ( _simpleEqualityMatch ) {
            if ( singlePlan->exactKeyMatch() && !single->matcher()->needRecord() ) {
                *_simpleEqualityMatch = true;
            }
        }
        return single;
    }
    
    shared_ptr<Cursor> CursorGenerator::generate() {

        setArgumentsHint();
        shared_ptr<Cursor> cursor = shortcutCursor();
        if ( cursor ) {
            return cursor;
        }
        
        setMultiPlanScanner();
        cursor = singlePlanCursor();
        if ( cursor ) {
            return cursor;
        }
        
        return newQueryOptimizerCursor( _mps, _planPolicy, isOrderRequired(), explain() );
    }

    /** This interface is just available for testing. */
    shared_ptr<Cursor> newQueryOptimizerCursor
    ( const char *ns, const BSONObj &query, const BSONObj &order,
     const QueryPlanSelectionPolicy &planPolicy, bool requireOrder,
     const shared_ptr<const ParsedQuery> &parsedQuery ) {
        auto_ptr<MultiPlanScanner> mps( MultiPlanScanner::make( ns, query, order, parsedQuery ) );
        return newQueryOptimizerCursor( mps, planPolicy, requireOrder, false );
    }
        
} // namespace mongo;
