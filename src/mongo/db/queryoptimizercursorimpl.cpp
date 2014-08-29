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

#include "mongo/pch.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/collection.h"
#include "mongo/db/queryoptimizercursorimpl.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/cursor.h"
#include "mongo/db/explain.h"
#include "mongo/db/query_plan_summary.h"
#include "mongo/db/query_optimizer_internal.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/exception.h"

namespace mongo {
    
    QueryOptimizerCursorImpl* QueryOptimizerCursorImpl::make
            ( auto_ptr<MultiPlanScanner>& mps,
              const QueryPlanSelectionPolicy& planPolicy,
              bool requireOrder,
              bool explain ) {
        auto_ptr<QueryOptimizerCursorImpl> ret( new QueryOptimizerCursorImpl( mps, planPolicy,
                                                                              requireOrder ) );
        ret->init( explain );
        return ret.release();
    }
        
    bool QueryOptimizerCursorImpl::ok() {
        return _takeover ? _takeover->ok() : !_currPK().isEmpty();
    }
        
    BSONObj QueryOptimizerCursorImpl::current() {
        if ( _takeover ) {
            return _takeover->current();
        }
        assertOk();
        return _currRunner->current();
    }
        
    BSONObj QueryOptimizerCursorImpl::currPK() const {
        return _takeover ? _takeover->currPK() : _currPK();
    }
        
    BSONObj QueryOptimizerCursorImpl::_currPK() const {
        dassert( !_takeover );
        return _currRunner ? _currRunner->currPK() : BSONObj();
    }

    bool QueryOptimizerCursorImpl::advance() {
        return _advance( false );
    }

    BSONObj QueryOptimizerCursorImpl::currKey() const {
        if ( _takeover ) {
            return _takeover->currKey();
        }
        assertOk();
        return _currRunner->currKey();
    }

    BSONObj QueryOptimizerCursorImpl::indexKeyPattern() const {
        if ( _takeover ) {
            return _takeover->indexKeyPattern();
        }
        assertOk();
        return _currRunner->cursor()->indexKeyPattern();
    }

    bool QueryOptimizerCursorImpl::getsetdup(const BSONObj &pk) {
        if ( _takeover ) {
            if ( getdupInternal( pk ) ) {
                return true;
            }
            return _takeover->getsetdup( pk );
        }
        assertOk();
        return getsetdupInternal( pk );
    }
        
    bool QueryOptimizerCursorImpl::isMultiKey() const {
        if ( _takeover ) {
            return _takeover->isMultiKey();
        }
        assertOk();
        return _currRunner->cursor()->isMultiKey();
    }
        
    long long QueryOptimizerCursorImpl::nscanned() const {
        return _takeover ? _takeover->nscanned() : _nscanned;
    }

    CoveredIndexMatcher* QueryOptimizerCursorImpl::matcher() const {
        if ( _takeover ) {
            return _takeover->matcher();
        }
        assertOk();
        return _currRunner->queryPlan().matcher().get();
    }

    bool QueryOptimizerCursorImpl::currentMatches( MatchDetails* details ) {
        if ( _takeover ) {
            return _takeover->currentMatches( details );
        }
        assertOk();
        return _currRunner->currentMatches( details );
    }
        
    const FieldRangeSet* QueryOptimizerCursorImpl::initialFieldRangeSet() const {
        if ( _takeover ) {
            return 0;
        }
        assertOk();
        return &_currRunner->queryPlan().multikeyFrs();
    }
        
    bool QueryOptimizerCursorImpl::currentPlanScanAndOrderRequired() const {
        if ( _takeover ) {
            return _takeover->queryPlan().scanAndOrderRequired();
        }
        assertOk();
        return _currRunner->queryPlan().scanAndOrderRequired();
    }
        
    const Projection::KeyOnly* QueryOptimizerCursorImpl::keyFieldsOnly() const {
        if ( _takeover ) {
            return _takeover->keyFieldsOnly();
        }
        assertOk();
        return _currRunner->keyFieldsOnly();
    }
        
    bool QueryOptimizerCursorImpl::runningInitialInOrderPlan() const {
        if ( _takeover ) {
            return false;
        }
        assertOk();
        return _mps->haveInOrderPlan();
    }

    bool QueryOptimizerCursorImpl::hasPossiblyExcludedPlans() const {
        if ( _takeover ) {
            return false;
        }
        assertOk();
        return _mps->hasPossiblyExcludedPlans();
    }

    void QueryOptimizerCursorImpl::clearIndexesForPatterns() {
        if ( !_takeover ) {
            _mps->clearIndexesForPatterns();
        }
    }
        
    void QueryOptimizerCursorImpl::abortOutOfOrderPlans() {
        _requireOrder = true;
    }

    void QueryOptimizerCursorImpl::noteIterate( bool match, bool loadedDocument, bool chunkSkip ) {
        if ( _explainQueryInfo ) {
            _explainQueryInfo->noteIterate( match, loadedDocument, chunkSkip );
        }
        if ( _takeover ) {
            _takeover->noteIterate( match, loadedDocument );
        }
    }
        
    QueryOptimizerCursorImpl::QueryOptimizerCursorImpl( auto_ptr<MultiPlanScanner>& mps,
                                                        const QueryPlanSelectionPolicy& planPolicy,
                                                        bool requireOrder ) :
        _requireOrder( requireOrder ),
        _mps( mps ),
        _initialCandidatePlans( _mps->possibleInOrderPlan(), _mps->possibleOutOfOrderPlan() ),
        _originalRunner( new QueryPlanRunner( _nscanned,
                                              planPolicy,
                                              _requireOrder,
                                              !_initialCandidatePlans.hybridPlanSet() ) ),
        _currRunner(),
        _completePlanOfHybridSetScanAndOrderRequired(),
        _nscanned() {
    }
        
    void QueryOptimizerCursorImpl::init( bool explain ) {
        _mps->initialRunner( _originalRunner );
        if ( explain ) {
            _explainQueryInfo = _mps->generateExplainInfo();
        }
        shared_ptr<QueryPlanRunner> runner = _mps->nextRunner();
        rethrowOnError( runner );
        if ( !runner->complete() ) {
            _currRunner = runner.get();
        }
    }

    bool QueryOptimizerCursorImpl::_advance( bool force ) {
        if ( _takeover ) {
            return _takeover->advance();
        }

        if ( !force && !ok() ) {
            return false;
        }

        _currRunner = 0;
        shared_ptr<QueryPlanRunner> runner = _mps->nextRunner();
        rethrowOnError( runner );

        if ( !runner->complete() ) {
            // The 'runner' will be valid until we call _mps->nextOp() again.  We return 'current'
            // values from this op.
            _currRunner = runner.get();
        }
        else if ( runner->stopRequested() ) {
            if ( runner->cursor() ) {
                _takeover.reset( new MultiCursor( _mps,
                                                  runner->cursor(),
                                                  runner->queryPlan().matcher(),
                                                  runner->explainInfo(),
                                                  *runner,
                                                  _nscanned - runner->cursor()->nscanned() ) );
            }
        }
        else {
            if ( _initialCandidatePlans.hybridPlanSet() ) {
                _completePlanOfHybridSetScanAndOrderRequired =
                        runner->queryPlan().scanAndOrderRequired();
            }
        }

        return ok();
    }
    
    /** Forward an exception when the runner errs out. */
    void QueryOptimizerCursorImpl::rethrowOnError( const shared_ptr< QueryPlanRunner > &runner ) {
        if ( runner->error() ) {
            throw MsgAssertionException( runner->exception() );   
        }
    }

    bool QueryOptimizerCursorImpl::getsetdupInternal(const BSONObj &pk) {
        return _dups.getsetdup( pk );
    }

    bool QueryOptimizerCursorImpl::getdupInternal(const BSONObj &pk) {
        dassert( _takeover );
        return _dups.getdup( pk );
    }
    
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
    
    CursorGenerator::CursorGenerator( const StringData &ns,
                                     const BSONObj &query,
                                     const BSONObj &order,
                                     const QueryPlanSelectionPolicy &planPolicy,
                                     const shared_ptr<const ParsedQuery> &parsedQuery,
                                     bool requireOrder,
                                     QueryPlanSummary *singlePlanSummary ) :
    _ns( ns ),
    _query( query ),
    _order( order ),
    _planPolicy( planPolicy ),
    _parsedQuery( parsedQuery ),
    _requireOrder( requireOrder ),
    _singlePlanSummary( singlePlanSummary ) {
        // Initialize optional return variables.
        if ( _singlePlanSummary ) {
            *_singlePlanSummary = QueryPlanSummary();
        }
    }
    
    BSONObj CursorGenerator::hint() const {
        return _argumentsHint.isEmpty() ? _planPolicy.planHint( _ns ) : _argumentsHint;
    }

    void CursorGenerator::setArgumentsHint() {
        if (storageGlobalParams.useHints && _parsedQuery) {
            _argumentsHint = _parsedQuery->getHint();
        }
    }
    
    shared_ptr<Cursor> CursorGenerator::shortcutCursor() const {
        if ( !mayShortcutQueryOptimizer() ) {
            return shared_ptr<Cursor>();
        }
        
        Collection *cl = getCollection(_ns);
        const int numWanted = _parsedQuery ? _parsedQuery->getSkip() + _parsedQuery->getNumToReturn() : 0;
        if ( _planPolicy.permitOptimalNaturalPlan() && _query.isEmpty() && _order.isEmpty() ) {
            // Table-scan
            return Cursor::make(cl, 1, _planPolicy.requestCountingCursor());
        }

        if (cl != NULL && isSimpleIdQuery(_query)) {
            const int idxNo = cl->findIdIndex();
            if (idxNo >= 0) {
                IndexDetails &idx = cl->idx(idxNo);
                const BSONObj key = idx.getKeyFromQuery(_query);
                return Cursor::make(cl, idx, key, key, true, 1, numWanted);
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

        bool needMatcher = 
            // If a matcher is requested or ...
            _planPolicy.requestMatcher() ||
            // ... the index ranges do not exactly match the query or ...
            singlePlan->mayBeMatcherNecessary() ||
            // ... the matcher must look at the full record
            singlePlan->matcher()->needRecord();

        // Counting cursors cannot utilize a matcher, so do not request one
        // if a matcher is needed.
        shared_ptr<Cursor> single = singlePlan->newCursor( !needMatcher && _planPolicy.requestCountingCursor() );
        if ( !_query.isEmpty() && !single->matcher() ) {

            // The query plan must have a matcher.  The matcher's constructor performs some aspects
            // of query validation that should occur before a cursor is returned.
            fassert( 16449, singlePlan->matcher() );

            if ( needMatcher ) {
                single->setMatcher( singlePlan->matcher() );
            }
        }
        if ( singlePlan->keyFieldsOnly() ) {
            single->setKeyFieldsOnly( singlePlan->keyFieldsOnly() );
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
