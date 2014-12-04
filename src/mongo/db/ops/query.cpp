// query.cpp

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

#include "mongo/db/ops/query.h"

#include "mongo/bson/util/builder.h"
#include "mongo/db/client.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/commands.h"
#include "mongo/db/parsed_query.h"
#include "mongo/db/query_plan_summary.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/db/query_optimizer_internal.h"
#include "mongo/db/queryoptimizercursor.h"
#include "mongo/db/replutil.h"
#include "mongo/db/scanandorder.h"
#include "mongo/s/d_logic.h"
#include "mongo/s/stale_exception.h"  // for SendStaleConfigException
#include "mongo/server.h"

namespace mongo {

    /* We cut off further objects once we cross this threshold; thus, you might get
       a little bit more than this, it is a threshold rather than a limit.
    */
    const int32_t MaxBytesToReturnToClientAtOnce = 4 * 1024 * 1024;

    bool runCommands(const char *ns, BSONObj& jsobj, CurOp& curop, BufBuilder &b, BSONObjBuilder& anObjBuilder, bool fromRepl, int queryOptions) {
        try {
            return _runCommands(ns, jsobj, b, anObjBuilder, fromRepl, queryOptions);
        }
        catch( SendStaleConfigException& ){
            throw;
        }
        catch ( AssertionException& e ) {
            verify( e.getCode() != SendStaleConfigCode && e.getCode() != RecvStaleConfigCode );

            e.getInfo().append( anObjBuilder , "assertion" , "assertionCode" );
            curop.debug().exceptionInfo = e.getInfo();
        }
        anObjBuilder.append("errmsg", "db assertion failure");
        anObjBuilder.append("ok", 0.0);
        BSONObj x = anObjBuilder.done();
        b.appendBuf((void*) x.objdata(), x.objsize());
        return true;
    }


    BSONObj id_obj = fromjson("{\"_id\":1}");
    BSONObj empty_obj = fromjson("{}");


    //int dump = 0;

    /* empty result for error conditions */
    QueryResult* emptyMoreResult(long long cursorid) {
        BufBuilder b(32768);
        b.skip(sizeof(QueryResult));
        QueryResult *qr = (QueryResult *) b.buf();
        qr->cursorId = 0; // 0 indicates no more data to retrieve.
        qr->startingFrom = 0;
        qr->len = b.len();
        qr->setOperation(opReply);
        qr->initializeResultFlags();
        qr->nReturned = 0;
        b.decouple();
        return qr;
    }


    bool queryByPKHack(Collection *cl, const BSONObj &pk,
                       const BSONObj &query, BSONObj &result,
                       ResultDetails *resDetails) {
        cc().curop()->debug().idhack = true;

        BSONObj obj;
        bool objMatches = true;
        const bool found = cl->findByPK(pk, obj);
        if (found) {
            // Only use a matcher for queries with more than just an _id
            // component. The _id was already 'matched' by the find.
            const bool singleQueryField = query.nFields() == 1; // TODO: Optimize?
            if (!singleQueryField) {
                Matcher matcher(query);
                objMatches = matcher.matches(obj, &resDetails->matchDetails);
            }
        }

        const bool ok = found && objMatches;
        cl->getPKIndex().noteQuery(ok ? 1 : 0, 0);
        result = ok ? obj : BSONObj();
        return ok;
    }

    QueryResult* processGetMore(const char* ns,
                                int ntoreturn,
                                long long cursorid,
                                CurOp& curop,
                                int pass,
                                bool& exhaust,
                                bool* isCursorAuthorized ) {
        exhaust = false;
        ClientCursor::Pin p(cursorid);
        ClientCursor *client_cursor = p.c();

        int bufSize = 512 + sizeof( QueryResult ) + MaxBytesToReturnToClientAtOnce;

        BufBuilder b( bufSize );
        b.skip(sizeof(QueryResult));
        int resultFlags = ResultFlag_AwaitCapable;
        int start = 0;
        int n = 0;

        if ( unlikely(!client_cursor) ) {
            LOGSOME << "getMore: cursorid not found " << ns << " " << cursorid << endl;
            cursorid = 0;
            resultFlags = ResultFlag_CursorNotFound;
        }
        else {
            // check for spoofing of the ns such that it does not match the one originally there for the cursor
            uassert(14833, "auth error", str::equals(ns, client_cursor->ns().c_str()));

            int queryOptions = client_cursor->queryOptions();
            OpSettings settings;
            settings.setBulkFetch(true);
            settings.setQueryCursorMode(DEFAULT_LOCK_CURSOR);
            settings.setCappedAppendPK(queryOptions & QueryOption_AddHiddenPK);
            cc().setOpSettings(settings);

            // Check if the cursor is part of a multi-statement transaction. If it is
            // and this is not the right client (meaning the current transaction stack
            // does not match that in the cursor), it will uassert. If the cursor is
            // not part of a multi-statement transaction, then we need to use the stack
            // in the cursor for this scope.
            const bool cursorPartOfMultiStatementTxn = client_cursor->checkMultiStatementTxn();
            scoped_ptr<Client::WithTxnStack> wts;
            if (!cursorPartOfMultiStatementTxn) {
                // For simplicity, prevent multi-statement transactions from
                // reading cursors it didn't create.
                uassert(16813, "Cannot getMore() on a cursor not created by this multi-statement transaction",
                           !cc().hasTxn());
                wts.reset(new Client::WithTxnStack(client_cursor->transactions)); 
            }

            *isCursorAuthorized = true;

            if (pass == 0) {
                client_cursor->updateSlaveLocation( curop );
            }
            
            curop.debug().query = client_cursor->query();

            start = client_cursor->pos();
            Cursor *c = client_cursor->c();

            // This manager may be stale, but it's the state of chunking when the cursor was created.
            ShardChunkManagerPtr manager = client_cursor->getChunkManager();

            while ( 1 ) {
                if ( !c->ok() ) {
                    if ( c->tailable() ) {
                        /* when a tailable cursor hits "EOF", ok() goes false, and current() is null.  however
                           advance() can still be retries as a reactivation attempt.  when there is new data, it will
                           return true.  that's what we are doing here.
                           */
                        if ( c->advance() )
                            continue;

                        if( n == 0 && (queryOptions & QueryOption_AwaitData) && pass < 1000 ) {
                            return 0;
                        }

                        break;
                    }
                    p.release();

                    // Done with this cursor, steal transaction stack back to commit or abort it here.
                    bool ok = ClientCursor::erase(cursorid);
                    verify(ok);
                    cursorid = 0;
                    client_cursor = 0;
                    break;
                }

                MatchDetails details;
                if ( client_cursor->fields && client_cursor->fields->getArrayOpType() == Projection::ARRAY_OP_POSITIONAL ) {
                    // field projection specified, and contains an array operator
                    details.requestElemMatchKey();
                }

                // in some cases (clone collection) there won't be a matcher
                if ( !c->currentMatches( &details ) ) {
                }
                else if ( manager && ! manager->belongsToMe( client_cursor ) ){
                    LOG(2) << "cursor skipping document in un-owned chunk: " << c->current() << endl;
                }
                else {
                    if( c->getsetdup(c->currPK()) ) {
                        //out() << "  but it's a dup \n";
                    }
                    else {
                        // save this so that at the end of the loop,
                        // we can update the location for write concern
                        // in replication. Note that if this cursor is not
                        // doing replication, this is pointless
                        if ( client_cursor->queryOptions() & QueryOption_OplogReplay ) {
                            client_cursor->storeOpForSlave( c->current() );
                        }
                        n++;

                        client_cursor->fillQueryResultFromObj( b, &details );

                        if ( ( ntoreturn && n >= ntoreturn ) || b.len() > MaxBytesToReturnToClientAtOnce ) {
                            c->advance();
                            client_cursor->incPos( n );
                            break;
                        }
                    }
                }
                c->advance();
            }
            
            if ( client_cursor ) {
                client_cursor->resetIdleAge();
                exhaust = client_cursor->queryOptions() & QueryOption_Exhaust;
            } else if (!cursorPartOfMultiStatementTxn) {
                // This cursor is done and it wasn't part of a multi-statement
                // transaction. We can commit the transaction now.
                cc().commitTopTxn();
                wts->release();
            }
        }

        QueryResult *qr = (QueryResult *) b.buf();
        qr->len = b.len();
        qr->setOperation(opReply);
        qr->_resultFlags() = resultFlags;
        qr->cursorId = cursorid;
        qr->startingFrom = start;
        qr->nReturned = n;
        b.decouple();

        return qr;
    }

    ResultDetails::ResultDetails() :
        match(),
        orderedMatch(),
        loadedRecord(),
        chunkSkip() {
    }

    ExplainRecordingStrategy::ExplainRecordingStrategy
    ( const ExplainQueryInfo::AncillaryInfo &ancillaryInfo ) :
    _ancillaryInfo( ancillaryInfo ) {
    }

    shared_ptr<ExplainQueryInfo> ExplainRecordingStrategy::doneQueryInfo() {
        shared_ptr<ExplainQueryInfo> ret = _doneQueryInfo();
        ret->setAncillaryInfo( _ancillaryInfo );
        return ret;
    }
    
    NoExplainStrategy::NoExplainStrategy() :
    ExplainRecordingStrategy( ExplainQueryInfo::AncillaryInfo() ) {
    }

    shared_ptr<ExplainQueryInfo> NoExplainStrategy::_doneQueryInfo() {
        verify( false );
        return shared_ptr<ExplainQueryInfo>();
    }
    
    MatchCountingExplainStrategy::MatchCountingExplainStrategy
    ( const ExplainQueryInfo::AncillaryInfo &ancillaryInfo ) :
    ExplainRecordingStrategy( ancillaryInfo ),
    _orderedMatches() {
    }
    
    void MatchCountingExplainStrategy::noteIterate( const ResultDetails& resultDetails ) {
        _noteIterate( resultDetails );
        if ( resultDetails.orderedMatch ) {
            ++_orderedMatches;
        }
    }
    
    SimpleCursorExplainStrategy::SimpleCursorExplainStrategy
    ( const ExplainQueryInfo::AncillaryInfo &ancillaryInfo,
     const shared_ptr<Cursor> &cursor ) :
    MatchCountingExplainStrategy( ancillaryInfo ),
    _cursor( cursor ),
    _explainInfo( new ExplainSinglePlanQueryInfo() ) {
    }
 
    void SimpleCursorExplainStrategy::notePlan( bool scanAndOrder, bool indexOnly ) {
        _explainInfo->notePlan( *_cursor, scanAndOrder, indexOnly );
    }

    void SimpleCursorExplainStrategy::_noteIterate( const ResultDetails& resultDetails ) {
        _explainInfo->noteIterate( resultDetails.match,
                                   resultDetails.loadedRecord,
                                   resultDetails.chunkSkip,
                                   *_cursor );
    }

    shared_ptr<ExplainQueryInfo> SimpleCursorExplainStrategy::_doneQueryInfo() {
        _explainInfo->noteDone( *_cursor );
        return _explainInfo->queryInfo();
    }

    QueryOptimizerCursorExplainStrategy::QueryOptimizerCursorExplainStrategy
    ( const ExplainQueryInfo::AncillaryInfo &ancillaryInfo,
     const shared_ptr<QueryOptimizerCursor> &cursor ) :
    MatchCountingExplainStrategy( ancillaryInfo ),
    _cursor( cursor ) {
    }
    
    void QueryOptimizerCursorExplainStrategy::_noteIterate( const ResultDetails& resultDetails ) {
        // Note ordered matches only; if an unordered plan is selected, the explain result will
        // be updated with reviseN().
        _cursor->noteIterate( resultDetails.orderedMatch,
                              resultDetails.loadedRecord,
                              resultDetails.chunkSkip );
    }

    shared_ptr<ExplainQueryInfo> QueryOptimizerCursorExplainStrategy::_doneQueryInfo() {
        return _cursor->explainQueryInfo();
    }

    ResponseBuildStrategy::ResponseBuildStrategy( const ParsedQuery &parsedQuery,
                                                  const shared_ptr<Cursor> &cursor,
                                                  BufBuilder &buf ) :
    _parsedQuery( parsedQuery ),
    _cursor( cursor ),
    _queryOptimizerCursor( dynamic_pointer_cast<QueryOptimizerCursor>( _cursor ) ),
    _buf( buf ) {
    }

    void ResponseBuildStrategy::resetBuf() {
        _buf.reset();
        _buf.skip( sizeof( QueryResult ) );
    }

    BSONObj ResponseBuildStrategy::current( bool allowCovered,
                                            ResultDetails* resultDetails ) const {
        if ( _parsedQuery.returnKey() ) {
            BSONObjBuilder bob;
            bob.appendKeys( _cursor->indexKeyPattern(), _cursor->currKey() );
            return bob.obj();
        }
        if ( allowCovered ) {
            const Projection::KeyOnly *keyFieldsOnly = _cursor->keyFieldsOnly();
            if ( keyFieldsOnly ) {
                return keyFieldsOnly->hydrate( _cursor->currKey(), _cursor->currPK() );
            }
        }
        resultDetails->loadedRecord = true;
        BSONObj ret = _cursor->current();
        verify( ret.isValid() );
        return ret;
    }

    OrderedBuildStrategy::OrderedBuildStrategy( const ParsedQuery &parsedQuery,
                                               const shared_ptr<Cursor> &cursor,
                                               BufBuilder &buf ) :
    ResponseBuildStrategy( parsedQuery, cursor, buf ),
    _skip( _parsedQuery.getSkip() ),
    _bufferedMatches() {
    }
    
    bool OrderedBuildStrategy::handleMatch( ResultDetails* resultDetails ) {
        const BSONObj pk = _cursor->currPK();
        if ( _cursor->getsetdup( pk ) ) {
            return false;
        }
        if ( _skip > 0 ) {
            --_skip;
            return false;
        }
        BSONObj currentDocument = current( true, resultDetails );
        // Explain does not obey soft limits, so matches should not be buffered.
        if ( !_parsedQuery.isExplain() ) {
            fillQueryResultFromObj( _buf, _parsedQuery.getFields(),
                                    currentDocument, &resultDetails->matchDetails );
            ++_bufferedMatches;
        }
        resultDetails->match = true;
        resultDetails->orderedMatch = true;
        return true;
    }

    ReorderBuildStrategy* ReorderBuildStrategy::make( const ParsedQuery& parsedQuery,
                                                      const shared_ptr<Cursor>& cursor,
                                                      BufBuilder& buf,
                                                      const QueryPlanSummary& queryPlan ) {
        auto_ptr<ReorderBuildStrategy> ret( new ReorderBuildStrategy( parsedQuery, cursor, buf ) );
        ret->init( queryPlan );
        return ret.release();
    }

    ReorderBuildStrategy::ReorderBuildStrategy( const ParsedQuery &parsedQuery,
                                               const shared_ptr<Cursor> &cursor,
                                               BufBuilder &buf ) :
    ResponseBuildStrategy( parsedQuery, cursor, buf ),
    _bufferedMatches() {
    }
    
    void ReorderBuildStrategy::init( const QueryPlanSummary &queryPlan ) {
        _scanAndOrder.reset( newScanAndOrder( queryPlan ) );
    }

    bool ReorderBuildStrategy::handleMatch( ResultDetails* resultDetails ) {
        if ( _cursor->getsetdup( _cursor->currPK() ) ) {
            return false;
        }
        _handleMatchNoDedup( resultDetails );
        resultDetails->match = true;
        return true;
    }
    
    void ReorderBuildStrategy::_handleMatchNoDedup( ResultDetails* resultDetails ) {
        _scanAndOrder->add( current( false, resultDetails ) );
    }

    int ReorderBuildStrategy::rewriteMatches() {
        cc().curop()->debug().scanAndOrder = true;
        int ret = 0;
        _scanAndOrder->fill( _buf, &_parsedQuery, ret );
        _bufferedMatches = ret;
        return ret;
    }
    
    ScanAndOrder *
    ReorderBuildStrategy::newScanAndOrder( const QueryPlanSummary &queryPlan ) const {
        verify( !_parsedQuery.getOrder().isEmpty() );
        verify( _cursor->ok() );
        const FieldRangeSet *fieldRangeSet = 0;
        if ( queryPlan.valid() ) {
            fieldRangeSet = queryPlan.fieldRangeSetMulti.get();
        }
        else {
            verify( _queryOptimizerCursor );
            fieldRangeSet = _queryOptimizerCursor->initialFieldRangeSet();
        }
        verify( fieldRangeSet );
        return new ScanAndOrder( _parsedQuery.getSkip(),
                                _parsedQuery.getNumToReturn(),
                                _parsedQuery.getOrder(),
                                *fieldRangeSet );
    }

    HybridBuildStrategy* HybridBuildStrategy::make( const ParsedQuery& parsedQuery,
                                                    const shared_ptr<QueryOptimizerCursor>& cursor,
                                                    BufBuilder& buf ) {
        auto_ptr<HybridBuildStrategy> ret( new HybridBuildStrategy( parsedQuery, cursor, buf ) );
        ret->init();
        return ret.release();
    }

    HybridBuildStrategy::HybridBuildStrategy( const ParsedQuery &parsedQuery,
                                             const shared_ptr<QueryOptimizerCursor> &cursor,
                                             BufBuilder &buf ) :
    ResponseBuildStrategy( parsedQuery, cursor, buf ),
    _orderedBuild( _parsedQuery, _cursor, _buf ),
    _reorderedMatches() {
    }

    void HybridBuildStrategy::init() {
        _reorderBuild.reset( ReorderBuildStrategy::make( _parsedQuery, _cursor, _buf,
                                                         QueryPlanSummary() ) );
    }

    bool HybridBuildStrategy::handleMatch( ResultDetails* resultDetails ) {
        if ( !_queryOptimizerCursor->currentPlanScanAndOrderRequired() ) {
            return _orderedBuild.handleMatch( resultDetails );
        }
        return handleReorderMatch( resultDetails );
    }
    
    bool HybridBuildStrategy::handleReorderMatch( ResultDetails* resultDetails ) {
        const BSONObj pk = _cursor->currPK();
        if ( _scanAndOrderDups.getsetdup( pk ) ) {
            return false;
        }
        resultDetails->match = true;
        try {
            _reorderBuild->_handleMatchNoDedup( resultDetails );
        } catch ( const UserException &e ) {
            if ( e.getCode() == ScanAndOrderMemoryLimitExceededAssertionCode ) {
                if ( _queryOptimizerCursor->hasPossiblyExcludedPlans() ) {
                    _queryOptimizerCursor->clearIndexesForPatterns();
                    throw QueryRetryException();
                }
                else if ( _queryOptimizerCursor->runningInitialInOrderPlan() ) {
                    _queryOptimizerCursor->abortOutOfOrderPlans();
                    return true;
                }
            }
            throw;
        }
        return true;
    }
    
    int HybridBuildStrategy::rewriteMatches() {
        if ( !_queryOptimizerCursor->completePlanOfHybridSetScanAndOrderRequired() ) {
            return _orderedBuild.rewriteMatches();
        }
        _reorderedMatches = true;
        resetBuf();
        return _reorderBuild->rewriteMatches();
    }

    int HybridBuildStrategy::bufferedMatches() const {
        return _reorderedMatches ?
                _reorderBuild->bufferedMatches() :
                _orderedBuild.bufferedMatches();
    }

    void HybridBuildStrategy::finishedFirstBatch() {
        _queryOptimizerCursor->abortOutOfOrderPlans();
    }
    
    QueryResponseBuilder *QueryResponseBuilder::make( const ParsedQuery &parsedQuery,
                                                     const shared_ptr<Cursor> &cursor,
                                                     const QueryPlanSummary &queryPlan,
                                                     const BSONObj &oldPlan ) {
        auto_ptr<QueryResponseBuilder> ret( new QueryResponseBuilder( parsedQuery, cursor ) );
        ret->init( queryPlan, oldPlan );
        return ret.release();
    }
    
    QueryResponseBuilder::QueryResponseBuilder( const ParsedQuery &parsedQuery,
                                               const shared_ptr<Cursor> &cursor ) :
    _parsedQuery( parsedQuery ),
    _cursor( cursor ),
    _queryOptimizerCursor( dynamic_pointer_cast<QueryOptimizerCursor>( _cursor ) ),
    _buf( 32768 ) { // TODO be smarter here
    }
    
    void QueryResponseBuilder::init( const QueryPlanSummary &queryPlan, const BSONObj &oldPlan ) {
        _chunkManager = newChunkManager();
        _explain = newExplainRecordingStrategy( queryPlan, oldPlan );
        _builder = newResponseBuildStrategy( queryPlan );
        _builder->resetBuf();
    }

    bool QueryResponseBuilder::addMatch() {
        ResultDetails resultDetails;

        if ( _parsedQuery.getFields() && _parsedQuery.getFields()->getArrayOpType() == Projection::ARRAY_OP_POSITIONAL ) {
            // field projection specified, and contains an array operator
            resultDetails.matchDetails.requestElemMatchKey();
        }

        bool match =
                currentMatches( &resultDetails ) &&
                chunkMatches( &resultDetails ) &&
                _builder->handleMatch( &resultDetails );

        _explain->noteIterate( resultDetails );
        return match;
    }

    bool QueryResponseBuilder::enoughForFirstBatch() const {
        return _parsedQuery.enoughForFirstBatch( _builder->bufferedMatches(), _buf.len() );
    }

    bool QueryResponseBuilder::enoughTotalResults() const {
        if ( _parsedQuery.isExplain() ) {
            return _parsedQuery.enoughForExplain( _explain->orderedMatches() );
        }
        return ( _parsedQuery.enough( _builder->bufferedMatches() ) ||
                _buf.len() >= MaxBytesToReturnToClientAtOnce );
    }

    void QueryResponseBuilder::finishedFirstBatch() {
        _builder->finishedFirstBatch();
    }

    int QueryResponseBuilder::handoff( Message &result ) {
        int rewriteCount = _builder->rewriteMatches();
        if ( _parsedQuery.isExplain() ) {
            shared_ptr<ExplainQueryInfo> explainInfo = _explain->doneQueryInfo();
            if ( rewriteCount != -1 ) {
                explainInfo->reviseN( rewriteCount );
            }
            _builder->resetBuf();
            fillQueryResultFromObj( _buf, 0, explainInfo->bson() );
            result.appendData( _buf.buf(), _buf.len() );
            _buf.decouple();
            return 1;
        }
        if ( _buf.len() > 0 ) {
            result.appendData( _buf.buf(), _buf.len() );
            _buf.decouple();
        }
        return _builder->bufferedMatches();
    }

    ShardChunkManagerPtr QueryResponseBuilder::newChunkManager() const {
        if ( !shardingState.needShardChunkManager( _parsedQuery.ns() ) ) {
            return ShardChunkManagerPtr();
        }
        return shardingState.getShardChunkManager( _parsedQuery.ns() );
    }

    shared_ptr<ExplainRecordingStrategy> QueryResponseBuilder::newExplainRecordingStrategy
    ( const QueryPlanSummary &queryPlan, const BSONObj &oldPlan ) const {
        if ( !_parsedQuery.isExplain() ) {
            return shared_ptr<ExplainRecordingStrategy>( new NoExplainStrategy() );
        }
        ExplainQueryInfo::AncillaryInfo ancillaryInfo;
        ancillaryInfo._oldPlan = oldPlan;
        if ( _queryOptimizerCursor ) {
            return shared_ptr<ExplainRecordingStrategy>
            ( new QueryOptimizerCursorExplainStrategy( ancillaryInfo, _queryOptimizerCursor ) );
        }
        shared_ptr<ExplainRecordingStrategy> ret
        ( new SimpleCursorExplainStrategy( ancillaryInfo, _cursor ) );
        ret->notePlan( queryPlan.valid() && queryPlan.scanAndOrderRequired,
                       queryPlan.keyFieldsOnly );
        return ret;
    }

    shared_ptr<ResponseBuildStrategy> QueryResponseBuilder::newResponseBuildStrategy
    ( const QueryPlanSummary &queryPlan ) {
        bool unordered = _parsedQuery.getOrder().isEmpty();
        bool empty = !_cursor->ok();
        bool singlePlan = !_queryOptimizerCursor;
        bool singleOrderedPlan =
                singlePlan && ( !queryPlan.valid() || !queryPlan.scanAndOrderRequired );
        CandidatePlanCharacter queryOptimizerPlans;
        if ( _queryOptimizerCursor ) {
            queryOptimizerPlans = _queryOptimizerCursor->initialCandidatePlans();
        }
        if ( unordered ||
            empty ||
            singleOrderedPlan ||
            ( !singlePlan && !queryOptimizerPlans.mayRunOutOfOrderPlan() ) ) {
            return shared_ptr<ResponseBuildStrategy>
            ( new OrderedBuildStrategy( _parsedQuery, _cursor, _buf ) );
        }
        if ( singlePlan ||
            !queryOptimizerPlans.mayRunInOrderPlan() ) {
            return shared_ptr<ResponseBuildStrategy>
            ( ReorderBuildStrategy::make( _parsedQuery, _cursor, _buf, queryPlan ) );
        }
        return shared_ptr<ResponseBuildStrategy>
        ( HybridBuildStrategy::make( _parsedQuery, _queryOptimizerCursor, _buf ) );
    }

    bool QueryResponseBuilder::currentMatches( ResultDetails* resultDetails ) {
        bool matches = _cursor->currentMatches( &resultDetails->matchDetails );
        if ( resultDetails->matchDetails.hasLoadedRecord() ) {
            resultDetails->loadedRecord = true;
        }
        return matches;
    }

    bool QueryResponseBuilder::chunkMatches( ResultDetails* resultDetails ) {
        if ( !_chunkManager ) {
            return true;
        }
        // TODO: should make this covered at some point
        resultDetails->loadedRecord = true;
        if ( _chunkManager->belongsToMe( _cursor->current() ) ) {
            return true;
        }
        resultDetails->chunkSkip = true;
        return false;
    }
    
    /**
     * Run a query with a cursor provided by the query optimizer, or FindingStartCursor.
     * @returns true if client cursor was saved, false if the query has completed.
     */
    bool queryWithQueryOptimizer( int queryOptions, const string& ns,
                                  const BSONObj &jsobj, CurOp& curop,
                                  const BSONObj &query, const BSONObj &order,
                                  const shared_ptr<ParsedQuery> &pq_shared,
                                  const ConfigVersion &shardingVersionAtStart,
                                  const bool getCachedExplainPlan,
                                  const bool inMultiStatementTxn,
                                  Message &result ) {

        const ParsedQuery &pq( *pq_shared );
        shared_ptr<Cursor> cursor;
        QueryPlanSummary queryPlan;

        const bool tailable = pq.hasOption( QueryOption_CursorTailable ) && pq.getNumToReturn() != 1;
        
        LOG(1) << "query beginning read-only transaction. tailable: " << tailable << endl;
        
        BSONObj oldPlan;
        if (getCachedExplainPlan) {
            scoped_ptr<MultiPlanScanner> mps( MultiPlanScanner::make( ns.c_str(), query, order ) );
            oldPlan = mps->cachedPlanExplainSummary();
        }
        
        cursor = getOptimizedCursor( ns.c_str(), query, order, QueryPlanSelectionPolicy::any(),
                                     pq_shared, false, &queryPlan );
        verify( cursor );

        // Tailable cursors must be marked as such before any use. This is so that
        // the implementation knows that uncommitted data cannot be returned.
        if ( tailable ) {
            cursor->setTailable();
        }

        scoped_ptr<QueryResponseBuilder> queryResponseBuilder
                ( QueryResponseBuilder::make( pq, cursor, queryPlan, oldPlan ) );
        bool saveClientCursor = false;
        int options = QueryOption_NoCursorTimeout;
        if (pq.hasOption( QueryOption_OplogReplay )) {
            options |= QueryOption_OplogReplay;
        }
        // create a client cursor that does not create a cursorID.
        // The cursor ID will be created if and only if we save
        // the client cursor further below
        ClientCursor::Holder ccPointer(
            new ClientCursor( options, cursor, ns, BSONObj(), false, false )
            );

        bool opChecked = false;
        bool slaveLocationUpdated = false;
        BSONObj last;
        bool lastBSONObjSet = false;
        for ( ; cursor->ok(); cursor->advance() ) {

            if ( pq.getMaxScan() && cursor->nscanned() > pq.getMaxScan() ) {
                break;
            }
            
            if ( !queryResponseBuilder->addMatch() ) {
                continue;
            }
            
            // Note slave's position in the oplog.
            if ( pq.hasOption( QueryOption_OplogReplay ) ) {
                BSONObj current = cursor->current();
                last = current.copy();
                lastBSONObjSet = true;
                
                // the first row returned is equal to the last element that
                // the slave has synced up to, so we might as well update
                // the slave location
                if (!slaveLocationUpdated) {
                    ccPointer->updateSlaveLocation(curop);
                    slaveLocationUpdated = true;
                }
                // check if data we are about to return may be too stale
                if (!opChecked) {
                    ccPointer->storeOpForSlave(current);
                    opChecked = true;
                }
            }
            
            if ( pq.isExplain() ) {
                if ( queryResponseBuilder->enoughTotalResults() ) {
                    break;
                }
            }
            else if ( queryResponseBuilder->enoughForFirstBatch() ) {
                // if only 1 requested, no cursor saved for efficiency...we assume it is findOne()
                if ( pq.wantMore() && pq.getNumToReturn() != 1 ) {
                    queryResponseBuilder->finishedFirstBatch();
                    if ( cursor->advance() ) {
                        saveClientCursor = true;
                    }
                }
                break;
            }
        }

        // If the tailing request succeeded
        if ( cursor->tailable() ) {
            saveClientCursor = true;
        }
        
        if ( ! shardingState.getVersion( ns ).isWriteCompatibleWith( shardingVersionAtStart ) ) {
            // if the version changed during the query
            // we might be missing some data
            // and its safe to send this as mongos can resend
            // at this point
            throw SendStaleConfigException( ns , "version changed during initial query", shardingVersionAtStart, shardingState.getVersion( ns ) );
        }
        
        int nReturned = queryResponseBuilder->handoff( result );

        ccPointer.reset();
        long long cursorid = 0;
        if ( saveClientCursor ) {
            // Create a new ClientCursor, with a default timeout.
            ccPointer.reset( new ClientCursor( queryOptions, cursor, ns,
                                               jsobj.getOwned(), inMultiStatementTxn ) );
            cursorid = ccPointer->cursorid();
            DEV tlog(2) << "query has more, cursorid: " << cursorid << endl;
            
            if ( !ccPointer->ok() && ccPointer->c()->tailable() ) {
                DEV tlog() << "query has no more but tailable, cursorid: " << cursorid << endl;
            }
            
            if( queryOptions & QueryOption_Exhaust ) {
                curop.debug().exhaust = true;
            }
            
            // Set attributes for getMore.
            ccPointer->setChunkManager( queryResponseBuilder->chunkManager() );
            ccPointer->setPos( nReturned );
            ccPointer->pq = pq_shared;
            ccPointer->fields = pq.getFieldPtr();
            if (pq.hasOption( QueryOption_OplogReplay ) && lastBSONObjSet) {
                ccPointer->storeOpForSlave(last);
            }
            if (!inMultiStatementTxn) {
                // This cursor is not part of a multi-statement transaction, so
                // we pass off the current client's transaction stack to the
                // cursor so that it may be live as long as the cursor.
                cc().swapTransactionStack(ccPointer->transactions);
                verify(!cc().hasTxn());
            }
            ccPointer.release();
        }

        QueryResult *qr = (QueryResult *) result.header();
        qr->cursorId = cursorid;
        curop.debug().cursorid = ( cursorid == 0 ? -1 : qr->cursorId );
        qr->setResultFlagsToOk();
        // qr->len is updated automatically by appendData()
        curop.debug().responseLength = qr->len;
        qr->setOperation(opReply);
        qr->startingFrom = 0;
        qr->nReturned = nReturned;

        curop.debug().nscanned = ( cursor ? cursor->nscanned() : 0LL );
        curop.debug().ntoskip = pq.getSkip();
        curop.debug().nreturned = nReturned;

        return saveClientCursor;
    }

    bool _tryQueryByPKHack(const char *ns, const BSONObj &query,
                           const ParsedQuery &pq, CurOp &curop, Message &result) {
        Collection *cl = getCollection(ns);
        if (cl == NULL) {
            return false; // ns doesn't exist, fall through to optimizer for legacy reasons
        }

        const BSONObj &pk = cl->getSimplePKFromQuery(query);
        if (pk.isEmpty()) {
            return false; // unable to query by PK - resort to using the optimizer
        }

        ResultDetails resDetails;
        if ( pq.getFields() && pq.getFields()->getArrayOpType() == Projection::ARRAY_OP_POSITIONAL ) {
            // field projection specified, and contains an array operator
            resDetails.matchDetails.requestElemMatchKey();
        }

        BSONObj resObject;
        bool found = queryByPKHack(cl, pk, query, resObject, &resDetails);

        if ( shardingState.needShardChunkManager( ns ) ) {
            ShardChunkManagerPtr m = shardingState.getShardChunkManager( ns );
            if ( m && ! m->belongsToMe( resObject ) ) {
                // I have something for this _id
                // but it doesn't belong to me
                // so return nothing
                resObject = BSONObj();
                found = false;
            }
        }

        BufBuilder bb(sizeof(QueryResult)+resObject.objsize()+32);
        bb.skip(sizeof(QueryResult));

        if ( found ) {
            fillQueryResultFromObj( bb , pq.getFields() , resObject, &resDetails.matchDetails );
        }

        auto_ptr< QueryResult > qr( (QueryResult *) bb.buf() );
        bb.decouple();
        qr->setResultFlagsToOk();
        qr->len = bb.len();

        curop.debug().responseLength = bb.len();
        qr->setOperation(opReply);
        qr->cursorId = 0;
        qr->startingFrom = 0;
        qr->nReturned = found ? 1 : 0;

        result.setData( qr.release(), true );
        return true;
    }

    /**
     * Run a query -- includes checking for and running a Command.
     * @return points to ns if exhaust mode. 0=normal mode
     * @locks the db mutex for reading (and potentially for writing temporarily to create a new db).
     * @asserts on scan and order memory exhaustion and other cases.
     */
    string runQuery(Message& m, QueryMessage& q, CurOp& curop, Message &result) {
        shared_ptr<ParsedQuery> pq_shared( new ParsedQuery(q) );
        ParsedQuery& pq( *pq_shared );
        BSONObj jsobj = q.query;
        int queryOptions = q.queryOptions;
        const char *ns = q.ns;

        uassert( 16332 , "can't have an empty ns" , ns[0] );

        if( logLevel >= 2 )
            log() << "runQuery called " << ns << " " << jsobj << endl;

        curop.debug().ns = ns;
        curop.debug().ntoreturn = pq.getNumToReturn();
        curop.debug().query = jsobj;
        curop.setQuery(jsobj);

        uassert( 16256, str::stream() << "Invalid ns [" << ns << "]", NamespaceString::isValid(ns) );

        // Run a command.
        
        if ( pq.couldBeCommand() ) {
            curop.markCommand();
            BufBuilder bb;
            bb.skip(sizeof(QueryResult));
            BSONObjBuilder cmdResBuf;
            if ( runCommands(ns, jsobj, curop, bb, cmdResBuf, false, queryOptions) ) {
                curop.debug().iscommand = true;
                curop.debug().query = jsobj;

                auto_ptr< QueryResult > qr;
                qr.reset( (QueryResult *) bb.buf() );
                bb.decouple();
                qr->setResultFlagsToOk();
                qr->len = bb.len();
                curop.debug().responseLength = bb.len();
                qr->setOperation(opReply);
                qr->cursorId = 0;
                qr->startingFrom = 0;
                qr->nReturned = 1;
                result.setData( qr.release(), true );
            }
            else {
                uasserted(13530, "bad or malformed command request?");
            }
            return "";
        }

        const bool explain = pq.isExplain();
        const bool tailable = pq.hasOption(QueryOption_CursorTailable);
        BSONObj order = pq.getOrder();
        BSONObj query = pq.getFilter();

        /* The ElemIter will not be happy if this isn't really an object. So throw exception
           here when that is true.
           (Which may indicate bad data from client.)
        */
        if ( query.objsize() == 0 ) {
            out() << "Bad query object?\n  jsobj:";
            out() << jsobj.toString() << "\n  query:";
            out() << query.toString() << endl;
            uassert( 10110 , "bad query object", false);
        }

        // Tailable cursors need to read newly written entries from the tail
        // of the collection. They manually arbitrate with the collection over
        // what data is readable and when, so we choose read uncommited isolation.
        OpSettings settings;
        settings.setQueryCursorMode(DEFAULT_LOCK_CURSOR);
        settings.setBulkFetch(true);
        settings.setCappedAppendPK(pq.hasOption(QueryOption_AddHiddenPK));
        cc().setOpSettings(settings);

        // If our caller has a transaction, it's multi-statement.
        const bool inMultiStatementTxn = cc().hasTxn();
        if (tailable) {
            // Because it's easier to disable this. It shouldn't be happening in a normal system.
            uassert(16812, "May not perform a tailable query in a multi-statement transaction.",
                           !inMultiStatementTxn);
        }

        // Begin a read-only, snapshot transaction under normal circumstances.
        // If the cursor is tailable, we need to be able to read uncommitted data.
        const int txnFlags = (tailable ? DB_READ_UNCOMMITTED : DB_TXN_SNAPSHOT) | DB_TXN_READ_ONLY;
        LOCK_REASON(lockReason, "query");
        Client::ReadContext ctx(ns, lockReason);
        scoped_ptr<Client::Transaction> transaction(!inMultiStatementTxn ?
                                                    new Client::Transaction(txnFlags) : NULL);

        bool hasRetried = false;
        while ( 1 ) {
            try {
                replVerifyReadsOk(&pq);

                // Fast-path for primary key queries.
                if (!explain && !tailable) {
                    replVerifyReadsOk(&pq);
                    if (_tryQueryByPKHack(ns, query, pq, curop, result)) {
                        if (transaction) {
                            transaction->commit();
                        }
                        return "";
                    }
                }

                // sanity check the query and projection
                if (pq.getFields() != NULL) {
                    pq.getFields()->validateQuery( query );
                }

                        
                if (tailable) {
                    Collection *cl = getCollection( ns );
                    if (cl != NULL && !(cl->isCapped() || str::equals(ns, rsoplog))) {
                        uasserted( 13051, "tailable cursor requested on non-capped, non-oplog collection" );
                    }
                    const BSONObj nat1 = BSON( "$natural" << 1 );
                    if ( order.isEmpty() ) {
                        order = nat1;
                    } else {
                        uassert( 13052, "only {$natural:1} order allowed for tailable cursor", order == nat1 );
                    }
                }
                    
                // Run a regular query.

                // these now may stored in a ClientCursor or somewhere else,
                // so make sure we use a real copy
                jsobj = jsobj.getOwned();
                query = query.getOwned();
                order = order.getOwned();
                const ConfigVersion shardingVersionAtStart = shardingState.getVersion( ns );
                const bool getCachedExplainPlan = ! hasRetried && explain && ! pq.hasIndexSpecifier();
                const bool savedCursor = queryWithQueryOptimizer( queryOptions, ns, jsobj, curop, query,
                                                                  order, pq_shared, shardingVersionAtStart,
                                                                  getCachedExplainPlan, inMultiStatementTxn,
                                                                  result );
                // Did not save the cursor, so we can commit the transaction now if it exists.
                if (transaction && !savedCursor) {
                    transaction->commit();
                }
                return curop.debug().exhaust ? ns : "";
            }
            catch ( const QueryRetryException & ) {
                // In some cases the query may be retried if there is an in memory sort size assertion.
                verify( ! hasRetried );
                hasRetried = true;
            }
        }
    }
} // namespace mongo
