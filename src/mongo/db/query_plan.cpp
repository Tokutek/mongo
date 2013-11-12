/**
 *    Copyright (C) 2008 10gen Inc.
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

#include "mongo/db/query_plan.h"

#include "mongo/db/cmdline.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/parsed_query.h"
#include "mongo/db/query_plan_summary.h"
#include "mongo/db/queryutil.h"
#include "mongo/server.h"

namespace mongo {

    QueryPlanSummary QueryPlan::summary() const {
        QueryPlanSummary summary;
        summary.fieldRangeSetMulti.reset( new FieldRangeSet( multikeyFrs() ) );
        summary.keyFieldsOnly = keyFieldsOnly();
        summary.scanAndOrderRequired = scanAndOrderRequired();
        return summary;
    }

    double elementDirection( const BSONElement& e ) {
        if ( e.isNumber() )
            return e.number();
        return 1;
    }

    QueryPlan* QueryPlan::make( NamespaceDetails* d,
                                int idxNo,
                                const FieldRangeSetPair& frsp,
                                const FieldRangeSetPair* originalFrsp,
                                const BSONObj& originalQuery,
                                const BSONObj& order,
                                const shared_ptr<const ParsedQuery>& parsedQuery,
                                const BSONObj& startKey,
                                const BSONObj& endKey,
                                const std::string& special ) {
        auto_ptr<QueryPlan> ret( new QueryPlan( d,
                                                idxNo,
                                                frsp,
                                                originalQuery,
                                                order,
                                                parsedQuery,
                                                special ) );
        ret->init( originalFrsp, startKey, endKey );
        return ret.release();
    }
    
    QueryPlan::QueryPlan( NamespaceDetails* d,
                          int idxNo,
                          const FieldRangeSetPair& frsp,
                          const BSONObj& originalQuery,
                          const BSONObj& order,
                          const shared_ptr<const ParsedQuery>& parsedQuery,
                          const std::string& special ) :
        _d( d ),
        _idxNo( idxNo ),
        _frs( frsp.frsForIndex( _d, _idxNo ) ),
        _frsMulti( frsp.frsForIndex( _d, -1 ) ),
        _originalQuery( originalQuery ),
        _order( order ),
        _parsedQuery( parsedQuery ),
        _index( 0 ),
        _scanAndOrderRequired( true ),
        _matcherNecessary( true ),
        _direction( 0 ),
        _endKeyInclusive(),
        _utility( Helpful ),
        _special( special ),
        _startOrEndSpec() {
    }
    
    void QueryPlan::init( const FieldRangeSetPair* originalFrsp,
                          const BSONObj& startKey,
                          const BSONObj& endKey ) {
        _endKeyInclusive = endKey.isEmpty();
        _startOrEndSpec = !startKey.isEmpty() || !endKey.isEmpty();
        
        BSONObj idxKey = _idxNo < 0 ? BSONObj() : _d->idx( _idxNo ).keyPattern();

        if ( !_frs.matchPossibleForIndex( idxKey ) ) {
            _utility = Impossible;
            _scanAndOrderRequired = false;
            return;
        }
            
        if ( willScanTable() ) {
            if ( _order.isEmpty() || !strcmp( _order.firstElementFieldName(), "$natural" ) )
                _scanAndOrderRequired = false;
            return;
        }

        _index = &_d->idx(_idxNo);

        // If the parsing or index indicates this is a special query, don't continue the processing
        if (!_special.empty() ||
            ( _index->special() &&
             _index->suitability( _frs, _order ) != IndexDetails::USELESS ) ) {

            if (_special.empty()) {
                const string &special = _index->getSpecialIndexName();
                massert( 13040 , str::stream() << "no type for special: " + _special,
                                !special.empty() );
                _special = special;
            }

            // hopefully safe to use original query in these contexts;
            // don't think we can mix special with $or clause separation yet
            _scanAndOrderRequired = !_order.isEmpty();
            return;
        }

        BSONObjIterator o( _order );
        BSONObjIterator k( idxKey );
        if ( !o.moreWithEOO() )
            _scanAndOrderRequired = false;
        while( o.moreWithEOO() ) {
            BSONElement oe = o.next();
            if ( oe.eoo() ) {
                _scanAndOrderRequired = false;
                break;
            }
            if ( !k.moreWithEOO() )
                break;
            BSONElement ke;
            while( 1 ) {
                ke = k.next();
                if ( ke.eoo() )
                    goto doneCheckOrder;
                if ( strcmp( oe.fieldName(), ke.fieldName() ) == 0 )
                    break;
                if ( !_frs.range( ke.fieldName() ).equality() )
                    goto doneCheckOrder;
            }
            int d = elementDirection( oe ) == elementDirection( ke ) ? 1 : -1;
            if ( _direction == 0 )
                _direction = d;
            else if ( _direction != d )
                break;
        }
doneCheckOrder:
        if ( _scanAndOrderRequired )
            _direction = 0;
        BSONObjIterator i( idxKey );
        int exactIndexedQueryCount = 0;
        int optimalIndexedQueryCount = 0;
        bool awaitingLastOptimalField = true;
        set<string> orderFieldsUnindexed;
        _order.getFieldNames( orderFieldsUnindexed );
        while( i.moreWithEOO() ) {
            BSONElement e = i.next();
            if ( e.eoo() )
                break;
            const FieldRange& fr = _frs.range( e.fieldName() );
            if ( awaitingLastOptimalField ) {
                if ( !fr.universal() )
                    ++optimalIndexedQueryCount;
                if ( !fr.equality() )
                    awaitingLastOptimalField = false;
            }
            else {
                if ( !fr.universal() )
                    optimalIndexedQueryCount = -1;
            }
            if ( fr.equality() ) {
                BSONElement e = fr.max();
                if ( !e.isNumber() && !e.mayEncapsulate() && e.type() != RegEx )
                    ++exactIndexedQueryCount;
            }
            orderFieldsUnindexed.erase( e.fieldName() );
        }
        if ( !_scanAndOrderRequired &&
                ( optimalIndexedQueryCount == _frs.numNonUniversalRanges() ) )
            _utility = Optimal;
        _frv.reset( new FieldRangeVector( _frs, idxKey, _direction ) );

        if ( // If all field range constraints are on indexed fields and ...
             _utility == Optimal &&
             // ... the field ranges exactly represent the query and ...
             _frs.mustBeExactMatchRepresentation() &&
             // ... all indexed ranges are represented in the field range vector ...
             _frv->hasAllIndexedRanges() ) {

            // ... then the field range vector is sufficient to perform query matching against index
            // keys.  No matcher is required.
            _matcherNecessary = false;
        }

        if ( originalFrsp ) {
            _originalFrv.reset( new FieldRangeVector( originalFrsp->frsForIndex( _d, _idxNo ),
                                                      idxKey,
                                                      _direction ) );
        }
        else {
            _originalFrv = _frv;
        }
        if ( _startOrEndSpec ) {
            BSONObj newStart, newEnd;
            if ( !startKey.isEmpty() )
                _startKey = startKey;
            else
                _startKey = _frv->startKey();
            if ( !endKey.isEmpty() )
                _endKey = endKey;
            else
                _endKey = _frv->endKey();
        }

        if ( ( _scanAndOrderRequired || _order.isEmpty() ) && 
            _frs.range( idxKey.firstElementFieldName() ).universal() ) { // NOTE SERVER-2140
            _utility = Unhelpful;
        }
            
        if ( _index->sparse() && hasPossibleExistsFalsePredicate() ) {
            _utility = Disallowed;
        }

        if ( _parsedQuery && _parsedQuery->getFields() && !_d->isMultikey( _idxNo ) ) {
            // Does not check modifiedKeys()
            _keyFieldsOnly.reset( _parsedQuery->getFields()->checkKey( _index->keyPattern(), _d->pkPattern() ) );
        }
    }

    shared_ptr<Cursor> QueryPlan::newCursor(const bool requestCountingCursor) const {

        if ( _index != NULL && _index->special() ) {
            // hopefully safe to use original query in these contexts - don't think we can mix type
            // with $or clause separation yet
            int numWanted = 0;
            if ( _parsedQuery ) {
                // SERVER-5390
                numWanted = _parsedQuery->getSkip() + _parsedQuery->getNumToReturn();
            }
            return _index->newCursor( _originalQuery, _order, numWanted );
        }

        if ( _utility == Impossible ) {
            // Dummy table scan cursor returning no results.  Allowed in --notablescan mode.
            return shared_ptr<Cursor>( new DummyCursor() );
        }

        if ( willScanTable() ) {
            checkTableScanAllowed();
            const int direction = _order.getField("$natural").number() >= 0 ? 1 : -1;
            NamespaceDetails *d = nsdetails( _frs.ns() );
            return shared_ptr<Cursor>( BasicCursor::make( d, direction ) );
        }

        if ( _startOrEndSpec ) {
            // we are sure to spec _endKeyInclusive
            return shared_ptr<Cursor>( IndexCursor::make( _d,
                                                          *_index,
                                                          _startKey,
                                                          _endKey,
                                                          _endKeyInclusive,
                                                          _direction >= 0 ? 1 : -1 ) );
        }

        // A CountingIndexCursor is returned if explicitly requested AND _frv is exactly
        // represented by a single interval within the index. Because CountingIndexCursors
        // cannot provide meaningful results for currPK/currKey/current(), we must not
        // use them for multikey indexes where manual deduplication is required.
        if (requestCountingCursor && _utility == Optimal && _frv->isSingleInterval() && !isMultiKey()) {
            return shared_ptr<Cursor>( new IndexCountCursor( _d, *_index, _frv ) );
        }

        if ( _index->special() ) {
            return shared_ptr<Cursor>( IndexCursor::make( _d,
                                                          *_index,
                                                          _frv->startKey(),
                                                          _frv->endKey(),
                                                          true,
                                                          _direction >= 0 ? 1 : -1 ) );
        }

        return shared_ptr<Cursor>( IndexCursor::make( _d,
                                                      *_index,
                                                      _frv,
                                                      independentRangesSingleIntervalLimit(),
                                                      _direction >= 0 ? 1 : -1 ) );
    }

    BSONObj QueryPlan::indexKey() const {
        if ( !_index )
            return BSON( "$natural" << 1 );
        return _index->keyPattern();
    }

    const char* QueryPlan::ns() const {
        return _frs.ns();
    }

    void QueryPlan::registerSelf( long long nScanned,
                                  CandidatePlanCharacter candidatePlans ) const {
        // Impossible query constraints can be detected before scanning and historically could not
        // generate a QueryPattern.
        if ( _utility == Impossible ) {
            return;
        }

        NamespaceDetails *d = nsdetails(ns());
        if (d != NULL) {
            NamespaceDetails::QueryCacheRWLock::Exclusive lk(d);
            QueryPattern queryPattern = _frs.pattern( _order );
            CachedQueryPlan queryPlanToCache( indexKey(), nScanned, candidatePlans );
            d->registerCachedQueryPlanForPattern( queryPattern, queryPlanToCache );
        }
    }
    
    void QueryPlan::checkTableScanAllowed() const {
        if ( likely( !cmdLine.noTableScan ) )
            return;

        // TODO - is this desirable?  See SERVER-2222.
        if ( _frs.numNonUniversalRanges() == 0 )
            return;

        if ( strstr( ns(), ".system." ) )
            return;

        if( str::startsWith( ns(), "local." ) )
            return;

        if ( !nsdetails( ns() ) )
            return;

        uassert( 10111, (string)"table scans not allowed:" + ns(), !cmdLine.noTableScan );
    }

    int QueryPlan::independentRangesSingleIntervalLimit() const {
        if ( _scanAndOrderRequired &&
             _parsedQuery &&
             !_parsedQuery->wantMore() &&
             !isMultiKey() &&
             queryBoundsExactOrderSuffix() ) {
            verify( _direction == 0 );
            // Limit the results for each compound interval. SERVER-5063
            return _parsedQuery->getSkip() + _parsedQuery->getNumToReturn();
        }
        return 0;
    }

    bool QueryPlan::hasPossibleExistsFalsePredicate() const {
        return matcher()->docMatcher().hasExistsFalse();
    }
    
    bool QueryPlan::queryBoundsExactOrderSuffix() const {
        if ( !indexed() ||
             !_frs.matchPossible() ||
             !_frs.mustBeExactMatchRepresentation() ) {
            return false;
        }
        BSONObj idxKey = indexKey();
        BSONObjIterator index( idxKey );
        BSONObjIterator order( _order );
        int coveredNonUniversalRanges = 0;
        while( index.more() ) {
            const FieldRange& indexFieldRange = _frs.range( (*index).fieldName() );
            if ( !indexFieldRange.isPointIntervalSet() ) {
                if ( !indexFieldRange.universal() ) {
                    // The last indexed range may be a non point set containing a single interval.
                    // SERVER-5777
                    if ( indexFieldRange.intervals().size() > 1 ) {
                        return false;
                    }
                    ++coveredNonUniversalRanges;
                }
                break;
            }
            ++coveredNonUniversalRanges;
            if ( order.more() && str::equals( (*index).fieldName(), (*order).fieldName() ) ) {
                ++order;
            }
            ++index;
        }
        if ( coveredNonUniversalRanges != _frs.numNonUniversalRanges() ) {
            return false;
        }
        while( index.more() && order.more() ) {
            if ( !str::equals( (*index).fieldName(), (*order).fieldName() ) ) {
                return false;
            }
            if ( ( elementDirection( *index ) < 0 ) != ( elementDirection( *order ) < 0 ) ) {
                return false;
            }
            ++order;
            ++index;
        }
        return !order.more();
    }

    string QueryPlan::toString() const {
        return BSON(
                    "index" << indexKey() <<
                    "frv" << ( _frv ? _frv->toString() : "" ) <<
                    "order" << _order
                    ).jsonString();
    }
    
    shared_ptr<CoveredIndexMatcher> QueryPlan::matcher() const {
        if ( !_matcher ) {
            _matcher.reset( new CoveredIndexMatcher( originalQuery(), indexKey() ) );
        }
        return _matcher;
    }
    
    bool QueryPlan::isMultiKey() const {
        if ( _idxNo < 0 )
            return false;
        return _d->isMultikey( _idxNo );
    }

    std::ostream& operator<< ( std::ostream& out, const QueryPlan::Utility& utility ) {
        out << "QueryPlan::";
        switch( utility ) {
            case QueryPlan::Impossible: return out << "Impossible";
            case QueryPlan::Optimal:    return out << "Optimal";
            case QueryPlan::Helpful:    return out << "Helpful";
            case QueryPlan::Unhelpful:  return out << "Unhelpful";
            case QueryPlan::Disallowed: return out << "Disallowed";
            default:
                return out << "UNKNOWN(" << utility << ")";
        }
    }
    
} // namespace mongo
