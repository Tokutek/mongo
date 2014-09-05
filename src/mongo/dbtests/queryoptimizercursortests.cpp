// queryoptimizertests.cpp : query optimizer unit tests
//

/**
 *    Copyright (C) 2009 10gen Inc.
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

#include "mongo/client/dbclientcursor.h"
#include "mongo/db/queryoptimizercursorimpl.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/db/query_optimizer_internal.h"
#include "mongo/db/instance.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/json.h"
#include "mongo/db/queryutil.h"
#include "mongo/dbtests/dbtests.h"

namespace mongo {
    shared_ptr<Cursor> newQueryOptimizerCursor( const char *ns, const BSONObj &query,
                                               const BSONObj &order = BSONObj(),
                                               const QueryPlanSelectionPolicy &planPolicy =
                                               QueryPlanSelectionPolicy::any(),
                                               bool requireOrder = true,
                                               const shared_ptr<const ParsedQuery> &parsedQuery =
                                               shared_ptr<const ParsedQuery>() );
} // namespace mongo

namespace QueryOptimizerCursorTests {

    using boost::shared_ptr;
    
    void dropCollection( const char *ns ) {
     	string errmsg;
        BSONObjBuilder result;
        Collection *d = getCollection(ns);
        if (d != NULL) {
            d->drop(errmsg, result);
        }
    }

    void ensureIndex(const char *ns, BSONObj keyPattern, bool unique, const char *name) {
        Collection *d = getCollection(ns);
        if( d == 0 )
            return;

        {
            for (int i = 0; i < d->nIndexes(); i++) {
                IndexDetails &ii = d->idx(i);
                if( ii.keyPattern().woCompare(keyPattern) == 0 )
                    return;
            }
        }

        string system_indexes = cc().database()->name() + ".system.indexes";

        BSONObjBuilder b;
        b.append("name", name);
        b.append("ns", ns);
        b.append("key", keyPattern);
        b.appendBool("unique", unique);
        BSONObj o = b.done();

        insertObject(system_indexes.c_str(), o, 0, true);
    }
        
    namespace CachedMatchCounter {
        
        using mongo::CachedMatchCounter;
        
        class Count {
        public:
            void run() {
                long long aggregateNscanned;
                CachedMatchCounter c( aggregateNscanned, 0 );
                ASSERT_EQUALS( 0, c.count() );
                ASSERT_EQUALS( 0, c.cumulativeCount() );

                c.resetMatch();
                ASSERT( !c.knowMatch() );

                c.setMatch( false );
                ASSERT( c.knowMatch() );

                c.incMatch( BSONObj() );
                ASSERT_EQUALS( 0, c.count() );
                ASSERT_EQUALS( 0, c.cumulativeCount() );
                
                c.resetMatch();
                ASSERT( !c.knowMatch() );
                
                c.setMatch( true );
                ASSERT( c.knowMatch() );
                
                c.incMatch( BSONObj() );
                ASSERT_EQUALS( 1, c.count() );
                ASSERT_EQUALS( 1, c.cumulativeCount() );

                // Don't count the same match twice, without checking the document location.
                c.incMatch( BSON( "a" << 1 ) );
                ASSERT_EQUALS( 1, c.count() );
                ASSERT_EQUALS( 1, c.cumulativeCount() );

                // Reset and count another match.
                c.resetMatch();
                c.setMatch( true );
                c.incMatch( BSON( "a" << 1 ) );
                ASSERT_EQUALS( 2, c.count() );
                ASSERT_EQUALS( 2, c.cumulativeCount() );
            }
        };
        
        class Accumulate {
        public:
            void run() {
                long long aggregateNscanned;
                CachedMatchCounter c( aggregateNscanned, 10 );
                ASSERT_EQUALS( 0, c.count() );
                ASSERT_EQUALS( 10, c.cumulativeCount() );
                
                c.setMatch( true );
                c.incMatch( BSONObj() );
                ASSERT_EQUALS( 1, c.count() );
                ASSERT_EQUALS( 11, c.cumulativeCount() );
            }
        };
        
        class Dedup {
        public:
            void run() {
                long long aggregateNscanned;
                CachedMatchCounter c( aggregateNscanned, 0 );

                c.setCheckDups( true );
                c.setMatch( true );
                c.incMatch( BSONObj() );
                ASSERT_EQUALS( 1, c.count() );

                c.resetMatch();
                c.setMatch( true );
                c.incMatch( BSONObj() );
                ASSERT_EQUALS( 1, c.count() );
            }
        };

        class Nscanned {
        public:
            void run() {
                long long aggregateNscanned = 5;
                CachedMatchCounter c( aggregateNscanned, 0 );
                ASSERT_EQUALS( 0, c.nscanned() );
                ASSERT_EQUALS( 5, c.aggregateNscanned() );

                c.updateNscanned( 4 );
                ASSERT_EQUALS( 4, c.nscanned() );
                ASSERT_EQUALS( 9, c.aggregateNscanned() );
            }
        };
        
    } // namespace CachedMatchCounter
        
    namespace SmallDupSet {
        
        using mongo::SmallDupSet;

        class Upgrade {
        public:
            void run() {
                SmallDupSet d;
                for( int i = 0; i < 100; ++i ) {
                    ASSERT( !d.getsetdup( BSON( "a" << i ) ) );
                    for( int j = 0; j <= i; ++j ) {
                        ASSERT( d.getdup( BSON( "a" << j ) ) );
                    }
                }
            }
        };
        
        class UpgradeRead {
        public:
            void run() {
                SmallDupSet d;
                d.getsetdup( BSON( "a" << 0 ) );
                for( int i = 0; i < 550; ++i ) {
                    ASSERT( d.getdup( BSON( "a" << 0 ) ) );
                }
                ASSERT( d.getsetdup( BSON( "a" << 0 ) ) );
            }
        };
        
        class UpgradeWrite {
        public:
            void run() {
                SmallDupSet d;
                for( int i = 0; i < 550; ++i ) {
                    ASSERT( !d.getsetdup( BSON( "a" << i ) ) );
                }
                for( int i = 0; i < 550; ++i ) {
                    ASSERT( d.getsetdup( BSON( "a" << i ) ) );
                }
            }
        };

    } // namespace SmallDupSet
    
    class DurationTimerStop {
    public:
        void run() {
            DurationTimer t;
            while( t.duration() == 0 );
            ASSERT( t.duration() > 0 );
            t.stop();
            ASSERT( t.duration() > 0 );
            ASSERT( t.duration() > 0 );
        }
    };

    class Base {
    public:
        Base() {
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            string err;
            userCreateNS( ns(), BSONObj(), err, false );
            dropCollection( ns() );
            transaction.commit();
        }
        ~Base() {
            cc().curop()->reset();
        }
    protected:
        DBDirectClient _cli;
        static const char *ns() { return "unittests.QueryOptimizerTests"; }
        void setQueryOptimizerCursor( const BSONObj &query, const BSONObj &order = BSONObj() ) {
            setQueryOptimizerCursorWithoutAdvancing( query, order );
            if ( ok() && !mayReturnCurrent() ) {
                advance();
            }
        }
        void setQueryOptimizerCursorWithoutAdvancing( const BSONObj &query, const BSONObj &order = BSONObj() ) {
            _c = newQueryOptimizerCursor( ns(), query, order );
        }
        bool ok() const { return _c->ok(); }
        /** Handles matching and deduping. */
        bool advance() {
            while( _c->advance() && !mayReturnCurrent() );
            return ok();
        }
        int itcount() {
            int ret = 0;
            while( ok() ) {
                ++ret;
                advance();
            }
            return ret;
        }
        BSONObj current() const { return _c->current(); }
        BSONObj currPK() const { return _c->currPK(); }
        bool mayReturnCurrent() {
            return ( !_c->matcher() || _c->matcher()->matchesCurrent( _c.get() ) ) && !_c->getsetdup( _c->currPK() );
        }
        shared_ptr<Cursor> c() { return _c; }
        long long nscanned() const { return _c->nscanned(); }
        unsigned nNsCursors() const {
            set<CursorId> nsCursors;
            ClientCursor::find( ns(), nsCursors );
            return nsCursors.size();
        }
        BSONObj cachedIndexForQuery( const BSONObj &query, const BSONObj &order = BSONObj() ) {
            QueryPattern queryPattern = FieldRangeSet( ns(), query, true, true ).pattern( order );
            return getCollection(ns())->getQueryCache().cachedQueryPlanForPattern( queryPattern ).indexKey();
        }
    private:
        shared_ptr<Cursor> _c;
    };
    
    /** No results for empty collection. */
    class Empty : public Base {
    public:
        void run() {
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            shared_ptr<QueryOptimizerCursor> c =
            dynamic_pointer_cast<QueryOptimizerCursor>
            ( newQueryOptimizerCursor( ns(), BSONObj() ) );
            ASSERT( !c->ok() );
            ASSERT_THROWS( c->current(), AssertionException );
            ASSERT( c->currPK().isEmpty() );
            ASSERT( !c->advance() );
            ASSERT_THROWS( c->currKey(), AssertionException );
            ASSERT_THROWS( c->getsetdup( BSONObj() ), AssertionException );
            ASSERT_THROWS( c->isMultiKey(), AssertionException );
            ASSERT_THROWS( c->matcher(), AssertionException );
            
            ASSERT_THROWS( c->initialFieldRangeSet(), AssertionException );
            ASSERT_THROWS( c->currentPlanScanAndOrderRequired(), AssertionException );
            ASSERT_THROWS( c->keyFieldsOnly(), AssertionException );
            ASSERT_THROWS( c->runningInitialInOrderPlan(), AssertionException );
            ASSERT_THROWS( c->hasPossiblyExcludedPlans(), AssertionException );

            // ok
            c->initialCandidatePlans();
            c->completePlanOfHybridSetScanAndOrderRequired();
            c->clearIndexesForPatterns();
            c->abortOutOfOrderPlans();
            c->noteIterate( false, false, false );
            c->explainQueryInfo();
            transaction.commit();
        }
    };
    
    /** Simple table scan. */
    class Unindexed : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << 1 ) );
            _cli.insert( ns(), BSON( "_id" << 2 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSONObj() );
            ASSERT_EQUALS( 2, itcount() );
            transaction.commit();
        }
    };
    
    /** Basic test with two indexes and deduping requirement. */
    class Basic : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << 1 << "a" << 2 ) );
            _cli.insert( ns(), BSON( "_id" << 2 << "a" << 1 ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "_id" << GT << 0 << "a" << GT << 0 ) );
            ASSERT( ok() );
            ASSERT_EQUALS( BSON( "_id" << 1 << "a" << 2 ), current() );
            ASSERT( advance() );
            ASSERT_EQUALS( BSON( "_id" << 2 << "a" << 1 ), current() );
            ASSERT( !advance() );
            ASSERT( !ok() );
            transaction.commit();
        }
    };
    
    class NoMatch : public Base {
    public:
        void run() {
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "_id" << GT << 5 << LT << 4 << "a" << GT << 0 ) );
            ASSERT( !ok() );
            transaction.commit();
        }            
    };
    
    /** Order of results indicates that interleaving is occurring. */
    class Interleaved : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << 1 << "a" << 2 ) );
            _cli.insert( ns(), BSON( "_id" << 3 << "a" << 1 ) );
            _cli.insert( ns(), BSON( "_id" << 2 << "a" << 2 ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );

            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "_id" << GT << 0 << "a" << GT << 0 ) );
            ASSERT( ok() );
            ASSERT_EQUALS( BSON( "_id" << 1 << "a" << 2 ), current() );
            ASSERT( advance() );
            ASSERT_EQUALS( BSON( "_id" << 3 << "a" << 1 ), current() );
            ASSERT( advance() );
            ASSERT_EQUALS( BSON( "_id" << 2 << "a" << 2 ), current() );
            ASSERT( !advance() );
            ASSERT( !ok() );
            transaction.commit();
        }
    };
    
    /** Some values on each index do not match. */
    class NotMatch : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << 0 << "a" << 10 ) );
            _cli.insert( ns(), BSON( "_id" << 10 << "a" << 0 ) );
            _cli.insert( ns(), BSON( "_id" << 11 << "a" << 12 ) );
            _cli.insert( ns(), BSON( "_id" << 12 << "a" << 11 ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );

            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "_id" << GT << 5 << "a" << GT << 5 ) );
            ASSERT( ok() );
            ASSERT_EQUALS( BSON( "_id" << 11 << "a" << 12 ), current() );
            ASSERT( advance() );
            ASSERT_EQUALS( BSON( "_id" << 12 << "a" << 11 ), current() );
            ASSERT( !advance() );
            ASSERT( !ok() );
            transaction.commit();
        }            
    };
    
    /** After the first 101 matches for a plan, we stop interleaving the plans. */
    class StopInterleaving : public Base {
    public:
        void run() {
            for( int i = 0; i < 101; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i << "a" << i ) );   
            }
            for( int i = 101; i < 200; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i << "a" << (301-i) ) );   
            }
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );

            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );

            setQueryOptimizerCursor( BSON( "_id" << GT << -1 << "a" << GT << -1 ) );
            for( int i = 0; i < 200; ++i ) {
                ASSERT( ok() );
                ASSERT_EQUALS( i, current().getIntField( "_id" ) );
                advance();
            }
            ASSERT( !advance() );
            ASSERT( !ok() );                
        }
    };
    
    /** Test correct deduping with the takeover cursor. */
    class TakeoverWithDup : public Base {
    public:
        void run() {
            for( int i = 0; i < 101; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i << "a" << i ) );   
            }
            _cli.insert( ns(), BSON( "_id" << 500 << "a" << BSON_ARRAY( 0 << 300 ) ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "_id" << GT << -1 << "a" << GT << -1 ) );
            ASSERT_EQUALS( 102, itcount() );
            transaction.commit();
        }
    };
    
    /** Test usage of matcher with takeover cursor. */
    class TakeoverWithNonMatches : public Base {
    public:
        void run() {
            for( int i = 0; i < 101; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i << "a" << i ) );   
            }
            _cli.insert( ns(), BSON( "_id" << 101 << "a" << 600 ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "_id" << GT << -1 << "a" << LT << 500 ) );
            ASSERT_EQUALS( 101, itcount() );
            transaction.commit();
        }
    };
    
    /** Check deduping of dups within just the takeover cursor. */
    class TakeoverWithTakeoverDup : public Base {
    public:
        void run() {
            for( int i = 0; i < 101; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i*2 << "a" << 0 ) );
                _cli.insert( ns(), BSON( "_id" << i*2+1 << "a" << 1 ) );
            }
            _cli.insert( ns(), BSON( "_id" << 202 << "a" << BSON_ARRAY( 2 << 3 ) ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "_id" << GT << -1 << "a" << GT << 0) );
            ASSERT_EQUALS( 102, itcount() );
            transaction.commit();
        }
    };
    
    /** Basic test with $or query. */
    class BasicOr : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << 0 << "a" << 0 ) );
            _cli.insert( ns(), BSON( "_id" << 1 << "a" << 1 ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "$or" << BSON_ARRAY( BSON( "_id" << 0 ) << BSON( "a" << 1 ) ) ) );
            ASSERT_EQUALS( BSON( "_id" << 0 << "a" << 0 ), current() );
            ASSERT( advance() );
            ASSERT_EQUALS( BSON( "_id" << 1 << "a" << 1 ), current() );
            ASSERT( !advance() );
            transaction.commit();
        }
    };
    
    /** $or first clause empty. */
    class OrFirstClauseEmpty : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << 0 << "a" << 1 ) );
            _cli.insert( ns(), BSON( "_id" << 1 << "a" << 1 ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "$or" << BSON_ARRAY( BSON( "_id" << -1 ) << BSON( "a" << 1 ) ) ) );
            ASSERT_EQUALS( BSON( "_id" << 0 << "a" << 1 ), current() );
            ASSERT( advance() );
            ASSERT_EQUALS( BSON( "_id" << 1 << "a" << 1 ), current() );
            ASSERT( !advance() );
            transaction.commit();
        }
    };        
    
    /** $or second clause empty. */
    class OrSecondClauseEmpty : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << 0 << "a" << 1 ) );
            _cli.insert( ns(), BSON( "_id" << 1 << "a" << 1 ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "$or" << BSON_ARRAY( BSON( "_id" << 0 ) << BSON( "_id" << -1 ) << BSON( "a" << 1 ) ) ) );
            ASSERT_EQUALS( BSON( "_id" << 0 << "a" << 1 ), current() );
            ASSERT( advance() );
            ASSERT_EQUALS( BSON( "_id" << 1 << "a" << 1 ), current() );
            ASSERT( !advance() );
            transaction.commit();
        }
    };
    
    /** $or multiple clauses empty empty. */
    class OrMultipleClausesEmpty : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << 0 << "a" << 1 ) );
            _cli.insert( ns(), BSON( "_id" << 1 << "a" << 1 ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "$or" << BSON_ARRAY( BSON( "_id" << 2 ) << BSON( "_id" << 4 ) << BSON( "_id" << 0 ) << BSON( "_id" << -1 ) << BSON( "_id" << 6 ) << BSON( "a" << 1 ) << BSON( "_id" << 9 ) ) ) );
            ASSERT_EQUALS( BSON( "_id" << 0 << "a" << 1 ), current() );
            ASSERT( advance() );
            ASSERT_EQUALS( BSON( "_id" << 1 << "a" << 1 ), current() );
            ASSERT( !advance() );
            transaction.commit();
        }
    };
    
    /** Check that takeover occurs at proper match count with $or clauses */
    class TakeoverCountOr : public Base {
    public:
        void run() {
            for( int i = 0; i < 60; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i << "a" << 0 ) );   
            }
            for( int i = 60; i < 120; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i << "a" << 1 ) );
            }
            for( int i = 120; i < 150; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i << "a" << (200-i) ) );
            }
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "$or" << BSON_ARRAY( BSON( "a" << 0 ) << BSON( "a" << 1 ) << BSON( "_id" << GTE << 120 << "a" << GT << 1 ) ) ) );
            for( int i = 0; i < 120; ++i ) {
                ASSERT( ok() );
                advance();
            }
            // Expect to be scanning on _id index only.
            for( int i = 120; i < 150; ++i ) {
                ASSERT_EQUALS( i, current().getIntField( "_id" ) );
                advance();
            }
            ASSERT( !ok() );
            transaction.commit();
        }
    };
    
    /** Takeover just at end of clause. */
    class TakeoverEndOfOrClause : public Base {
    public:
        void run() {
            for( int i = 0; i < 102; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i ) );   
            }
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "$or" << BSON_ARRAY( BSON( "_id" << LT << 101 ) << BSON( "_id" << 101 ) ) ) );
            for( int i = 0; i < 102; ++i ) {
                ASSERT_EQUALS( i, current().getIntField( "_id" ) );
                advance();
            }
            ASSERT( !ok() );
            transaction.commit();
        }
    };
    
    class TakeoverBeforeEndOfOrClause : public Base {
    public:
        void run() {
            for( int i = 0; i < 101; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i ) );   
            }
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "$or" << BSON_ARRAY( BSON( "_id" << LT << 100 ) << BSON( "_id" << 100 ) ) ) );
            for( int i = 0; i < 101; ++i ) {
                ASSERT_EQUALS( i, current().getIntField( "_id" ) );
                advance();
            }
            ASSERT( !ok() );
            transaction.commit();
        }
    };
    
    class TakeoverAfterEndOfOrClause : public Base {
    public:
        void run() {
            for( int i = 0; i < 103; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i ) );   
            }
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "$or" << BSON_ARRAY( BSON( "_id" << LT << 102 ) << BSON( "_id" << 102 ) ) ) );
            for( int i = 0; i < 103; ++i ) {
                ASSERT_EQUALS( i, current().getIntField( "_id" ) );
                advance();
            }
            ASSERT( !ok() );
            transaction.commit();
        }
    };
    
    /** Test matching and deduping done manually by cursor client. */
    class ManualMatchingDeduping : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << 0 << "a" << 10 ) );
            _cli.insert( ns(), BSON( "_id" << 10 << "a" << 0 ) ); 
            _cli.insert( ns(), BSON( "_id" << 11 << "a" << 12 ) );
            _cli.insert( ns(), BSON( "_id" << 12 << "a" << 11 ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            shared_ptr< Cursor > c = newQueryOptimizerCursor( ns(), BSON( "_id" << GT << 5 << "a" << GT << 5 ) );
            ASSERT( c->ok() );
            
            // _id 10 {_id:1}
            ASSERT_EQUALS( 10, c->current().getIntField( "_id" ) );
            ASSERT( !c->matcher()->matchesCurrent( c.get() ) );
            ASSERT( c->advance() );
            
            // _id 0 {a:1}
            ASSERT_EQUALS( 0, c->current().getIntField( "_id" ) );
            ASSERT( !c->matcher()->matchesCurrent( c.get() ) );
            ASSERT( c->advance() );
            
            // _id 11 {_id:1}
            ASSERT_EQUALS( BSON( "_id" << 11 << "a" << 12 ), c->current() );
            ASSERT( c->matcher()->matchesCurrent( c.get() ) );
            ASSERT( !c->getsetdup( c->currPK() ) );
            ASSERT( c->advance() );
            
            // _id 12 {a:1}
            ASSERT_EQUALS( BSON( "_id" << 12 << "a" << 11 ), c->current() );
            ASSERT( c->matcher()->matchesCurrent( c.get() ) );
            ASSERT( !c->getsetdup( c->currPK() ) );
            ASSERT( c->advance() );
            
            // _id 12 {_id:1}
            ASSERT_EQUALS( BSON( "_id" << 12 << "a" << 11 ), c->current() );
            ASSERT( c->matcher()->matchesCurrent( c.get() ) );
            ASSERT( c->getsetdup( c->currPK() ) );
            ASSERT( c->advance() );
            
            // _id 11 {a:1}
            ASSERT_EQUALS( BSON( "_id" << 11 << "a" << 12 ), c->current() );
            ASSERT( c->matcher()->matchesCurrent( c.get() ) );
            ASSERT( c->getsetdup( c->currPK() ) );
            
            // {_id:1} scan is complete.
            ASSERT( !c->advance() );
            ASSERT( !c->ok() );       
            
            // Scan the results again - this time the winning plan has been
            // recorded.
            c = newQueryOptimizerCursor( ns(), BSON( "_id" << GT << 5 << "a" << GT << 5 ) );
            ASSERT( c->ok() );
            
            // _id 10 {_id:1}
            ASSERT_EQUALS( 10, c->current().getIntField( "_id" ) );
            ASSERT( !c->matcher()->matchesCurrent( c.get() ) );
            ASSERT( c->advance() );
            
            // _id 11 {_id:1}
            ASSERT_EQUALS( BSON( "_id" << 11 << "a" << 12 ), c->current() );
            ASSERT( c->matcher()->matchesCurrent( c.get() ) );
            ASSERT( !c->getsetdup( c->currPK() ) );
            ASSERT( c->advance() );
            
            // _id 12 {_id:1}
            ASSERT_EQUALS( BSON( "_id" << 12 << "a" << 11 ), c->current() );
            ASSERT( c->matcher()->matchesCurrent( c.get() ) );
            ASSERT( !c->getsetdup( c->currPK() ) );
            
            // {_id:1} scan complete
            ASSERT( !c->advance() );
            ASSERT( !c->ok() );
            transaction.commit();
        }
    };
    
    /** Curr key must be correct for currPK for correct matching. */
    class ManualMatchingUsingCurrKey : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << "a" ) );
            _cli.insert( ns(), BSON( "_id" << "b" ) );
            _cli.insert( ns(), BSON( "_id" << "ba" ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            shared_ptr< Cursor > c = newQueryOptimizerCursor( ns(), fromjson( "{_id:/a/}" ) );
            ASSERT( c->ok() );
            // "a"
            ASSERT( c->matcher()->matchesCurrent( c.get() ) );
            ASSERT( !c->getsetdup( c->currPK() ) );
            ASSERT( c->advance() );
            ASSERT( c->ok() );
            
            // "b"
            ASSERT( !c->matcher()->matchesCurrent( c.get() ) );
            ASSERT( c->advance() );
            ASSERT( c->ok() );
            
            // "ba"
            ASSERT( c->matcher()->matchesCurrent( c.get() ) );
            ASSERT( !c->getsetdup( c->currPK() ) );
            ASSERT( !c->advance() );
            transaction.commit();
        }
    };
    
    /** Test matching and deduping done manually by cursor client. */
    class ManualMatchingDedupingTakeover : public Base {
    public:
        void run() {
            for( int i = 0; i < 150; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i << "a" << 0 ) );
            }
            _cli.insert( ns(), BSON( "_id" << 300 << "a" << 1 ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            shared_ptr< Cursor > c = newQueryOptimizerCursor( ns(), BSON( "$or" << BSON_ARRAY( BSON( "_id" << LT << 300 ) << BSON( "a" << 1 ) ) ) );
            for( int i = 0; i < 151; ++i ) {
                ASSERT( c->ok() );
                ASSERT( c->matcher()->matchesCurrent( c.get() ) );
                ASSERT( !c->getsetdup( c->currPK() ) );
                c->advance();
            }
            ASSERT( !c->ok() );
            transaction.commit();
        }
    };
    
    /** Test single key matching bounds. */
    class Singlekey : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "a" << "10" ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            shared_ptr< Cursor > c = newQueryOptimizerCursor( ns(), BSON( "a" << GT << 1 << LT << 5 ) );
            // Two sided bounds work.
            ASSERT( !c->ok() );
            transaction.commit();
        }
    };
    
    /** Test multi key matching bounds. */
    class Multikey : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "a" << BSON_ARRAY( 1 << 10 ) ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "a" << GT << 5 << LT << 3 ) );
            // Multi key bounds work.
            ASSERT( ok() );
            transaction.commit();
        }
    };
    
    /** Add other plans when the recorded one is doing more poorly than expected. */
    class AddOtherPlans : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << 0 << "a" << 0 << "b" << 0 ) );
            _cli.insert( ns(), BSON( "_id" << 1 << "a" << 1 << "b" << 0 ) );
            for( int i = 100; i < 150; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i << "a" << 100 << "b" << i ) );
            }
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            shared_ptr<Cursor> c = newQueryOptimizerCursor( ns(), BSON( "a" << 0 << "b" << 0 ) );
            
            ASSERT_EQUALS( BSON( "_id" << 0 << "a" << 0 << "b" << 0 ), c->current() );
            ASSERT_EQUALS( BSON( "a" << 1 ), c->indexKeyPattern() );

            ASSERT( c->advance() );
            ASSERT_EQUALS( BSON( "_id" << 0 << "a" << 0 << "b" << 0 ), c->current() );
            ASSERT_EQUALS( BSON( "b" << 1 ), c->indexKeyPattern() );
            
            ASSERT( !c->advance() );
            
            c = newQueryOptimizerCursor( ns(), BSON( "a" << 100 << "b" << 149 ) );
            // Try {a:1}, which was successful previously.
            for( int i = 0; i < 12; ++i ) {
                ASSERT( 149 != c->current().getIntField( "b" ) );
                ASSERT( c->advance() );
            }
            bool sawB1Index = false;
            do {
                if ( c->indexKeyPattern() == BSON( "b" << 1 ) ) {
                    ASSERT_EQUALS( 149, c->current().getIntField( "b" ) );
                    // We should try the {b:1} index and only see one result from it.
                    ASSERT( !sawB1Index );
                    sawB1Index = true;
                }
            } while ( c->advance() );
            ASSERT( sawB1Index );
            transaction.commit();
        }
    };

    /**
     * Add other plans when the recorded one is doing more poorly than expected, with deletion before
     * and after adding the additional plans.
     */
    class AddOtherPlansContinuousDelete : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << 0 << "a" << 0 << "b" << 0 ) );
            _cli.insert( ns(), BSON( "_id" << 1 << "a" << 1 << "b" << 0 ) );
            for( int i = 100; i < 400; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i << "a" << i << "b" << ( 499 - i ) ) );
            }
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            shared_ptr<Cursor> c = newQueryOptimizerCursor( ns(), BSON( "a" << GTE << -1 << LTE << 0 << "b" << GTE << -1 << LTE << 0 ) );
            while( c->advance() );
            // {a:1} plan should be recorded now.
          
            c = newQueryOptimizerCursor( ns(), BSON( "a" << GTE << 100 << LTE << 400 << "b" << GTE << 100 << LTE << 400 ) );
            int count = 0;
            while( c->ok() ) {
                if ( c->currentMatches() ) {
                    ASSERT( !c->getsetdup( c->currPK() ) );
                    ++count;
                    int id = c->current().getIntField( "_id" );
                    c->advance();
                    _cli.remove( ns(), BSON( "_id" << id ) );
                } else {
                    c->advance();
                }
            }
            ASSERT_EQUALS( 300, count );
            ASSERT_EQUALS( 2U, _cli.count( ns(), BSONObj() ) );
            transaction.commit();
        }
    };
    
    /** Check $or clause range elimination. */
    class OrRangeElimination : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << 1 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            shared_ptr<Cursor> c = newQueryOptimizerCursor( ns(), BSON( "$or" << BSON_ARRAY( BSON( "_id" << GT << 0 ) << BSON( "_id" << 1 ) ) ) );
            ASSERT( c->ok() );
            ASSERT( !c->advance() );
            transaction.commit();
        }
    };
    
    /** Check $or match deduping - in takeover cursor. */
    class OrDedup : public Base {
    public:
        void run() {
            for( int i = 0; i < 150; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i << "a" << i ) );   
            }
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            shared_ptr<Cursor> c = newQueryOptimizerCursor( ns(), BSON( "$or" << BSON_ARRAY( BSON( "_id" << LT << 140 ) << BSON( "_id" << 145 ) << BSON( "a" << 145 ) ) ) );
            
            while( c->current().getIntField( "_id" ) < 140 ) {
                ASSERT( c->advance() );
            }
            // Match from second $or clause.
            ASSERT_EQUALS( 145, c->current().getIntField( "_id" ) );
            ASSERT( c->matcher()->matchesCurrent( c.get() ) );
            ASSERT( c->advance() );
            // Match from third $or clause.
            ASSERT_EQUALS( 145, c->current().getIntField( "_id" ) );
            // $or deduping is handled by the matcher.
            ASSERT( !c->matcher()->matchesCurrent( c.get() ) );
            ASSERT( !c->advance() );
            transaction.commit();
        }
    };
    
    /** Standard dups with a multikey cursor. */
    class EarlyDups : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "a" << BSON_ARRAY( 0 << 1 << 200 ) ) );
            for( int i = 2; i < 150; ++i ) {
                _cli.insert( ns(), BSON( "a" << i ) );   
            }
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "a" << GT << -1 ) );
            ASSERT_EQUALS( 149, itcount() );
            transaction.commit();
        }
    };
    
    /** Pop or clause in takeover cursor. */
    class OrPopInTakeover : public Base {
    public:
        void run() {
            for( int i = 0; i < 150; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i ) );   
            }
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            shared_ptr<Cursor> c = newQueryOptimizerCursor( ns(), BSON( "$or" << BSON_ARRAY( BSON( "_id" << LTE << 147 ) << BSON( "_id" << 148 ) << BSON( "_id" << 149 ) ) ) );
            for( int i = 0; i < 150; ++i ) {
                ASSERT( c->ok() );
                ASSERT_EQUALS( i, c->current().getIntField( "_id" ) );
                c->advance();
            }
            ASSERT( !c->ok() );
            transaction.commit();
        }
    };
    
    class OrderId : public Base {
    public:
        void run() {
            for( int i = 0; i < 10; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i ) );
            }
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSONObj(), BSON( "_id" << 1 ) );
            
            for( int i = 0; i < 10; ++i, advance() ) {
                ASSERT( ok() );
                ASSERT_EQUALS( i, current().getIntField( "_id" ) );
            }
            transaction.commit();
        }
    };
    
    class OrderMultiIndex : public Base {
    public:
        void run() {
            for( int i = 0; i < 10; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i << "a" << 1 ) );
            }
            _cli.ensureIndex( ns(), BSON( "_id" << 1 << "a" << 1 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "_id" << GTE << 0 << "a" << GTE << 0 ), BSON( "_id" << 1 ) );
            
            for( int i = 0; i < 10; ++i, advance() ) {
                ASSERT( ok() );
                ASSERT_EQUALS( i, current().getIntField( "_id" ) );
            }
            transaction.commit();
        }
    };
    
    class OrderReject : public Base {
    public:
        void run() {
            for( int i = 0; i < 10; ++i ) {
                _cli.insert( ns(), BSON( "_id" << i << "a" << i % 5 ) );
            }
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "a" << GTE << 3 ), BSON( "_id" << 1 ) );
            
            ASSERT( ok() );
            ASSERT_EQUALS( 3, current().getIntField( "_id" ) );
            ASSERT( advance() );
            ASSERT_EQUALS( 4, current().getIntField( "_id" ) );
            ASSERT( advance() );
            ASSERT_EQUALS( 8, current().getIntField( "_id" ) );
            ASSERT( advance() );
            ASSERT_EQUALS( 9, current().getIntField( "_id" ) );
            ASSERT( !advance() );
            transaction.commit();
        }
    };
    
    class OrderNatural : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << 5 ) );
            _cli.insert( ns(), BSON( "_id" << 4 ) );
            _cli.insert( ns(), BSON( "_id" << 6 ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            setQueryOptimizerCursor( BSON( "_id" << GT << 0 ), BSON( "$natural" << 1 ) );
            
            ASSERT( ok() );
            ASSERT_EQUALS( 4, current().getIntField( "_id" ) );
            ASSERT( advance() );                
            ASSERT_EQUALS( 5, current().getIntField( "_id" ) );
            ASSERT( advance() );
            ASSERT_EQUALS( 6, current().getIntField( "_id" ) );
            ASSERT( !advance() );                
            transaction.commit();
        }
    };
    
    class OrderUnindexed : public Base {
    public:
        void run() {
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            ASSERT( !newQueryOptimizerCursor( ns(), BSONObj(), BSON( "a" << 1 ) ).get() );
            transaction.commit();
        }
    };
    
    class RecordedOrderInvalid : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "a" << 1 << "b" << 1 ) );
            _cli.insert( ns(), BSON( "a" << 2 << "b" << 2 ) );
            _cli.insert( ns(), BSON( "a" << 3 << "b" << 3 ) );
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
            // Plan {a:1} will be chosen and recorded.
            ASSERT( _cli.query( ns(), QUERY( "a" << 2 ).sort( "b" ) )->more() );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            shared_ptr<Cursor> c = newQueryOptimizerCursor( ns(), BSON( "a" << 2 ),
                                                           BSON( "b" << 1 ) );
            // Check that we are scanning {b:1} not {a:1}, since {a:1} is not properly ordered.
            for( int i = 0; i < 3; ++i ) {
                ASSERT( c->ok() );  
                c->advance();
            }
            ASSERT( !c->ok() );
            transaction.commit();
        }
    };
    
    class KillOp : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << 1 << "b" << 1 ) );
            _cli.insert( ns(), BSON( "_id" << 2 << "b" << 2 ) );
            _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
            
            Client::Transaction transaction(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);
            Client::ReadContext ctx( ns(), mongo::unittest::EMPTY_STRING );
            setQueryOptimizerCursor( BSON( "_id" << GT << 0 << "b" << GT << 0 ) );
            ASSERT( ok() );
            cc().curop()->kill();
            // First advance() call throws, subsequent calls just fail.
            ASSERT_THROWS( advance(), MsgAssertionException );
            ASSERT( !advance() );
            transaction.commit();
        }
    };
    
    class KillOpFirstClause : public Base {
    public:
        void run() {
            _cli.insert( ns(), BSON( "_id" << 1 << "b" << 1 ) );
            _cli.insert( ns(), BSON( "_id" << 2 << "b" << 2 ) );
            _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
            
            Client::Transaction transaction(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);
            Client::ReadContext ctx( ns(), mongo::unittest::EMPTY_STRING );
            shared_ptr<Cursor> c = newQueryOptimizerCursor( ns(), BSON( "$or" << BSON_ARRAY( BSON( "_id" << GT << 0 ) << BSON( "b" << GT << 0 ) ) ) );
            ASSERT( c->ok() );
            cc().curop()->kill();
            // First advance() call throws, subsequent calls just fail.
            ASSERT_THROWS( c->advance(), MsgAssertionException );
            ASSERT( !c->advance() );
            transaction.commit();
        }
    };

    namespace ClientCursor {

        using mongo::ClientCursor;

        /** Test that a ClientCursor holding a QueryOptimizerCursor may be safely invalidated. */
        class Invalidate : public Base {
        public:
            void run() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
                _cli.insert( ns(), BSON( "a" << 1 << "b" << 1 ) );
                Client::Transaction transaction(DB_SERIALIZABLE);
                {
                    Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);

                    Client::Context ctx( ns() );
                    ClientCursor::Holder p
                            ( new ClientCursor
                             ( QueryOption_NoCursorTimeout,
                              getOptimizedCursor
                              ( ns(), BSON( "a" << GTE << 0 << "b" << GTE << 0 ) ),
                              ns() ) );
                    ClientCursor::invalidate( ns() );
                    ASSERT_EQUALS( 0U, nNsCursors() );
                }
                transaction.commit();
            }
        };
    
        /** Test that a ClientCursor holding a QueryOptimizerCursor may be safely timed out. */
        class TimeoutClientCursorHolder : public Base {
          public:
            void run() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
                _cli.insert( ns(), BSON( "a" << 1 << "b" << 1 ) );
                Client::Transaction transaction(DB_SERIALIZABLE);
                {
                    Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
                    Client::Context ctx( ns() );
                    ClientCursor::Holder p
                            ( new ClientCursor
                              ( 0,
                                getOptimizedCursor
                                ( ns(), BSON( "a" << GTE << 0 << "b" << GTE << 0 ) ),
                                ns() ) );
                
                    // Construct component client cursors.
                    ASSERT( nNsCursors() > 0 );
                
                    ClientCursor::invalidate( ns() );
                    ASSERT_EQUALS( 0U, nNsCursors() );
                }
                transaction.commit();
            }
        };
        
        /** Test that a ClientCursor holding a QueryOptimizerCursor may be safely timed out. */
        class Timeout : public Base {
        public:
            void run() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
                _cli.insert( ns(), BSON( "a" << 1 << "b" << 1 ) );
                Client::Transaction transaction(DB_SERIALIZABLE);
                {
                    Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);

                    Client::Context ctx( ns() );
                    ClientCursor::Holder p
                            ( new ClientCursor
                             ( 0,
                              getOptimizedCursor
                              ( ns(), BSON( "a" << GTE << 0 << "b" << GTE << 0 ) ),
                              ns() ) );
                    
                    // Construct component client cursors.
                    ASSERT( nNsCursors() > 0 );
                    
                    ClientCursor::idleTimeReport( 600001 );
                    ASSERT_EQUALS( 0U, nNsCursors() );
                }
                transaction.commit();
            }
        };
        
        /** The collection of a QueryOptimizerCursor stored in a ClientCursor is dropped. */
        class Drop : public Base {
        public:
            void run() {
                _cli.insert( ns(), BSON( "_id" << 1 ) );
                _cli.insert( ns(), BSON( "_id" << 2 ) );
                
                {
                    Client::Transaction transaction(DB_SERIALIZABLE);
                    {
                        Client::WriteContext ctx(ns(), mongo::unittest::EMPTY_STRING);
                        ClientCursor::Holder p
                            ( new ClientCursor
                             ( QueryOption_NoCursorTimeout,
                              getOptimizedCursor
                              ( ns(), BSON( "_id" << GT << 0 << "z" << 0 ) ),
                              ns() ) );

                        ASSERT_EQUALS( 1, p->c()->current().getIntField( "_id" ) );
                    }
                    transaction.commit();
                }
                
                // No assertion is expected when the collection is dropped and the cursor cannot be
                // recovered.
                
                _cli.dropCollection( ns() );
            }
        };
        
    } // namespace ClientCursor
        
    class AllowOutOfOrderPlan : public Base {
    public:
        void run() {
            Client::Transaction transaction(DB_SERIALIZABLE);
            Client::WriteContext ctx(ns(), mongo::unittest::EMPTY_STRING);
            shared_ptr<Cursor> c =
            newQueryOptimizerCursor( ns(), BSONObj(), BSON( "a" << 1 ),
                                    QueryPlanSelectionPolicy::any(), false );
            ASSERT( c );
            transaction.commit();
        }
    };
    
    class NoTakeoverByOutOfOrderPlan : public Base {
    public:
        void run() {
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            // Add enough early matches that the {$natural:1} plan would be chosen if it did not
            // require scan and order.
            for( int i = 0; i < 150; ++i ) {
                _cli.insert( ns(), BSON( "a" << 2 << "b" << 1 ) );
            }
            // Add non matches early on the {a:1} plan.
            for( int i = 0; i < 150; ++i ) {
                _cli.insert( ns(), BSON( "a" << 1 << "b" << 5 ) );
            }
            // Add enough matches outside the {a:1} index range that the {$natural:1} scan will not
            // complete before the {a:1} plan records 101 matches and is selected for takeover.
            for( int i = 0; i < 150; ++i ) {
                _cli.insert( ns(), BSON( "a" << 3 << "b" << 10 ) );
            }            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Client::WriteContext ctx(ns(), mongo::unittest::EMPTY_STRING);
            shared_ptr<Cursor> c =
            newQueryOptimizerCursor( ns(), BSON( "a" << LT << 3 << "b" << 1 ), BSON( "a" << 1 ),
                                    QueryPlanSelectionPolicy::any(), false );
            ASSERT( c );
            BSONObj idxKey;
            while( c->ok() ) {
                idxKey = c->indexKeyPattern();
                c->advance();
            }
            // Check that the ordered plan {a:1} took over, despite the unordered plan {$natural:1}
            // seeing > 101 matches.
            ASSERT_EQUALS( BSON( "a" << 1 ), idxKey );
            transaction.commit();
        }
    };
    
    /** If no in order plans are possible, an out of order plan may take over. */
    class OutOfOrderOnlyTakeover : public Base {
    public:
        void run() {
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            for( int i = 0; i < 300; ++i ) {
                _cli.insert( ns(), BSON( "a" << 1 ) );
                _cli.insert( ns(), BSON( "a" << 2 ) );
            }
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Client::WriteContext ctx(ns(), mongo::unittest::EMPTY_STRING);
            shared_ptr<Cursor> c =
            newQueryOptimizerCursor( ns(), BSON( "a" << 1 ), BSON( "b" << 1 ),
                                    QueryPlanSelectionPolicy::any(), false );
            ASSERT( c );
            while( c->advance() );
            // Check that one of the plans took over, and we didn't scan both plans until the a:1
            // index completed (which would yield an nscanned near 600).
            ASSERT( c->nscanned() < 500 );
            transaction.commit();
        }
    };
    
    class CoveredIndex : public Base {
    public:
        void run() {
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
            _cli.insert( ns(), BSON( "a" << 1 << "b" << 10 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Client::WriteContext ctx(ns(), mongo::unittest::EMPTY_STRING);
            shared_ptr<ParsedQuery> parsedQuery
                    ( new ParsedQuery( ns(), 0, 0, 0, BSONObj(), BSON( "_id" << 0 << "a" << 1 ) ) );
            shared_ptr<QueryOptimizerCursor> c =
            dynamic_pointer_cast<QueryOptimizerCursor>
            ( newQueryOptimizerCursor( ns(), BSON( "a" << GTE << 0 << "b" << GTE << 0 ),
                                      BSON( "a" << 1 ), QueryPlanSelectionPolicy::any(), false,
                                      parsedQuery ) );
            bool foundA = false;
            bool foundB = false;
            while( c->ok() ) {
                if ( c->indexKeyPattern() == BSON( "a" << 1 ) ) {
                    foundA = true;
                    ASSERT( c->keyFieldsOnly() );
                    ASSERT_EQUALS( BSON( "a" << 1 ), c->keyFieldsOnly()->hydrate( c->currKey(), c->currPK() ) );
                }
                if ( c->indexKeyPattern() == BSON( "b" << 1 ) ) {
                    foundB = true;
                    ASSERT( !c->keyFieldsOnly() );
                }
                c->advance();
            }
            ASSERT( foundA );
            ASSERT( foundB );
            transaction.commit();
        }
    };
    
    class CoveredIndexTakeover : public Base {
    public:
        void run() {
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
            for( int i = 0; i < 150; ++i ) {
                _cli.insert( ns(), BSON( "a" << 1 << "b" << 1 ) );
            }
            _cli.insert( ns(), BSON( "a" << 2 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Client::WriteContext ctx(ns(), mongo::unittest::EMPTY_STRING);
            shared_ptr<ParsedQuery> parsedQuery
                    ( new ParsedQuery( ns(), 0, 0, 0, BSONObj(), BSON( "_id" << 0 << "a" << 1 ) ) );
            shared_ptr<QueryOptimizerCursor> c =
            dynamic_pointer_cast<QueryOptimizerCursor>
            ( newQueryOptimizerCursor( ns(), fromjson( "{$or:[{a:1},{b:1},{a:2}]}" ), BSONObj(),
                                      QueryPlanSelectionPolicy::any(), false, parsedQuery ) );
            bool foundA = false;
            bool foundB = false;
            while( c->ok() ) {
                if ( c->indexKeyPattern() == BSON( "a" << 1 ) ) {
                    foundA = true;
                    ASSERT( c->keyFieldsOnly() );
                    ASSERT( BSON( "a" << 1 ) == c->keyFieldsOnly()->hydrate( c->currKey(), c->currPK() ) ||
                           BSON( "a" << 2 ) == c->keyFieldsOnly()->hydrate( c->currKey(), c->currPK() ) );
                }
                if ( c->indexKeyPattern() == BSON( "b" << 1 ) ) {
                    foundB = true;
                    ASSERT( !c->keyFieldsOnly() );
                }
                c->advance();
            }
            ASSERT( foundA );
            ASSERT( foundB );
            transaction.commit();
        }
    };
    
    class PlanChecking : public Base {
    public:
        virtual ~PlanChecking() {}
    protected:
        void nPlans( int n, const BSONObj &query, const BSONObj &order ) {
            auto_ptr< FieldRangeSetPair > frsp( new FieldRangeSetPair( ns(), query ) );
            auto_ptr< FieldRangeSetPair > frspOrig( new FieldRangeSetPair( *frsp ) );
            scoped_ptr<QueryPlanSet> s( QueryPlanSet::make( ns(), frsp, frspOrig, query, order,
                                                            shared_ptr<const ParsedQuery>(),
                                                            BSONObj(), QueryPlanGenerator::Use,
                                                            BSONObj(), BSONObj(), true ) );
            ASSERT_EQUALS( n, s->nPlans() );
        }
        static shared_ptr<QueryOptimizerCursor> getCursor( const BSONObj &query,
                                                          const BSONObj &order ) {
            shared_ptr<ParsedQuery> parsedQuery
                    ( new ParsedQuery( ns(), 0, 0, 0,
                                      BSON( "$query" << query << "$orderby" << order ),
                                      BSONObj() ) );
            shared_ptr<Cursor> cursor =
            getOptimizedCursor( ns(), query, order,
                                                  QueryPlanSelectionPolicy::any(), parsedQuery,
                                                  false );
            shared_ptr<QueryOptimizerCursor> ret =
            dynamic_pointer_cast<QueryOptimizerCursor>( cursor );
            ASSERT( ret );
            return ret;
        }
        void runQuery( const BSONObj &query, const BSONObj &order ) {
            shared_ptr<QueryOptimizerCursor> cursor = getCursor( query, order );
            while( cursor->advance() );
        }
    };
    
    class SaveGoodIndex : public PlanChecking {
    public:
        void run() {
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            _cli.ensureIndex( ns(), BSON( "b" << 1 ) );

            Lock::DBWrite lk(ns(), mongo::unittest::EMPTY_STRING);
            {
                Client::Transaction transaction(DB_SERIALIZABLE);
                Client::Context ctx( ns() );
                
                // No best plan - all must be tried.
                nPlans( 2 );
                runQuery();
                // Best plan selected by query.
                nPlans( 1 );
                nPlans( 1 );
                ensureIndex( ns(), BSON( "c" << 1 ), false, "c_1" );
                // Best plan cleared when new index added.
                nPlans( 2 );
                runQuery();
                // Best plan selected by query.
                nPlans( 1 );
                transaction.commit();
            }
            
            {
                DBDirectClient client;
                for( int i = 0; i < 334; ++i ) {
                    client.insert( ns(), BSON( "i" << i ) );
                    client.update( ns(), QUERY( "i" << i ), BSON( "i" << i + 1 ) );
                    client.remove( ns(), BSON( "i" << i + 1 ) );
                }
            }

            Client::Transaction transaction(DB_SERIALIZABLE);
            {
                Client::Context ctx( ns() );

                // Best plan cleared by ~1000 writes.
                nPlans( 2 );

                shared_ptr<ParsedQuery> parsedQuery
                        ( new ParsedQuery( ns(), 0, 0, 0,
                                          BSON( "$query" << BSON( "a" << 4 ) <<
                                               "$hint" << BSON( "$natural" << 1 ) ),
                                          BSON( "b" << 1 ) ) );
                shared_ptr<Cursor> cursor =
                getOptimizedCursor( ns(), BSON( "a" << 4 ), BSONObj(),
                                                      QueryPlanSelectionPolicy ::any(), parsedQuery,
                                                      false );
                while( cursor->advance() );
                // No plan recorded when a hint is used.
                nPlans( 2 );
                
                shared_ptr<ParsedQuery> parsedQuery2
                        ( new ParsedQuery( ns(), 0, 0, 0,
                                          BSON( "$query" << BSON( "a" << 4 ) <<
                                               "$orderby" << BSON( "b" << 1 << "c" << 1 ) ),
                                          BSONObj() ) );
                shared_ptr<Cursor> cursor2 =
                getOptimizedCursor( ns(), BSON( "a" << 4 ),
                                                     BSON( "b" << 1 << "c" << 1 ),
                                                     QueryPlanSelectionPolicy ::any(),
                                                     parsedQuery2, false );
                while( cursor2->advance() );
                // Plan recorded was for a different query pattern (different sort spec).
                nPlans( 2 );
                
                // Best plan still selected by query after all these other tests.
                runQuery();
                nPlans( 1 );
            }
            transaction.commit();
        }
    private:
        void nPlans( int n ) {
            return PlanChecking::nPlans( n, BSON( "a" << 4 ), BSON( "b" << 1 ) );
        }
        void runQuery() {
            return PlanChecking::runQuery( BSON( "a" << 4 ), BSON( "b" << 1 ) );
        }
    };
    
    class PossiblePlans : public PlanChecking {
    protected:
        void checkCursor( bool mayRunInOrderPlan, bool mayRunOutOfOrderPlan,
                         bool runningInitialInOrderPlan, bool possiblyExcludedPlans ) {
            CandidatePlanCharacter plans = _cursor->initialCandidatePlans();
            ASSERT_EQUALS( mayRunInOrderPlan, plans.mayRunInOrderPlan() );
            ASSERT_EQUALS( mayRunOutOfOrderPlan, plans.mayRunOutOfOrderPlan() );
            ASSERT_EQUALS( runningInitialInOrderPlan, _cursor->runningInitialInOrderPlan() );
            ASSERT_EQUALS( possiblyExcludedPlans, _cursor->hasPossiblyExcludedPlans() );            
        }
        void setCursor( const BSONObj &query, const BSONObj &order ) {
            _cursor = PlanChecking::getCursor( query, order );
        }
        void runCursor( bool completePlanOfHybridSetScanAndOrderRequired = false ) {
            while( _cursor->ok() ) {
                checkIterate( _cursor );
                _cursor->advance();
            }
            ASSERT_EQUALS( completePlanOfHybridSetScanAndOrderRequired,
                          _cursor->completePlanOfHybridSetScanAndOrderRequired() );
        }
        void runCursorUntilTakeover() {
            // This is a bit of a hack, relying on initialFieldRangeSet() being nonzero before
            // takeover and zero after takeover.
            while( _cursor->ok() && _cursor->initialFieldRangeSet() ) {
                checkIterate( _cursor );
                _cursor->advance();
            }
        }
        void checkTakeoverCursor( bool currentPlanScanAndOrderRequired ) {
            ASSERT( !_cursor->initialFieldRangeSet() );
            ASSERT_EQUALS( currentPlanScanAndOrderRequired,
                          _cursor->currentPlanScanAndOrderRequired() );
            ASSERT( !_cursor->completePlanOfHybridSetScanAndOrderRequired() );
            ASSERT( !_cursor->runningInitialInOrderPlan() );
            ASSERT( !_cursor->hasPossiblyExcludedPlans() );
        }
        virtual void checkIterate( const shared_ptr<QueryOptimizerCursor> &cursor ) const = 0;
        shared_ptr<QueryOptimizerCursor> _cursor;
    };
    
    class PossibleBothPlans : public PossiblePlans {
    public:
        void run() {
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
            _cli.insert( ns(), BSON( "a" << 1 << "b" << 1 ) );
            _cli.insert( ns(), BSON( "a" << 2 << "b" << 1 ) );
            for( int i = 0; i < 20; ++i ) {
                _cli.insert( ns(), BSON( "a" << 2 ) );
                if ( i % 10 == 0 ) {
                    _cli.insert( ns(), BSON( "b" << 2 ) );                    
                }
            }
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Client::WriteContext ctx(ns(), mongo::unittest::EMPTY_STRING);
            nPlans( 2, BSON( "a" << 1 << "b" << 1 ), BSON( "b" << 1 ) );
            setCursor( BSON( "a" << 1 << "b" << 1 ), BSON( "b" << 1 ) );
            checkCursor( true, false );
            ASSERT( _cursor->initialFieldRangeSet()->range( "a" ).equality() );
            ASSERT( _cursor->initialFieldRangeSet()->range( "b" ).equality() );
            ASSERT( !_cursor->initialFieldRangeSet()->range( "x" ).equality() );
            
            // Without running the (nonempty) cursor, no cached plan is recorded.
            setCursor( BSON( "a" << 1 << "b" << 1 ), BSON( "b" << 1 ) );
            checkCursor( true, false );
            
            // Running the cursor records the a:1 plan.
            runCursor( true );
            nPlans( 1, BSON( "a" << 1 << "b" << 1 ), BSON( "b" << 1 ) );
            setCursor( BSON( "a" << 1 << "b" << 1 ), BSON( "b" << 1 ) );
            checkCursor( false, true );
            
            // Other plans may be added.
            setCursor( BSON( "a" << 2 << "b" << 2 ), BSON( "b" << 1 ) );
            checkCursor( false, true );
            for( int i = 0; i < 10; ++i, _cursor->advance() );
            // The other plans have been added in (including ordered b:1).
            checkCursor( true, false );
            runCursor( false );
            
            // The b:1 plan was recorded.
            setCursor( BSON( "a" << 1 << "b" << 1 ), BSON( "b" << 1 ) );
            checkCursor( true, true );
            
            // Clear the recorded plan manually.
            _cursor->clearIndexesForPatterns();
            setCursor( BSON( "a" << 2 << "b" << 1 ), BSON( "b" << 1 ) );
            checkCursor( true, false );
            
            // Add more data, and run until takeover occurs.
            for( int i = 0; i < 120; ++i ) {
                _cli.insert( ns(), BSON( "a" << 3 << "b" << 3 ) );
            }
            
            setCursor( BSON( "a" << 3 << "b" << 3 ), BSON( "b" << 1 ) );
            checkCursor( true, false );
            runCursorUntilTakeover();
            ASSERT( _cursor->ok() );
            checkTakeoverCursor( false );
            ASSERT_EQUALS( BSON( "b" << 1 ), _cursor->indexKeyPattern() );
            
            // Try again, with a cached plan this time.
            setCursor( BSON( "a" << 3 << "b" << 3 ), BSON( "b" << 1 ) );
            checkCursor( true, true );
            runCursorUntilTakeover();
            checkTakeoverCursor( false );
            ASSERT_EQUALS( BSON( "b" << 1 ), _cursor->indexKeyPattern() );
            transaction.commit();
        }
    private:
        void checkCursor( bool runningInitialInOrderPlan, bool runningInitialCachedPlan ) {
            return PossiblePlans::checkCursor( true, true, runningInitialInOrderPlan,
                                              runningInitialCachedPlan );
        }
        virtual void checkIterate( const shared_ptr<QueryOptimizerCursor> &cursor ) const {
            if ( cursor->indexKeyPattern() == BSON( "b" << 1 ) ) {
                ASSERT( !cursor->currentPlanScanAndOrderRequired() );
            }
            else {
                ASSERT( cursor->currentPlanScanAndOrderRequired() );                
            }
            ASSERT( !cursor->completePlanOfHybridSetScanAndOrderRequired() );
        }
    };
    
    /** Out of order plans are not added after abortOutOfOrderPlans() is called. */
    class AbortOutOfOrderPlansBeforeAddOtherPlans : public PlanChecking {
    public:
        void run() {
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
            _cli.insert( ns(), BSON( "a" << -1 << "b" << 0 ) );
            for( int b = 0; b < 2; ++b ) {
                for( int a = 0; a < 10; ++a ) {
                    _cli.insert( ns(), BSON( "a" << a << "b" << b ) );
                }
            }

            // Selectivity is better for the a:1 index.
            _aPreferableQuery = BSON( "a" << GTE << -100 << LTE << -1 << "b" << 0 );
            // Selectivity is better for the b:1 index.
            _bPreferableQuery = BSON( "a" << GTE << 0 << LTE << 100 << "b" << 0 );

            Client::Transaction transaction(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);
            Client::ReadContext ctx( ns() , mongo::unittest::EMPTY_STRING);
            
            // If abortOutOfOrderPlans() is not set, other plans will be attempted.
            recordAIndex();
            _cursor = getCursor( _bPreferableQuery, BSON( "a" << 1 ) );
            checkInitialIteratePlans();
            // The b:1 index is attempted later.
            checkBIndexUsed( true );

            // If abortOutOfOrderPlans() is set, other plans will not be attempted.
            recordAIndex();
            _cursor = getCursor( _bPreferableQuery, BSON( "a" << 1 ) );
            checkInitialIteratePlans();
            _cursor->abortOutOfOrderPlans();
            // The b:1 index is not attempted.
            checkBIndexUsed( false );
            transaction.commit();
        }
    private:
        /** Record the a:1 index for the query pattern of interest. */
        void recordAIndex() const {
            Client::Transaction transaction(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);
            Client::ReadContext ctx( ns() , mongo::unittest::EMPTY_STRING);
            getCollection(ns())->getQueryCache().clearQueryCache();
            shared_ptr<QueryOptimizerCursor> c = getCursor( _aPreferableQuery, BSON( "a" << 1 ) );
            while( c->advance() );
            FieldRangeSet aPreferableFields( ns(), _aPreferableQuery, true, true );
            ASSERT_EQUALS( BSON( "a" << 1 ),
                          getCollection(ns())->getQueryCache().cachedQueryPlanForPattern
                          ( aPreferableFields.pattern( BSON( "a" << 1 ) ) ).indexKey() );
            transaction.commit();
        }
        /** The first results come from the recorded index. */
        void checkInitialIteratePlans() const {
            for( int i = 0; i < 5; ++i ) {
                ASSERT_EQUALS( BSON( "a" << 1 ), _cursor->indexKeyPattern() );
            }            
        }
        /** Check if the b:1 index is used during iteration. */
        void checkBIndexUsed( bool expected ) const {
            bool bIndexUsed = false;
            while( _cursor->advance() ) {
                if ( BSON( "b" << 1 ) == _cursor->indexKeyPattern() ) {
                    bIndexUsed = true;
                }
            }
            ASSERT_EQUALS( expected, bIndexUsed );            
        }
        BSONObj _aPreferableQuery;
        BSONObj _bPreferableQuery;
        shared_ptr<QueryOptimizerCursor> _cursor;
    };
    
    class TakeoverOrRangeElimination : public PlanChecking {
    public:
        void run() {
            _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            for( int i = 0; i < 120; ++i ) {
                _cli.insert( ns(), BSON( "a" << 1 ) );
            }
            for( int i = 0; i < 20; ++i ) {
                _cli.insert( ns(), BSON( "a" << 2 ) );
            }
            for( int i = 0; i < 20; ++i ) {
                _cli.insert( ns(), BSON( "a" << 3 ) );
            }

            Client::Transaction transaction(DB_SERIALIZABLE);
            Client::WriteContext ctx(ns(), mongo::unittest::EMPTY_STRING);
            
            shared_ptr<QueryOptimizerCursor> c =
            getCursor( fromjson( "{$or:[{a:{$lte:2}},{a:{$gte:2}},{a:9}]}" ), BSONObj() );

            int count = 0;
            while( c->ok() ) {
                c->advance();
                ++count;
            }
            ASSERT_EQUALS( 160, count );
            transaction.commit();
        }
    };
    
    class TakeoverOrDedups : public PlanChecking {
    public:
        void run() {
            _cli.ensureIndex( ns(), BSON( "a" << 1 << "b" << 1 ) );
            for( int i = 0; i < 120; ++i ) {
                _cli.insert( ns(), BSON( "a" << 1 << "b" << 1 ) );
            }
            for( int i = 0; i < 20; ++i ) {
                _cli.insert( ns(), BSON( "a" << 2 << "b" << 2 ) );
            }
            for( int i = 0; i < 20; ++i ) {
                _cli.insert( ns(), BSON( "a" << 3 << "b" << 3 ) );
            }
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Client::WriteContext ctx(ns(), mongo::unittest::EMPTY_STRING);
            
            BSONObj query =
            BSON(
                 "$or" << BSON_ARRAY(
                                     BSON(
                                          "a" << GTE << 0 << LTE << 2 <<
                                          "b" << GTE << 0 << LTE << 2
                                          ) <<
                                     BSON(
                                          "a" << GTE << 1 << LTE << 3 <<
                                          "b" << GTE << 1 << LTE << 3
                                          ) <<
                                     BSON(
                                          "a" << GTE << 1 << LTE << 4 <<
                                          "b" << GTE << 1 << LTE << 4
                                          )
                                     )
                 );
                                     
            shared_ptr<QueryOptimizerCursor> c = getCursor( query, BSONObj() );
            
            int count = 0;
            while( c->ok() ) {
                if ( ( c->indexKeyPattern() == BSON( "a" << 1 << "b" << 1 ) ) &&
                    c->currentMatches() ) {
                    ++count;
                }
                c->advance();
            }
            ASSERT_EQUALS( 160, count );
            transaction.commit();
        }
    };
    
    /** Proper index matching when transitioning between $or clauses after a takeover. */
    class TakeoverOrDifferentIndex : public PlanChecking {
    public:
        void run() {
            for( int i = 0; i < 120; ++i ) {
                _cli.insert( ns(), BSON( "a" << 1 << "b" << 2 ) );
            }
            _cli.ensureIndex( ns(), BSON( "a" << 1 << "b" << 1 ) );
            for( int i = 0; i < 130; ++i ) {
                _cli.insert( ns(), BSON( "b" << 3 << "a" << 4 ) );
            }
            _cli.ensureIndex( ns(), BSON( "b" << 1 << "a" << 1 ) );
            
            Client::Transaction transaction(DB_SERIALIZABLE);
            Client::WriteContext ctx(ns(), mongo::unittest::EMPTY_STRING);

            // This $or query will scan index a:1,b:1 then b:1,a:1.  If the key pattern is specified
            // incorrectly for the second clause, matching will fail.
            setQueryOptimizerCursor( fromjson( "{$or:[{a:1,b:{$gte:0}},{b:3,a:{$gte:0}}]}" ) );
            // All documents match, and there are no dups.
            ASSERT_EQUALS( 250, itcount() );
            transaction.commit();
        }
    };

    /** Check that an elemMatchKey can be retrieved from MatchDetails using a qo cursor. */
    class ElemMatchKey : public Base {
    public:
        void run() {
            _cli.ensureIndex( ns(), BSON( "a.b" << 1 ) );
            _cli.insert( ns(), fromjson( "{ a:[ { b:1 } ] }" ) );

            Client::Transaction transaction(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY);
            Client::ReadContext ctx( ns() , mongo::unittest::EMPTY_STRING);
            setQueryOptimizerCursor( BSON( "a.b" << 1 ) );
            MatchDetails details;
            details.requestElemMatchKey();
            ASSERT( c()->currentMatches( &details ) );
            // The '0' entry of the 'a' array is matched.
            ASSERT_EQUALS( string( "0" ), details.elemMatchKey() );
            transaction.commit();
        }
    };

    namespace GetCursor {
        
        class Base : public QueryOptimizerCursorTests::Base {
        public:
            Base() {
                // create collection
                _cli.insert( ns(), BSON( "_id" << 5 ) );
            }
            virtual ~Base() {}
            void run() {
                Client::Transaction transaction(DB_SERIALIZABLE);
                Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
                Client::Context ctx( ns() );
                if ( expectException() ) {
                    ASSERT_THROWS
                    ( getOptimizedCursor
                     ( ns(), query(), order(), planPolicy() ),
                     MsgAssertionException );
                    return;
                }
                _query = query();
                _parsedQuery.reset( new ParsedQuery( ns(), skip(), limit(), 0, _query,
                                                    BSONObj() ) );
                BSONObj extractedQuery = _query;
                if ( !_query["$query"].eoo() ) {
                    extractedQuery = _query["$query"].Obj();
                }
                shared_ptr<Cursor> c =
                getOptimizedCursor( ns(), extractedQuery, order(), planPolicy(),
                                                      _parsedQuery, false );
                string type = c->toString().substr( 0, expectedType().length() );
                ASSERT_EQUALS( expectedType(), type );
                check( c );
                transaction.commit();
            }
        protected:
            virtual string expectedType() const { return "TESTDUMMY"; }
            virtual bool expectException() const { return false; }
            virtual BSONObj query() const { return BSONObj(); }
            virtual BSONObj order() const { return BSONObj(); }
            virtual int skip() const { return 0; }
            virtual int limit() const { return 0; }
            virtual const QueryPlanSelectionPolicy &planPolicy() const {
                return QueryPlanSelectionPolicy::any();
            }
            virtual void check( const shared_ptr<Cursor> &c ) {
                ASSERT( c->ok() );
                ASSERT( !c->matcher() );
                ASSERT_EQUALS( 5, c->current().getIntField( "_id" ) );
                ASSERT( !c->advance() );
            }
        private:
            BSONObj _query;
            shared_ptr<ParsedQuery> _parsedQuery;
        };
        
        class NoConstraints : public Base {
            string expectedType() const { return "BasicCursor"; }
        };
        
        class SimpleId : public Base {
        public:
            SimpleId() {
                _cli.insert( ns(), BSON( "_id" << 0 ) );
                _cli.insert( ns(), BSON( "_id" << 10 ) );
            }
            string expectedType() const { return "IndexCursor _id_"; }
            BSONObj query() const { return BSON( "_id" << 5 ); }
        };
        
        class OptimalIndex : public Base {
        public:
            OptimalIndex() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                _cli.insert( ns(), BSON( "a" << 5 ) );
                _cli.insert( ns(), BSON( "a" << 6 ) );
            }
            string expectedType() const { return "IndexCursor a_1"; }
            BSONObj query() const { return BSON( "a" << GTE << 5 ); }
            void check( const shared_ptr<Cursor> &c ) {
                ASSERT( c->ok() );
                ASSERT( c->matcher() );
                ASSERT_EQUALS( 5, c->current().getIntField( "a" ) );
                ASSERT( c->matcher()->matchesCurrent( c.get() ) );
                ASSERT( c->advance() );                    
                ASSERT_EQUALS( 6, c->current().getIntField( "a" ) );
                ASSERT( c->matcher()->matchesCurrent( c.get() ) );
                ASSERT( !c->advance() );                    
            }
        };
        
        class SimpleKeyMatch : public Base {
        public:
            SimpleKeyMatch() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                _cli.update( ns(), BSONObj(), BSON( "$set" << BSON( "a" << true ) ) );
            }
            string expectedType() const { return "IndexCursor a_1"; }
            bool expectSimpleEquality() const { return true; }
            BSONObj query() const { return BSON( "a" << true ); }
            virtual void check( const shared_ptr<Cursor> &c ) {
                ASSERT( c->ok() );
                ASSERT_EQUALS( 5, c->current().getIntField( "_id" ) );
                ASSERT( !c->advance() );
            }
        };
        
        class Geo : public Base {
        public:
            Geo() {
                _cli.insert( ns(), BSON( "_id" << 44 << "loc" << BSON_ARRAY( 44 << 45 ) ) );
                _cli.ensureIndex( ns(), BSON( "loc" << "2d" ) );
            }
            string expectedType() const { return "GeoSearchCursor"; }
            BSONObj query() const { return fromjson( "{ loc : { $near : [50,50] } }" ); }
            void check( const shared_ptr<Cursor> &c ) {
                ASSERT( c->ok() );
                ASSERT( c->matcher() );
                ASSERT( c->matcher()->matchesCurrent( c.get() ) );
                ASSERT_EQUALS( 44, c->current().getIntField( "_id" ) );
                ASSERT( !c->advance() );
            }
        };
        
        class GeoNumWanted : public Base {
        public:
            GeoNumWanted() {
                _cli.ensureIndex( ns(), BSON( "loc" << "2d" ) );
                for( int i = 0; i < 140; ++i ) {
                    _cli.insert( ns(), BSON( "loc" << BSON_ARRAY( 44 << 45 ) ) );
                }
            }
            string expectedType() const { return "GeoSearchCursor"; }
            BSONObj query() const { return fromjson( "{ loc : { $near : [50,50] } }" ); }
            void check( const shared_ptr<Cursor> &c ) {
                int count = 0;
                while( c->ok() ) {
                    ++count;
                    c->advance();
                }
                ASSERT_EQUALS( 130, count );
            }
            int skip() const { return 27; }
            int limit() const { return 103; }
        };
        
        class PreventOutOfOrderPlan : public QueryOptimizerCursorTests::Base {
        public:
            void run() {
                _cli.insert( ns(), BSON( "_id" << 5 ) );
                Client::Transaction transaction(DB_SERIALIZABLE);
                Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
                Client::Context ctx( ns() );
                shared_ptr<Cursor> c = getOptimizedCursor( ns(), BSONObj(), BSON( "b" << 1 ) );
                ASSERT( !c );
                transaction.commit();
            }
        };
        
        class AllowOutOfOrderPlan : public Base {
        public:
            void run() {
                Client::Transaction transaction(DB_SERIALIZABLE);
                Client::WriteContext ctx(ns(), mongo::unittest::EMPTY_STRING);
                shared_ptr<ParsedQuery> parsedQuery
                        ( new ParsedQuery( ns(), 0, 0, 0,
                                          BSON( "$query" << BSONObj() <<
                                               "$orderby" << BSON( "a" << 1 ) ),
                                          BSONObj() ) );
                shared_ptr<Cursor> c =
                getOptimizedCursor( ns(), BSONObj(), BSON( "a" << 1 ),
                                                     QueryPlanSelectionPolicy ::any(),
                                                     parsedQuery, false );
                ASSERT( c );
                transaction.commit();
            }
        };
        
        class BestSavedOutOfOrder : public QueryOptimizerCursorTests::Base {
        public:
            void run() {
                _cli.insert( ns(), BSON( "_id" << 5 << "b" << BSON_ARRAY( 1 << 2 << 3 << 4 << 5 ) ) );
                _cli.insert( ns(), BSON( "_id" << 1 << "b" << 6 ) );
                _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
                // record {_id:1} index for this query
                ASSERT( _cli.query( ns(), QUERY( "_id" << GT << 0 << "b" << GT << 0 ).sort( "b" ) )->more() );
                Client::Transaction transaction(DB_SERIALIZABLE);
                Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
                Client::Context ctx( ns() );
                shared_ptr<Cursor> c = getOptimizedCursor( ns(), BSON( "_id" << GT << 0 << "b" << GT << 0 ), BSON( "b" << 1 ) );
                // {_id:1} requires scan and order, so {b:1} must be chosen.
                ASSERT( c );
                ASSERT_EQUALS( 5, c->current().getIntField( "_id" ) );
                transaction.commit();
            }
        };

        /**
         * If an optimal plan is cached, return a Cursor for it rather than a QueryOptimizerCursor.
         */
        class BestSavedOptimal : public QueryOptimizerCursorTests::Base {
        public:
            void run() {
                _cli.insert( ns(), BSON( "_id" << 1 ) );
                _cli.ensureIndex( ns(), BSON( "_id" << 1 << "q" << 1 ) );
                ASSERT( _cli.query( ns(), QUERY( "_id" << GT << 0 ) )->more() );
                Client::Transaction transaction(DB_SERIALIZABLE);
                Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
                Client::Context ctx( ns() );
                // Check the plan that was recorded for this query.
                ASSERT_EQUALS( BSON( "_id" << 1 ), cachedIndexForQuery( BSON( "_id" << GT << 0 ) ) );
                shared_ptr<Cursor> c = getOptimizedCursor( ns(), BSON( "_id" << GT << 0 ) );
                // No need for query optimizer cursor since the plan is optimal.
                ASSERT_EQUALS( "IndexCursor _id_", c->toString() );
                transaction.commit();
            }
        };
        
        /** If a non optimal plan is a candidate a QueryOptimizerCursor should be returned, even if plan has been recorded. */
        class BestSavedNotOptimal : public QueryOptimizerCursorTests::Base {
        public:
            void run() {
                _cli.insert( ns(), BSON( "_id" << 1 << "q" << 1 ) );
                _cli.ensureIndex( ns(), BSON( "q" << 1 ) );
                // Record {_id:1} index for this query
                // Need to use a range on _id so that the queryByIdHack path is not taken.
                ASSERT( _cli.query( ns(), QUERY( "q" << 1 << "_id" << GTE << 1 << LTE << 1 ) )->more() );
                Client::Transaction transaction(DB_SERIALIZABLE);
                Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
                Client::Context ctx( ns() );
                ASSERT_EQUALS( BSON( "_id" << 1 ),
                              cachedIndexForQuery( BSON( "q" << 1 << "_id" << 1 ) ) );
                shared_ptr<Cursor> c = getOptimizedCursor( ns(), BSON( "q" << 1 << "_id" << 1 ) );
                // Need query optimizer cursor since the cached plan is not optimal.
                ASSERT_EQUALS( "QueryOptimizerCursor", c->toString() );
                transaction.commit();
            }
        };
                
        class MultiIndex : public Base {
        public:
            MultiIndex() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            }
            string expectedType() const { return "QueryOptimizerCursor"; }
            BSONObj query() const { return BSON( "_id" << GT << 0 << "a" << GT << 0 ); }
            void check( const shared_ptr<Cursor> &c ) {}
        };
        
        class Hint : public Base {
        public:
            Hint() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            }
            string expectedType() const { return "IndexCursor a_1"; }
            BSONObj query() const {
                return BSON( "$query" << BSON( "_id" << 1 ) << "$hint" << BSON( "a" << 1 ) );
            }
            void check( const shared_ptr<Cursor> &c ) {}
        };
        
        // $snapshot used to force the _id index (because that's how vanilla would
        // guarantee a 'snapshot' of the data with no duplicate objects due to
        // update/etc). TokuMX does not have this restriction so if you ask for
        // a query on 'a' with an index, you get the right index.
        class SnapshotOptionNoLongerNecessary : public Base {
        public:
            SnapshotOptionNoLongerNecessary() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            }
            string expectedType() const { return "IndexCursor a_1"; }
            BSONObj query() const {
                return BSON( "$query" << BSON( "a" << 1 ) << "$snapshot" << true );
            }
            void check( const shared_ptr<Cursor> &c ) {}            
        };
        
        class Min : public Base {
        public:
            Min() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            }
            string expectedType() const { return "IndexCursor a_1"; }
            BSONObj query() const {
                return BSON( "$query" << BSONObj() << "$min" << BSON( "a" << 1 ) );
            }
            void check( const shared_ptr<Cursor> &c ) {}            
        };
        
        class Max : public Base {
        public:
            Max() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            }
            string expectedType() const { return "IndexCursor a_1"; }
            BSONObj query() const {
                return BSON( "$query" << BSONObj() << "$max" << BSON( "a" << 1 ) );
            }
            void check( const shared_ptr<Cursor> &c ) {}            
        };
        
        namespace RequireIndex {
            
            class Base : public GetCursor::Base {
                const QueryPlanSelectionPolicy &planPolicy() const {
                    return QueryPlanSelectionPolicy::indexOnly();
                }
            };
            
            class NoConstraints : public Base {
                bool expectException() const { return true; }
            };

            class SimpleId : public Base {
                string expectedType() const { return "IndexCursor _id_"; }
                BSONObj query() const { return BSON( "_id" << 5 ); }
            };

            class UnindexedQuery : public Base {
                bool expectException() const { return true; }
                BSONObj query() const { return BSON( "a" << GTE << 5 ); }
            };

            class IndexedQuery : public Base {
            public:
                IndexedQuery() {
                    _cli.insert( ns(), BSON( "_id" << 6 << "a" << 6 << "c" << 4 ) );
                    _cli.ensureIndex( ns(), BSON( "a" << 1 << "b" << 1 << "c" << 1 ) );
                }
                string expectedType() const { return "IndexCursor a_1_b_1"; }
                BSONObj query() const { return BSON( "a" << GTE << 5 << "c" << 4 ); }
                void check( const shared_ptr<Cursor> &c ) {
                    ASSERT( c->ok() );
                    ASSERT( c->matcher() );
                    ASSERT_EQUALS( 6, c->current().getIntField( "_id" ) );
                    ASSERT( !c->advance() );
                }
            };

            class SecondOrClauseIndexed : public Base {
            public:
                SecondOrClauseIndexed() {
                    _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                    _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
                    _cli.insert( ns(), BSON( "a" << 1 ) );
                    _cli.insert( ns(), BSON( "b" << 1 ) );
                }
                string expectedType() const { return "QueryOptimizerCursor"; }
                BSONObj query() const { return fromjson( "{$or:[{a:1},{b:1}]}" ); }
                void check( const shared_ptr<Cursor> &c ) {
                    ASSERT( c->ok() );
                    ASSERT( c->matcher() );
                    ASSERT( c->advance() );
                    ASSERT( !c->advance() ); // 2 matches exactly
                }
            };
            
            class SecondOrClauseUnindexed : public Base {
            public:
                SecondOrClauseUnindexed() {
                    _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                    _cli.insert( ns(), BSON( "a" << 1 ) );
                }
                bool expectException() const { return true; }
                BSONObj query() const { return fromjson( "{$or:[{a:1},{b:1}]}" ); }
            };

            class SecondOrClauseUnindexedUndetected : public Base {
            public:
                SecondOrClauseUnindexedUndetected() {
                    _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                    _cli.ensureIndex( ns(), BSON( "a" << 1 << "b" << 1 ) );
                    _cli.insert( ns(), BSON( "a" << 1 ) );
                    _cli.insert( ns(), BSON( "b" << 1 ) );
                }
                string expectedType() const { return "QueryOptimizerCursor"; }
                BSONObj query() const { return fromjson( "{$or:[{a:1},{b:1}]}" ); }
                void check( const shared_ptr<Cursor> &c ) {
                    ASSERT( c->ok() );
                    ASSERT( c->matcher() );
                    // An unindexed cursor is required for the second clause, but is not allowed.
                    ASSERT_THROWS( c->advance(), MsgAssertionException );
                }
            };
            
        } // namespace RequireIndex
        
        /**
         * Generating a cursor for an invalid query asserts, even if the collection is empty or
         * missing.
         */
        class MatcherValidation : public Base {
        public:
            void run() {
                // Matcher validation with an empty collection.
                _cli.remove( ns(), BSONObj() );
                checkInvalidQueryAssertions();
                
                // Matcher validation with a missing collection.
                _cli.dropCollection( ns() );
                checkInvalidQueryAssertions();
            }
        private:
            static void checkInvalidQueryAssertions() {
                Client::ReadContext ctx( ns() , mongo::unittest::EMPTY_STRING);
                
                // An invalid query generating a single query plan asserts.
                BSONObj invalidQuery = fromjson( "{$and:[{$atomic:true}]}" );
                assertInvalidQueryAssertion( invalidQuery );
                
                // An invalid query generating multiple query plans asserts.
                BSONObj invalidIdQuery = fromjson( "{_id:0,$and:[{$atomic:true}]}" );
                assertInvalidQueryAssertion( invalidIdQuery );
            }
            static void assertInvalidQueryAssertion( const BSONObj &query ) {
                ASSERT_THROWS( getOptimizedCursor( ns(), query, BSONObj() ),
                               UserException );
            }
        };

        class RequestMatcherFalse : public QueryPlanSelectionPolicy {
            virtual string name() const { return "RequestMatcherFalse"; }
            virtual bool requestMatcher() const { return false; }
        } _requestMatcherFalse;
        /**
         * A Cursor returned by getOptimizedCursor() may or may not have a
         * matcher().  A Matcher will generally exist if required to match the provided query or
         * if specifically requested.
         */
        class MatcherSet : public Base {
        public:
            MatcherSet() {
                _cli.insert( ns(), BSON( "a" << 2 << "b" << 3 ) );
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            }
            void run() {
                Client::Transaction transaction(DB_SERIALIZABLE);
                {
                    // No matcher is set for an empty query.
                    ASSERT( !hasMatcher( BSONObj(), false ) );
                    // No matcher is set for an empty query, even if a matcher is requested.
                    ASSERT( !hasMatcher( BSONObj(), true ) );
                    // No matcher is set for an exact key match indexed query.
                    ASSERT( !hasMatcher( BSON( "a" << 2 ), false ) );
                    // No matcher is set for an exact key match indexed query, unless one is requested.
                    ASSERT( hasMatcher( BSON( "a" << 2 ), true ) );
                    // A matcher is set for a non exact key match indexed query.
                    ASSERT( hasMatcher( BSON( "a" << 2 << "b" << 3 ), false ) );
                }
                transaction.commit();
            }
        private:
            bool hasMatcher( const BSONObj& query, bool requestMatcher ) {
                Client::ReadContext ctx( ns() , mongo::unittest::EMPTY_STRING);
                shared_ptr<Cursor> cursor = getOptimizedCursor( ns(),
                                                                query,
                                                                BSONObj(),
                                                                requestMatcher ?
                                                                QueryPlanSelectionPolicy::any():
                                                                _requestMatcherFalse );
                return cursor->matcher();
            }
        };

        /**
         * Even though a Matcher may not be used to perform matching when requestMatcher == false, a
         * Matcher must be created because the Matcher's constructor performs query validation.
         */
        class MatcherValidate : public Base {
        public:
            void run() {
                Client::ReadContext ctx( ns() , mongo::unittest::EMPTY_STRING);
                Client::Transaction transaction(DB_SERIALIZABLE);
                {
                    // An assertion is triggered because { a:undefined } is an invalid query, even
                    // though no matcher is required.
                    ASSERT_THROWS( getOptimizedCursor( ns(),
                                                       fromjson( "{a:undefined}" ),
                                                       BSONObj(),
                                                       _requestMatcherFalse ),
                                   UserException );
                }
            }
        };

        class RequestCountingCursorTrue : public QueryPlanSelectionPolicy {
            virtual string name() const { return "RequestCountingCursorTrue"; }
            virtual bool requestCountingCursor() const { return true; }
        } _requestCountCursorTrue;

        /** An IndexCountCursor is selected when requested by a QueryPlanSelectionPolicy. */
        class CountCursor : public Base {
        public:
            CountCursor() {
                _cli.insert( ns(), BSON( "a" << 2 << "b" << 3 ) );
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            }
            void run() {
                Client::ReadContext ctx( ns() , mongo::unittest::EMPTY_STRING);
                shared_ptr<Cursor> cursor = getOptimizedCursor( ns(),
                                                                BSON( "a" << 1 ),
                                                                BSONObj(),
                                                                _requestCountCursorTrue );
                ASSERT_EQUALS( "IndexCountCursor", cursor->toString() );
            }
        };
    } // namespace GetCursor
    
    namespace Explain {

        class ClearRecordedIndex : public QueryOptimizerCursorTests::Base {
        public:
            void run() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
                _cli.insert( ns(), BSON( "a" << 1 << "b" << 1 ) );
                _cli.insert( ns(), BSON( "a" << 1 << "b" << 1 ) );
                
                Client::Transaction transaction(DB_SERIALIZABLE);
                {
                    Client::WriteContext ctx(ns(), mongo::unittest::EMPTY_STRING);
                    BSONObj query = BSON( "a" << 1 << "b" << 1 );
                    shared_ptr<Cursor> c =
                    getOptimizedCursor( ns(), query );
                    while( c->advance() );
                    shared_ptr<ParsedQuery> parsedQuery
                            ( new ParsedQuery( ns(), 0, 0, 0,
                                              BSON( "$query" << query << "$explain" << true ),
                                              BSONObj() ) );
                    c = getOptimizedCursor( ns(),
                                            query,
                                            BSONObj(),
                                            QueryPlanSelectionPolicy::any(),
                                            parsedQuery,
                                            false );
                    set<BSONObj> indexKeys;
                    while( c->ok() ) {
                        indexKeys.insert( c->indexKeyPattern() );
                        c->advance();
                    }
                    ASSERT( indexKeys.size() > 1 );
                }
                transaction.commit();
            }
        };
        
        class Base : public QueryOptimizerCursorTests::Base {
        public:
            virtual ~Base() {}
            void run() {
                setupCollection();
                
                Client::Transaction transaction(DB_SERIALIZABLE);
                {
                    Client::WriteContext ctx(ns(), mongo::unittest::EMPTY_STRING);
                    shared_ptr<ParsedQuery> parsedQuery
                            ( new ParsedQuery( ns(), 0, 0, 0,
                                              BSON( "$query" << query() << "$explain" << true ),
                                              fields() ) );
                    _cursor =
                    dynamic_pointer_cast<QueryOptimizerCursor>
                    ( getOptimizedCursor( ns(), query(), BSONObj(), QueryPlanSelectionPolicy ::any(),
                                                            parsedQuery, false ) );
                    // If the dynamic cast failed, then there is no optimizer cursor,
                    // there is only a single plan cursor and so nothing to test.
                    if (_cursor) {
                    
                        handleCursor();
                        
                        _explainInfo = _cursor->explainQueryInfo();
                        _explain = _explainInfo->bson();

                        checkExplain();

                    }
                }
                transaction.commit();
            }
        protected:
            virtual void setupCollection() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                _cli.insert( ns(), BSON( "a" << 1 << "b" << 1 ) );
                _cli.insert( ns(), BSON( "a" << 2 << "b" << 1 ) );
            }
            virtual BSONObj query() const { return BSON( "a" << 1 << "b" << 1 ); }
            virtual BSONObj fields() const { return BSONObj(); }
            virtual void handleCursor() {
            }
            virtual void checkExplain() {
            }
            shared_ptr<QueryOptimizerCursor> _cursor;
            shared_ptr<ExplainQueryInfo> _explainInfo;
            BSONObj _explain;
        };
        
        class Initial : public Base {
            virtual void checkExplain() {
                ASSERT( !_explain[ "cursor" ].eoo() );
                ASSERT( !_explain[ "isMultiKey" ].Bool() );
                ASSERT_EQUALS( 0, _explain[ "n" ].number() );
                ASSERT_EQUALS( 0, _explain[ "nscannedObjectsAllPlans" ].number() );
                ASSERT_EQUALS( 2, _explain[ "nscannedAllPlans" ].number() );
                ASSERT( !_explain[ "scanAndOrder" ].Bool() );
                ASSERT( !_explain[ "indexOnly" ].Bool() );
                ASSERT_EQUALS( 0, _explain[ "nChunkSkips" ].number() );
                ASSERT( !_explain[ "millis" ].eoo() );
                ASSERT( !_explain[ "indexBounds" ].eoo() );
                ASSERT_EQUALS( 2U, _explain[ "allPlans" ].Array().size() );

                BSONObj plan1 = _explain[ "allPlans" ].Array()[ 0 ].Obj();
                ASSERT_EQUALS( "IndexCursor a_1", plan1[ "cursor" ].String() );
                ASSERT_EQUALS( 0, plan1[ "n" ].number() );
                ASSERT_EQUALS( 0, plan1[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 1, plan1[ "nscanned" ].number() );
                ASSERT_EQUALS( fromjson( "{a:[[1,1]]}" ), plan1[ "indexBounds" ].Obj() );

                BSONObj plan2 = _explain[ "allPlans" ].Array()[ 1 ].Obj();
                ASSERT_EQUALS( "BasicCursor", plan2[ "cursor" ].String() );
                ASSERT_EQUALS( 0, plan2[ "n" ].number() );
                ASSERT_EQUALS( 0, plan2[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 1, plan2[ "nscanned" ].number() );
                ASSERT_EQUALS( BSONObj(), plan2[ "indexBounds" ].Obj() );
            }
        };
        
        class Empty : public Base {
            virtual void setupCollection() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
            }
            virtual void handleCursor() {
                ASSERT( !_cursor->ok() );
            }
            virtual void checkExplain() {
                ASSERT( !_explain[ "cursor" ].eoo() );
                ASSERT( !_explain[ "isMultiKey" ].Bool() );
                ASSERT_EQUALS( 0, _explain[ "n" ].number() );
                ASSERT_EQUALS( 0, _explain[ "nscannedObjectsAllPlans" ].number() );
                ASSERT_EQUALS( 0, _explain[ "nscannedAllPlans" ].number() );
                ASSERT( !_explain[ "scanAndOrder" ].Bool() );
                ASSERT( !_explain[ "indexOnly" ].Bool() );
                ASSERT_EQUALS( 0, _explain[ "nChunkSkips" ].number() );
                ASSERT( !_explain[ "millis" ].eoo() );
                ASSERT( !_explain[ "indexBounds" ].eoo() );
                ASSERT_EQUALS( 2U, _explain[ "allPlans" ].Array().size() );
                
                BSONObj plan1 = _explain[ "allPlans" ].Array()[ 0 ].Obj();
                ASSERT_EQUALS( "IndexCursor a_1", plan1[ "cursor" ].String() );
                ASSERT_EQUALS( 0, plan1[ "n" ].number() );
                ASSERT_EQUALS( 0, plan1[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 0, plan1[ "nscanned" ].number() );
                ASSERT_EQUALS( fromjson( "{a:[[1,1]]}" ), plan1[ "indexBounds" ].Obj() );
                
                BSONObj plan2 = _explain[ "allPlans" ].Array()[ 1 ].Obj();
                ASSERT_EQUALS( "BasicCursor", plan2[ "cursor" ].String() );
                ASSERT_EQUALS( 0, plan2[ "n" ].number() );
                ASSERT_EQUALS( 0, plan2[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 0, plan2[ "nscanned" ].number() );
                ASSERT_EQUALS( BSONObj(), plan2[ "indexBounds" ].Obj() );                
            }
        };
        
        class SimpleCount : public Base {
            virtual void handleCursor() {
                while( _cursor->ok() ) {
                    MatchDetails matchDetails;
                    if ( _cursor->currentMatches( &matchDetails ) &&
                        !_cursor->getsetdup( _cursor->currPK() ) ) {
                        _cursor->noteIterate( true, true, false );
                    }
                    else {
                        _cursor->noteIterate( false, matchDetails.hasLoadedRecord(), false );
                    }
                    _cursor->advance();
                }
            }
            virtual void checkExplain() {
                ASSERT_EQUALS( "IndexCursor a_1", _explain[ "cursor" ].String() );
                ASSERT_EQUALS( 1, _explain[ "n" ].number() );
                ASSERT_EQUALS( 2, _explain[ "nscannedObjectsAllPlans" ].number() );
                ASSERT_EQUALS( 2, _explain[ "nscannedAllPlans" ].number() );

                BSONObj plan1 = _explain[ "allPlans" ].Array()[ 0 ].Obj();
                ASSERT_EQUALS( 1, plan1[ "n" ].number() );
                ASSERT_EQUALS( 1, plan1[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 1, plan1[ "nscanned" ].number() );

                BSONObj plan2 = _explain[ "allPlans" ].Array()[ 1 ].Obj();
                ASSERT_EQUALS( 1, plan2[ "n" ].number() );
                ASSERT_EQUALS( 1, plan2[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 1, plan2[ "nscanned" ].number() );
            }
        };
        
        class IterateOnly : public Base {
            virtual BSONObj query() const { return BSON( "a" << GT << 0 << "b" << 1 ); }
            virtual void handleCursor() {
                while( _cursor->ok() ) {
                    _cursor->advance();
                }
            }
            virtual void checkExplain() {
                ASSERT_EQUALS( "IndexCursor a_1", _explain[ "cursor" ].String() );
                ASSERT_EQUALS( 0, _explain[ "n" ].number() ); // needs to be set with noteIterate()
                ASSERT_EQUALS( 0, _explain[ "nscannedObjectsAllPlans" ].number() );
                ASSERT_EQUALS( 3, _explain[ "nscannedAllPlans" ].number() );

                BSONObj plan1 = _explain[ "allPlans" ].Array()[ 0 ].Obj();
                ASSERT_EQUALS( 2, plan1[ "n" ].number() );
                ASSERT_EQUALS( 2, plan1[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 2, plan1[ "nscanned" ].number() );
                
                // Not fully incremented without checking for matches.
                BSONObj plan2 = _explain[ "allPlans" ].Array()[ 1 ].Obj();
                ASSERT_EQUALS( 1, plan2[ "n" ].number() );
                ASSERT_EQUALS( 1, plan2[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 1, plan2[ "nscanned" ].number() );                
            }
        };
        
        class ExtraMatchChecks : public Base {
            virtual BSONObj query() const { return BSON( "a" << GT << 0 << "b" << 1 ); }
            virtual void handleCursor() {
                while( _cursor->ok() ) {
                    _cursor->currentMatches();
                    _cursor->currentMatches();
                    _cursor->currentMatches();
                    _cursor->advance();
                }
            }
            virtual void checkExplain() {
                ASSERT_EQUALS( "IndexCursor a_1", _explain[ "cursor" ].String() );
                ASSERT_EQUALS( 0, _explain[ "n" ].number() ); // needs to be set with noteIterate()
                ASSERT_EQUALS( 0, _explain[ "nscannedObjectsAllPlans" ].number() );
                ASSERT_EQUALS( 4, _explain[ "nscannedAllPlans" ].number() );
                
                BSONObj plan1 = _explain[ "allPlans" ].Array()[ 0 ].Obj();
                ASSERT_EQUALS( 2, plan1[ "n" ].number() );
                // nscannedObjects are not deduped.
                ASSERT_EQUALS( 6, plan1[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 2, plan1[ "nscanned" ].number() );
                
                BSONObj plan2 = _explain[ "allPlans" ].Array()[ 1 ].Obj();
                ASSERT_EQUALS( 2, plan2[ "n" ].number() );
                ASSERT_EQUALS( 6, plan2[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 2, plan2[ "nscanned" ].number() );                
            }
        };
        
        class PartialIteration : public Base {
            virtual void handleCursor() {
                _cursor->currentMatches();
                _cursor->advance();
                _cursor->noteIterate( true, true, false );
            }
            virtual void checkExplain() {
                ASSERT_EQUALS( "IndexCursor a_1", _explain[ "cursor" ].String() );
                ASSERT_EQUALS( 1, _explain[ "n" ].number() );
                ASSERT_EQUALS( 1, _explain[ "nscannedObjectsAllPlans" ].number() );
                ASSERT_EQUALS( 2, _explain[ "nscannedAllPlans" ].number() );
                
                BSONObj plan1 = _explain[ "allPlans" ].Array()[ 0 ].Obj();
                ASSERT_EQUALS( 1, plan1[ "n" ].number() );
                ASSERT_EQUALS( 1, plan1[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 1, plan1[ "nscanned" ].number() );
                
                BSONObj plan2 = _explain[ "allPlans" ].Array()[ 1 ].Obj();
                ASSERT_EQUALS( 0, plan2[ "n" ].number() );
                ASSERT_EQUALS( 0, plan2[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 1, plan2[ "nscanned" ].number() );                
            }
        };
        
        class Multikey : public Base {
            virtual void setupCollection() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                _cli.insert( ns(), BSON( "a" << BSON_ARRAY( 1 << 2 ) ) );
            }
            virtual void handleCursor() {
                while( _cursor->advance() );
            }
            virtual void checkExplain() {
                ASSERT( _explain[ "isMultiKey" ].Bool() );
            }            
        };
        
        class MultikeyInitial : public Base {
            virtual void setupCollection() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                _cli.insert( ns(), BSON( "a" << BSON_ARRAY( 1 << 2 ) ) );
            }
            virtual void checkExplain() {
                ASSERT( _explain[ "isMultiKey" ].Bool() );
            }
        };

        class Count : public Base {
            virtual void setupCollection() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                for( int i = 0; i < 5; ++i ) {
                    _cli.insert( ns(), BSON( "a" << 1 << "b" << 1 ) );
                }
            }
            virtual void handleCursor() {
                while( _cursor->ok() ) {
                    MatchDetails matchDetails;
                    if ( _cursor->currentMatches( &matchDetails ) &&
                        !_cursor->getsetdup( _cursor->currPK() ) ) {
                        _cursor->noteIterate( true, true, false );
                    }
                    else {
                        _cursor->noteIterate( false, matchDetails.hasLoadedRecord(), false );
                    }
                    _cursor->advance();
                }
            }
            virtual void checkExplain() {
                ASSERT_EQUALS( "IndexCursor a_1", _explain[ "cursor" ].String() );
                ASSERT_EQUALS( 5, _explain[ "n" ].number() );
                ASSERT_EQUALS( 10, _explain[ "nscannedObjectsAllPlans" ].number() );
                ASSERT_EQUALS( 10, _explain[ "nscannedAllPlans" ].number() );
                
                BSONObj plan1 = _explain[ "allPlans" ].Array()[ 0 ].Obj();
                ASSERT_EQUALS( 5, plan1[ "n" ].number() );
                ASSERT_EQUALS( 5, plan1[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 5, plan1[ "nscanned" ].number() );
                
                BSONObj plan2 = _explain[ "allPlans" ].Array()[ 1 ].Obj();
                ASSERT_EQUALS( 5, plan2[ "n" ].number() );
                ASSERT_EQUALS( 5, plan2[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 5, plan2[ "nscanned" ].number() );                
            }
        };
        
        class MultipleClauses : public Count {
            virtual void setupCollection() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
                for( int i = 0; i < 4; ++i ) {
                    _cli.insert( ns(), BSON( "a" << 1 << "b" << 1 ) );
                }
                _cli.insert( ns(), BSON( "a" << 0 << "b" << 1 ) );
            }
            virtual BSONObj query() const { return fromjson( "{$or:[{a:1,c:null},{b:1,c:null}]}"); }
            virtual void checkExplain() {

                ASSERT_EQUALS( 5, _explain[ "n" ].number() );
                ASSERT_EQUALS( 9, _explain[ "nscannedObjectsAllPlans" ].number() );
                ASSERT_EQUALS( 9, _explain[ "nscannedAllPlans" ].number() );

                BSONObj clause1 = _explain[ "clauses" ].Array()[ 0 ].Obj();
                ASSERT_EQUALS( "IndexCursor a_1", clause1[ "cursor" ].String() );
                ASSERT_EQUALS( 4, clause1[ "n" ].number() );
                ASSERT_EQUALS( 4, clause1[ "nscannedObjectsAllPlans" ].number() );
                ASSERT_EQUALS( 4, clause1[ "nscannedAllPlans" ].number() );
                
                BSONObj c1plan1 = clause1[ "allPlans" ].Array()[ 0 ].Obj();
                ASSERT_EQUALS( "IndexCursor a_1", c1plan1[ "cursor" ].String() );
                ASSERT_EQUALS( 4, c1plan1[ "n" ].number() );
                ASSERT_EQUALS( 4, c1plan1[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 4, c1plan1[ "nscanned" ].number() );

                BSONObj clause2 = _explain[ "clauses" ].Array()[ 1 ].Obj();
                ASSERT_EQUALS( "IndexCursor b_1", clause2[ "cursor" ].String() );
                ASSERT_EQUALS( 1, clause2[ "n" ].number() );
                ASSERT_EQUALS( 5, clause2[ "nscannedObjectsAllPlans" ].number() );
                ASSERT_EQUALS( 5, clause2[ "nscannedAllPlans" ].number() );

                BSONObj c2plan1 = clause2[ "allPlans" ].Array()[ 0 ].Obj();
                ASSERT_EQUALS( "IndexCursor b_1", c2plan1[ "cursor" ].String() );
                ASSERT_EQUALS( 5, c2plan1[ "n" ].number() );
                ASSERT_EQUALS( 5, c2plan1[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 5, c2plan1[ "nscanned" ].number() );
            }
        };
        
        class MultiCursorTakeover : public Count {
            virtual void setupCollection() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                for( int i = 20; i >= 1; --i ) {
                    for( int j = 0; j < i; ++j ) {
                        _cli.insert( ns(), BSON( "a" << i ) );
                    }
                }
            }
            virtual BSONObj query() const {
                BSONArrayBuilder bab;
                for( int i = 20; i >= 1; --i ) {
                    bab << BSON( "a" << i );
                }
                return BSON( "$or" << bab.arr() );
            }
            virtual void checkExplain() {
                ASSERT_EQUALS( 20U, _explain[ "clauses" ].Array().size() );
                for( int i = 20; i >= 1; --i ) {
                    BSONObj clause = _explain[ "clauses" ].Array()[ 20-i ].Obj();
                    ASSERT_EQUALS( "IndexCursor a_1", clause[ "cursor" ].String() );
                    ASSERT_EQUALS( BSON( "a" << BSON_ARRAY( BSON_ARRAY( i << i ) ) ),
                                  clause[ "indexBounds" ].Obj() );
                    ASSERT_EQUALS( i, clause[ "n" ].number() );
                    ASSERT_EQUALS( i, clause[ "nscannedObjects" ].number() );
                    ASSERT_EQUALS( i, clause[ "nscanned" ].number() );
                    
                    ASSERT_EQUALS( 1U, clause[ "allPlans" ].Array().size() );
                    BSONObj plan = clause[ "allPlans" ].Array()[ 0 ].Obj();
                    ASSERT_EQUALS( i, plan[ "n" ].number() );
                    ASSERT_EQUALS( i, plan[ "nscannedObjects" ].number() );
                    ASSERT_EQUALS( i, plan[ "nscanned" ].number() );                    
                }
                
                ASSERT_EQUALS( 210, _explain[ "n" ].number() );
                ASSERT_EQUALS( 210, _explain[ "nscannedObjects" ].number() );
                ASSERT_EQUALS( 210, _explain[ "nscanned" ].number() );
            }
        };
        
        class NChunkSkipsTakeover : public Base {
            virtual void setupCollection() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                for( int i = 0; i < 200; ++i ) {
                    _cli.insert( ns(), BSON( "a" << 1 << "b" << 1 ) );
                }
                for( int i = 0; i < 200; ++i ) {
                    _cli.insert( ns(), BSON( "a" << 2 << "b" << 2 ) );
                }
            }
            virtual BSONObj query() const { return fromjson( "{$or:[{a:1,b:1},{a:2,b:2}]}" ); }
            virtual void handleCursor() {
                ASSERT_EQUALS( "QueryOptimizerCursor", _cursor->toString() );
                int i = 0;
                while( _cursor->ok() ) {
                    if ( _cursor->currentMatches() && !_cursor->getsetdup( _cursor->currPK() ) ) {
                        _cursor->noteIterate( true, true, i++ %2 == 0 );
                    }
                    _cursor->advance();
                }
            }
            virtual void checkExplain() {
                // Historically, nChunkSkips has been excluded from the query summary.
                ASSERT( _explain[ "nChunkSkips" ].eoo() );

                BSONObj clause0 = _explain[ "clauses" ].Array()[ 0 ].Obj();
                ASSERT_EQUALS( 100, clause0[ "nChunkSkips" ].number() );
                BSONObj plan0 = clause0[ "allPlans" ].Array()[ 0 ].Obj();
                ASSERT( plan0[ "nChunkSkips" ].eoo() );

                BSONObj clause1 = _explain[ "clauses" ].Array()[ 1 ].Obj();
                ASSERT_EQUALS( 100, clause1[ "nChunkSkips" ].number() );
                BSONObj plan1 = clause1[ "allPlans" ].Array()[ 0 ].Obj();
                ASSERT( plan1[ "nChunkSkips" ].eoo() );
            }            
        };

        class CoveredIndex : public Base {
            virtual BSONObj query() const { return fromjson( "{$or:[{a:1},{a:2}]}" ); }
            virtual BSONObj fields() const { return BSON( "_id" << 0 << "a" << 1 ); }
            virtual void handleCursor() {
                ASSERT_EQUALS( "QueryOptimizerCursor", _cursor->toString() );
                while( _cursor->advance() );
            }
            virtual void checkExplain() {
                BSONObj clause0 = _explain[ "clauses" ].Array()[ 0 ].Obj();
                ASSERT( clause0[ "indexOnly" ].Bool() );
                
                BSONObj clause1 = _explain[ "clauses" ].Array()[ 1 ].Obj();
                ASSERT( clause1[ "indexOnly" ].Bool() );
            }
        };

        class CoveredIndexTakeover : public Base {
            virtual void setupCollection() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                for( int i = 0; i < 150; ++i ) {
                    _cli.insert( ns(), BSON( "a" << 1 ) );
                }
                _cli.insert( ns(), BSON( "a" << 2 ) );
            }
            virtual BSONObj query() const { return fromjson( "{$or:[{a:1},{a:2}]}" ); }
            virtual BSONObj fields() const { return BSON( "_id" << 0 << "a" << 1 ); }
            virtual void handleCursor() {
                ASSERT_EQUALS( "QueryOptimizerCursor", _cursor->toString() );
                while( _cursor->advance() );
            }
            virtual void checkExplain() {
                BSONObj clause0 = _explain[ "clauses" ].Array()[ 0 ].Obj();
                ASSERT( clause0[ "indexOnly" ].Bool() );
                
                BSONObj clause1 = _explain[ "clauses" ].Array()[ 1 ].Obj();
                ASSERT( clause1[ "indexOnly" ].Bool() );
            }
        };
        
        /**
         * Check that the plan with the most matches is reported at the top of the explain output
         * in the absence of a done or picked plan.
         */
        class VirtualPickedPlan : public Base {
        public:
            void run() {
                _cli.ensureIndex( ns(), BSON( "a" << 1 ) );
                _cli.ensureIndex( ns(), BSON( "b" << 1 ) );
                _cli.ensureIndex( ns(), BSON( "c" << 1 ) );
                
                Client::Transaction transaction(DB_SERIALIZABLE);
                Client::WriteContext ctx(ns(), mongo::unittest::EMPTY_STRING);

                shared_ptr<Cursor> aCursor
                ( getOptimizedCursor( ns(), BSON( "a" << 1 ) ) );
                shared_ptr<Cursor> bCursor
                ( getOptimizedCursor( ns(), BSON( "b" << 1 ) ) );
                shared_ptr<Cursor> cCursor
                ( getOptimizedCursor( ns(), BSON( "c" << 1 ) ) );
                
                shared_ptr<ExplainPlanInfo> aPlan( new ExplainPlanInfo() );
                aPlan->notePlan( *aCursor, false, false );
                shared_ptr<ExplainPlanInfo> bPlan( new ExplainPlanInfo() );
                bPlan->notePlan( *bCursor, false, false );
                shared_ptr<ExplainPlanInfo> cPlan( new ExplainPlanInfo() );
                cPlan->notePlan( *cCursor, false, false );
                
                aPlan->noteIterate( true, false, *aCursor ); // one match a
                bPlan->noteIterate( true, false, *bCursor ); // two matches b
                bPlan->noteIterate( true, false, *bCursor );
                cPlan->noteIterate( true, false, *cCursor ); // one match c
                
                shared_ptr<ExplainClauseInfo> clause( new ExplainClauseInfo() );
                clause->addPlanInfo( aPlan );
                clause->addPlanInfo( bPlan );
                clause->addPlanInfo( cPlan );
                
                ASSERT_EQUALS( "IndexCursor b_1", clause->bson()[ "cursor" ].String() );
                transaction.commit();
            }
        };

        /** Simple check that an explain result can contain a large value of n. */
        class LargeN : public Base {
        public:
            void run() {
                Client::Transaction transaction(DB_SERIALIZABLE);
                Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);

                Client::Context ctx( ns() );
                
                shared_ptr<Cursor> cursor
                ( getOptimizedCursor( ns(), BSONObj() ) );
                ExplainSinglePlanQueryInfo explainHelper;
                explainHelper.notePlan( *cursor, false, false );
                explainHelper.noteIterate( false, false, false, *cursor );

                shared_ptr<ExplainQueryInfo> explain = explainHelper.queryInfo();
                explain->reviseN( 3000000000000LL );

                // large n could be stored either in bson as long or double
                // retrieve value using safeNumberLong() so that we don't have to guess
                // internal type
                BSONObj obj = explain->bson();
                BSONElement e  = obj.getField( "n" );
                long long n = e.safeNumberLong();
                ASSERT_EQUALS( 3000000000000LL, n );
                transaction.commit();
            }
        };

    } // namespace Explain
    
    class All : public Suite {
    public:
        All() : Suite( "queryoptimizercursor" ) {}
        
        void setupTests() {
            add<CachedMatchCounter::Count>();
            add<CachedMatchCounter::Accumulate>();
            add<CachedMatchCounter::Dedup>();
            add<CachedMatchCounter::Nscanned>();
            add<SmallDupSet::Upgrade>();
            add<SmallDupSet::UpgradeRead>();
            add<SmallDupSet::UpgradeWrite>();
            add<DurationTimerStop>();
            add<Empty>();
            add<Unindexed>();
            add<Basic>();
            add<NoMatch>();
            add<Interleaved>();
            add<NotMatch>();
            add<StopInterleaving>();
            add<TakeoverWithDup>();
            add<TakeoverWithNonMatches>();
            add<TakeoverWithTakeoverDup>();
            add<BasicOr>();
            add<OrFirstClauseEmpty>();
            add<OrSecondClauseEmpty>();
            add<OrMultipleClausesEmpty>();
            add<TakeoverCountOr>();
            add<TakeoverEndOfOrClause>();
            add<TakeoverBeforeEndOfOrClause>();
            add<TakeoverAfterEndOfOrClause>();
            add<ManualMatchingDeduping>();
            add<ManualMatchingUsingCurrKey>();
            add<ManualMatchingDedupingTakeover>();
            add<Singlekey>();
            add<Multikey>();
            add<AddOtherPlans>();
            add<AddOtherPlansContinuousDelete>();
            add<OrRangeElimination>();
            add<OrDedup>();
            add<EarlyDups>();
            add<OrPopInTakeover>();
            add<OrderId>();
            add<OrderMultiIndex>();
            add<OrderReject>();
            add<OrderNatural>();
            add<OrderUnindexed>();
            add<RecordedOrderInvalid>();
            add<KillOp>();
            add<KillOpFirstClause>();
            add<ClientCursor::Invalidate>();
            add<ClientCursor::Timeout>();
            add<ClientCursor::Drop>();
            add<AllowOutOfOrderPlan>();
            add<NoTakeoverByOutOfOrderPlan>();
            add<OutOfOrderOnlyTakeover>();
            add<CoveredIndex>();
            add<CoveredIndexTakeover>();
            add<SaveGoodIndex>();
            add<PossibleBothPlans>();
            add<AbortOutOfOrderPlansBeforeAddOtherPlans>();
            add<TakeoverOrRangeElimination>();
            add<TakeoverOrDedups>();
            add<TakeoverOrDifferentIndex>();
            add<ElemMatchKey>();
            add<GetCursor::NoConstraints>();
            add<GetCursor::SimpleId>();
            add<GetCursor::OptimalIndex>();
            add<GetCursor::SimpleKeyMatch>();
            // TokuMX: no geo
            //add<GetCursor::Geo>();
            //add<GetCursor::GeoNumWanted>();
            add<GetCursor::PreventOutOfOrderPlan>();
            add<GetCursor::AllowOutOfOrderPlan>();
            add<GetCursor::BestSavedOutOfOrder>();
            add<GetCursor::BestSavedOptimal>();
            add<GetCursor::BestSavedNotOptimal>();
            add<GetCursor::MultiIndex>();
            add<GetCursor::Hint>();
            add<GetCursor::SnapshotOptionNoLongerNecessary>();
            add<GetCursor::Min>();
            add<GetCursor::Max>();
            add<GetCursor::RequireIndex::NoConstraints>();
            add<GetCursor::RequireIndex::SimpleId>();
            add<GetCursor::RequireIndex::UnindexedQuery>();
            add<GetCursor::RequireIndex::IndexedQuery>();
            add<GetCursor::RequireIndex::SecondOrClauseIndexed>();
            add<GetCursor::RequireIndex::SecondOrClauseUnindexed>();
            add<GetCursor::RequireIndex::SecondOrClauseUnindexedUndetected>();
            // There's no more $atomic operator, so this test isn't useful anymore.
            //add<GetCursor::MatcherValidation>(); 
            add<GetCursor::MatcherSet>();
            add<GetCursor::MatcherValidate>();
            add<Explain::ClearRecordedIndex>();
            add<Explain::Initial>();
            add<Explain::Empty>();
            add<Explain::SimpleCount>();
            add<Explain::IterateOnly>();
            add<Explain::ExtraMatchChecks>();
            add<Explain::PartialIteration>();
            add<Explain::Multikey>();
            add<Explain::MultikeyInitial>();
            // Cursors should never "become" multikey, because of transaction isolation.
            //add<Explain::BecomesMultikey>();
            add<Explain::Count>();
            add<Explain::MultipleClauses>();
            add<Explain::MultiCursorTakeover>();
            add<Explain::NChunkSkipsTakeover>();
            add<Explain::CoveredIndex>();
            add<Explain::CoveredIndexTakeover>();
            add<Explain::VirtualPickedPlan>();
            add<Explain::LargeN>();
        }
    } myall;
    
} // namespace QueryOptimizerTests

