// matchertests.cpp : matcher unit tests
//

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
 */

#include "mongo/pch.h"

#include "mongo/db/cursor.h"
#include "mongo/db/json.h"
#include "mongo/db/matcher.h"
#include "mongo/db/matcher/matcher.h"
#include "mongo/db/collection.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/dbtests/dbtests.h"
#include "mongo/util/timer.h"

namespace MatcherTests {

    class CollectionBase {
    public:
        CollectionBase() :
        _ns( "unittests.matchertests" ) {
        }
        virtual ~CollectionBase() {
            client().dropCollection( ns() );
        }
    protected:
        const char * const ns() const { return _ns; }
        DBDirectClient &client() { return _client; }
    private:
        const char * const _ns;
        DBDirectClient _client;
    };

    template <typename M>
    class Basic {
    public:
        void run() {
            BSONObj query = fromjson( "{\"a\":\"b\"}" );
            M m( query );
            ASSERT( m.matches( fromjson( "{\"a\":\"b\"}" ) ) );
        }
    };

    template <typename M>
    class DoubleEqual {
    public:
        void run() {
            BSONObj query = fromjson( "{\"a\":5}" );
            M m( query );
            ASSERT( m.matches( fromjson( "{\"a\":5}" ) ) );
        }
    };

    template <typename M>
    class MixedNumericEqual {
    public:
        void run() {
            BSONObjBuilder query;
            query.append( "a", 5 );
            M m( query.done() );
            ASSERT( m.matches( fromjson( "{\"a\":5}" ) ) );
        }
    };

    template <typename M>
    class MixedNumericGt {
    public:
        void run() {
            BSONObj query = fromjson( "{\"a\":{\"$gt\":4}}" );
            M m( query );
            BSONObjBuilder b;
            b.append( "a", 5 );
            ASSERT( m.matches( b.done() ) );
        }
    };

    template <typename M>
    class MixedNumericIN {
    public:
        void run() {
            BSONObj query = fromjson( "{ a : { $in : [4,6] } }" );
            ASSERT_EQUALS( 4 , query["a"].embeddedObject()["$in"].embeddedObject()["0"].number() );
            ASSERT_EQUALS( NumberInt , query["a"].embeddedObject()["$in"].embeddedObject()["0"].type() );

            M m( query );

            {
                BSONObjBuilder b;
                b.append( "a" , 4.0 );
                ASSERT( m.matches( b.done() ) );
            }

            {
                BSONObjBuilder b;
                b.append( "a" , 5 );
                ASSERT( ! m.matches( b.done() ) );
            }


            {
                BSONObjBuilder b;
                b.append( "a" , 4 );
                ASSERT( m.matches( b.done() ) );
            }

        }
    };

    template <typename M>
    class MixedNumericEmbedded {
    public:
        void run() {
            M m( BSON( "a" << BSON( "x" << 1 ) ) );
            ASSERT( m.matches( BSON( "a" << BSON( "x" << 1 ) ) ) );
            ASSERT( m.matches( BSON( "a" << BSON( "x" << 1.0 ) ) ) );
        }
    };

    template <typename M>
    class Size {
    public:
        void run() {
            M m( fromjson( "{a:{$size:4}}" ) );
            ASSERT( m.matches( fromjson( "{a:[1,2,3,4]}" ) ) );
            ASSERT( !m.matches( fromjson( "{a:[1,2,3]}" ) ) );
            ASSERT( !m.matches( fromjson( "{a:[1,2,3,'a','b']}" ) ) );
            ASSERT( !m.matches( fromjson( "{a:[[1,2,3,4]]}" ) ) );
        }
    };

    /** Test that MatchDetails::elemMatchKey() is set correctly after a match. */
    template <typename M>
    class ElemMatchKey {
    public:
        void run() {
            M matcher( BSON( "a.b" << 1 ) );
            MatchDetails details;
            details.requestElemMatchKey();
            ASSERT( !details.hasElemMatchKey() );
            ASSERT( matcher.matches( fromjson( "{ a:[ { b:1 } ] }" ), &details ) );
            // The '0' entry of the 'a' array is matched.
            ASSERT( details.hasElemMatchKey() );
            ASSERT_EQUALS( string( "0" ), details.elemMatchKey() );
        }
    };

    template <typename M>
    class WhereSimple1 {
    public:
        void run() {
            Client::ReadContext ctx( "unittests.matchertests" );
            M m( BSON( "$where" << "function(){ return this.a == 1; }" ) );
            ASSERT( m.matches( BSON( "a" << 1 ) ) );
            ASSERT( !m.matches( BSON( "a" << 2 ) ) );
        }
    };

    namespace Covered { // Tests for CoveredIndexMatcher.
    
        /**
         * Test that MatchDetails::elemMatchKey() is set correctly after an unindexed cursor match.
         */
        class ElemMatchKeyUnindexed : public CollectionBase {
        public:
            void run() {
                client().insert( ns(), fromjson( "{ a:[ {}, { b:1 } ] }" ) );
                
                Client::Transaction transaction(DB_SERIALIZABLE);
                Client::ReadContext context( ns(), mongo::unittest::EMPTY_STRING );

                CoveredIndexMatcher matcher( BSON( "a.b" << 1 ), BSON( "$natural" << 1 ) );
                MatchDetails details;
                details.requestElemMatchKey();
                boost::shared_ptr<Cursor> cursor = getOptimizedCursor( ns(), BSONObj() );
                // Verify that the cursor is unindexed.
                ASSERT_EQUALS( "BasicCursor", cursor->toString() );
                ASSERT( matcher.matchesCurrent( cursor.get(), &details ) );
                // The '1' entry of the 'a' array is matched.
                ASSERT( details.hasElemMatchKey() );
                ASSERT_EQUALS( string( "1" ), details.elemMatchKey() );
                transaction.commit();
            }
        };
        
        /**
         * Test that MatchDetails::elemMatchKey() is set correctly after an indexed cursor match.
         */
        class ElemMatchKeyIndexed : public CollectionBase {
        public:
            void run() {
                client().ensureIndex( ns(), BSON( "a.b" << 1 ) );
                client().insert( ns(), fromjson( "{ a:[ {}, { b:9 }, { b:1 } ] }" ) );
                
                Client::Transaction transaction(DB_SERIALIZABLE);
                Client::ReadContext context( ns(), mongo::unittest::EMPTY_STRING );
                
                BSONObj query = BSON( "a.b" << 1 );
                CoveredIndexMatcher matcher( query, BSON( "a.b" << 1 ) );
                MatchDetails details;
                details.requestElemMatchKey();
                boost::shared_ptr<Cursor> cursor = getOptimizedCursor( ns(), query );
                // Verify that the cursor is indexed.
                ASSERT_EQUALS( "IndexCursor a.b_1", cursor->toString() );
                ASSERT( matcher.matchesCurrent( cursor.get(), &details ) );
                // The '2' entry of the 'a' array is matched.
                ASSERT( details.hasElemMatchKey() );
                ASSERT_EQUALS( string( "2" ), details.elemMatchKey() );
                transaction.commit();
            }
        };
        
        /**
         * Test that MatchDetails::elemMatchKey() is set correctly after an indexed cursor match
         * on a non multikey index.
         */
        class ElemMatchKeyIndexedSingleKey : public CollectionBase {
        public:
            void run() {
                client().ensureIndex( ns(), BSON( "a.b" << 1 ) );
                client().insert( ns(), fromjson( "{ a:[ { b:1 } ] }" ) );
                
                Client::Transaction transaction(DB_SERIALIZABLE);
                Client::ReadContext context( ns(), mongo::unittest::EMPTY_STRING );
                
                BSONObj query = BSON( "a.b" << 1 );
                CoveredIndexMatcher matcher( query, BSON( "a.b" << 1 ) );
                MatchDetails details;
                details.requestElemMatchKey();
                boost::shared_ptr<Cursor> cursor = getOptimizedCursor( ns(), query );
                // Verify that the cursor is indexed.
                ASSERT_EQUALS( "IndexCursor a.b_1", cursor->toString() );
                // Verify that the cursor is not multikey.
                ASSERT( !cursor->isMultiKey() );
                ASSERT( matcher.matchesCurrent( cursor.get(), &details ) );
                // The '0' entry of the 'a' array is matched.
                ASSERT( details.hasElemMatchKey() );
                ASSERT_EQUALS( string( "0" ), details.elemMatchKey() );
                transaction.commit();
            }
        };

    } // namespace Covered


    template< typename M >
    class TimingBase {
    public:
        long dotime( const BSONObj& patt , const BSONObj& obj ) {
            M m( patt );
            Timer t;
            for ( int i=0; i<900000; i++ ) {
                if ( !m.matches( obj ) ) {
                    ASSERT( 0 );
                }
            }
            return t.millis();
        }
    };

    template< typename M >
    class AllTiming : public TimingBase<M> {
    public:
        void run() {
            long normal = TimingBase<M>::dotime( BSON( "x" << 5 ),
                                                 BSON( "x" << 5 ) );

            long all = TimingBase<M>::dotime( BSON( "x" << BSON( "$all" << BSON_ARRAY( 5 ) ) ),
                                              BSON( "x" << 5 ) );

            cout << "AllTiming " << demangleName(typeid(M))
                 << " normal: " << normal << " all: " << all << endl;
        }
    };


    class All : public Suite {
    public:
        All() : Suite( "matcher" ) {
        }

#define ADD_BOTH(TEST) \
        add< TEST<Matcher2> >();

        void setupTests() {
            ADD_BOTH(Basic);
            ADD_BOTH(DoubleEqual);
            ADD_BOTH(MixedNumericEqual);
            ADD_BOTH(MixedNumericGt);
            ADD_BOTH(MixedNumericIN);
            ADD_BOTH(Size);
            ADD_BOTH(MixedNumericEmbedded);
            ADD_BOTH(ElemMatchKey);
            ADD_BOTH(WhereSimple1);
            add<Covered::ElemMatchKeyUnindexed>();
            add<Covered::ElemMatchKeyIndexed>();
            add<Covered::ElemMatchKeyIndexedSingleKey>();
            ADD_BOTH(AllTiming);
            ADD_BOTH(AtomicMatchTest);
            ADD_BOTH(SingleSimpleCriterion);
            ADD_BOTH(IndexPortion1);
            ADD_BOTH(ExistsFalse1);
        }
    } dball;

} // namespace MatcherTests

