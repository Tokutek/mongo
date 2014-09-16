// cusrortests.cpp // cursor related unit tests
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

#include "mongo/db/cursor.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/instance.h"
#include "mongo/db/json.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/dbtests/dbtests.h"

namespace CursorTests {

    using boost::shared_ptr;

    namespace IndexCursor {

        using mongo::IndexCursor;

        // The ranges expressed in these tests are impossible given our query
        // syntax, so going to do them a hacky way.

        class Base {
        protected:
            static const char *ns() { return "unittests.cursortests.Base"; }
            FieldRangeVector *vec( int *vals, int len, int direction = 1 ) {
                FieldRangeSet s( "", BSON( "a" << 1 ), true, true );
                for( int i = 0; i < len; i += 2 ) {
                    _objs.push_back( BSON( "a" << BSON( "$gte" << vals[ i ] << "$lte" << vals[ i + 1 ] ) ) );
                    FieldRangeSet s2( "", _objs.back(), true, true );
                    if ( i == 0 ) {
                        s.range( "a" ) = s2.range( "a" );
                    }
                    else {
                        s.range( "a" ) |= s2.range( "a" );
                    }
                }
                return new FieldRangeVector( s, BSON( "a" << 1 ), shared_ptr<KeyGenerator>(), direction );
            }
            DBDirectClient _c;
        private:
            vector< BSONObj > _objs;
        };

        class MultiRangeForward : public Base {
        public:
            void run() {
                const char *ns = "unittests.cursortests.IndexCursorTests.MultiRange";
                {
                    DBDirectClient c;
                    for( int i = 0; i < 10; ++i )
                        c.insert( ns, BSON( "a" << i ) );
                    ASSERT( c.ensureIndex( ns, BSON( "a" << 1 ) ) );
                }
                int v[] = { 1, 2, 4, 6 };
                boost::shared_ptr< FieldRangeVector > frv( vec( v, 4 ) );
                Client::Transaction transaction(DB_SERIALIZABLE);
                Client::WriteContext tc(ns, mongo::unittest::EMPTY_STRING);
                {
                    shared_ptr<Cursor> _c( Cursor::make( getCollection( ns ), getCollection( ns )->idx(1), frv, 0, 1 ) );
                    Cursor &c = *_c.get();
                    ASSERT_EQUALS( "IndexCursor a_1 multi", c.toString() );
                    double expected[] = { 1, 2, 4, 5, 6 };
                    for( int i = 0; i < 5; ++i ) {
                        ASSERT( c.ok() );
                        ASSERT_EQUALS( expected[ i ], c.currKey().firstElement().number() );
                        c.advance();
                    }
                    ASSERT( !c.ok() );
                }
                transaction.commit();
            }
        };

        class MultiRangeGap : public Base {
        public:
            void run() {
                const char *ns = "unittests.cursortests.IndexCursorTests.MultiRangeGap";
                {
                    DBDirectClient c;
                    for( int i = 0; i < 10; ++i )
                        c.insert( ns, BSON( "a" << i ) );
                    for( int i = 100; i < 110; ++i )
                        c.insert( ns, BSON( "a" << i ) );
                    ASSERT( c.ensureIndex( ns, BSON( "a" << 1 ) ) );
                }
                int v[] = { -50, 2, 40, 60, 109, 200 };
                boost::shared_ptr< FieldRangeVector > frv( vec( v, 6 ) );
                Client::Transaction transaction(DB_SERIALIZABLE);
                Client::WriteContext tc(ns, mongo::unittest::EMPTY_STRING);
                {
                    shared_ptr<Cursor> _c( Cursor::make(getCollection( ns ), getCollection( ns )->idx(1), frv, 0, 1 ) );
                    Cursor &c = *_c.get();
                    ASSERT_EQUALS( "IndexCursor a_1 multi", c.toString() );
                    double expected[] = { 0, 1, 2, 109 };
                    for( int i = 0; i < 4; ++i ) {
                        ASSERT( c.ok() );
                        ASSERT_EQUALS( expected[ i ], c.currKey().firstElement().number() );
                        c.advance();
                    }
                    ASSERT( !c.ok() );
                }
                transaction.commit();
            }
        };

        class MultiRangeReverse : public Base {
        public:
            void run() {
                const char *ns = "unittests.cursortests.IndexCursorTests.MultiRangeReverse";
                {
                    DBDirectClient c;
                    for( int i = 0; i < 10; ++i )
                        c.insert( ns, BSON( "a" << i ) );
                    ASSERT( c.ensureIndex( ns, BSON( "a" << 1 ) ) );
                }
                int v[] = { 1, 2, 4, 6 };
                boost::shared_ptr< FieldRangeVector > frv( vec( v, 4, -1 ) );
                Client::Transaction transaction(DB_SERIALIZABLE);
                Client::WriteContext ctx( ns , mongo::unittest::EMPTY_STRING );
                {
                    shared_ptr<Cursor> _c( Cursor::make( getCollection( ns ), getCollection( ns )->idx(1), frv, 0, -1 ) );
                    Cursor& c = *_c.get();
                    ASSERT_EQUALS( "IndexCursor a_1 reverse multi", c.toString() );
                    double expected[] = { 6, 5, 4, 2, 1 };
                    for( int i = 0; i < 5; ++i ) {
                        ASSERT( c.ok() );
                        ASSERT_EQUALS( expected[ i ], c.currKey().firstElement().number() );
                        c.advance();
                    }
                    ASSERT( !c.ok() );
                }
                transaction.commit();
            }
        };

        class Base2 {
        public:
            virtual ~Base2() { _c.dropCollection( ns() ); }
        protected:
            static const char *ns() { return "unittests.cursortests.Base2"; }
            DBDirectClient _c;
            virtual BSONObj idx() const = 0;
            virtual int direction() const { return 1; }
            void insert( const BSONObj &o ) {
                _objs.push_back( o );
                _c.insert( ns(), o );
            }
            void check( const BSONObj &spec ) {
                {
                    BSONObj keypat = idx();
                    //cout << keypat.toString() << endl;
                    _c.ensureIndex( ns(), idx() );
                }

                Client::Transaction transaction(DB_SERIALIZABLE);
                Client::WriteContext ctx( ns(), mongo::unittest::EMPTY_STRING );
                FieldRangeSet frs( ns(), spec, true, true );
                boost::shared_ptr< FieldRangeVector > frv( new FieldRangeVector( frs, idx(), shared_ptr<KeyGenerator>(), direction() ) );
                {
                    Collection *d = getCollection(ns());
                    int i = d->findIndexByKeyPattern(idx());
                    verify(i >= 0);
                    shared_ptr<Cursor> c( Cursor::make( d, d->idx( i ), frv, 0, direction() ) );
                    Matcher m( spec );
                    int count = 0;
                    while( c->ok() ) {
                        ASSERT( m.matches( c->current() ) );
                        c->advance();
                        ++count;
                    }
                    int expectedCount = 0;
                    for( vector< BSONObj >::const_iterator i = _objs.begin(); i != _objs.end(); ++i ) {
                        if ( m.matches( *i ) ) {
                            ++expectedCount;
                        }
                    }
                    ASSERT_EQUALS( expectedCount, count );
                }
                transaction.commit();
            }
        private:
            vector< BSONObj > _objs;
        };

        class EqEq : public Base2 {
        public:
            void run() {
                insert( BSON( "a" << 4 << "b" << 5 ) );
                insert( BSON( "a" << 4 << "b" << 5 ) );
                insert( BSON( "a" << 4 << "b" << 4 ) );
                insert( BSON( "a" << 5 << "b" << 4 ) );
                check( BSON( "a" << 4 << "b" << 5 ) );
            }
            virtual BSONObj idx() const { return BSON( "a" << 1 << "b" << 1 ); }
        };

        class EqRange : public Base2 {
        public:
            void run() {
                insert( BSON( "a" << 3 << "b" << 5 ) );
                insert( BSON( "a" << 4 << "b" << 0 ) );
                insert( BSON( "a" << 4 << "b" << 5 ) );
                insert( BSON( "a" << 4 << "b" << 6 ) );
                insert( BSON( "a" << 4 << "b" << 6 ) );
                insert( BSON( "a" << 4 << "b" << 10 ) );
                insert( BSON( "a" << 4 << "b" << 11 ) );
                insert( BSON( "a" << 5 << "b" << 5 ) );
                check( BSON( "a" << 4 << "b" << BSON( "$gte" << 1 << "$lte" << 10 ) ) );
            }
            virtual BSONObj idx() const { return BSON( "a" << 1 << "b" << 1 ); }
        };

        class EqIn : public Base2 {
        public:
            void run() {
                insert( BSON( "a" << 3 << "b" << 5 ) );
                insert( BSON( "a" << 4 << "b" << 0 ) );
                insert( BSON( "a" << 4 << "b" << 5 ) );
                insert( BSON( "a" << 4 << "b" << 6 ) );
                insert( BSON( "a" << 4 << "b" << 6 ) );
                insert( BSON( "a" << 4 << "b" << 10 ) );
                insert( BSON( "a" << 4 << "b" << 11 ) );
                insert( BSON( "a" << 5 << "b" << 5 ) );
                check( BSON( "a" << 4 << "b" << BSON( "$in" << BSON_ARRAY( 5 << 6 << 11 ) ) ) );
            }
            virtual BSONObj idx() const { return BSON( "a" << 1 << "b" << 1 ); }
        };

        class RangeEq : public Base2 {
        public:
            void run() {
                insert( BSON( "a" << 0 << "b" << 4 ) );
                insert( BSON( "a" << 1 << "b" << 4 ) );
                insert( BSON( "a" << 4 << "b" << 3 ) );
                insert( BSON( "a" << 5 << "b" << 4 ) );
                insert( BSON( "a" << 7 << "b" << 4 ) );
                insert( BSON( "a" << 4 << "b" << 4 ) );
                insert( BSON( "a" << 9 << "b" << 6 ) );
                insert( BSON( "a" << 11 << "b" << 1 ) );
                insert( BSON( "a" << 11 << "b" << 4 ) );
                check( BSON( "a" << BSON( "$gte" << 1 << "$lte" << 10 ) << "b" << 4 ) );
            }
            virtual BSONObj idx() const { return BSON( "a" << 1 << "b" << 1 ); }
        };

        class RangeIn : public Base2 {
        public:
            void run() {
                insert( BSON( "a" << 0 << "b" << 4 ) );
                insert( BSON( "a" << 1 << "b" << 5 ) );
                insert( BSON( "a" << 4 << "b" << 3 ) );
                insert( BSON( "a" << 5 << "b" << 4 ) );
                insert( BSON( "a" << 7 << "b" << 5 ) );
                insert( BSON( "a" << 4 << "b" << 4 ) );
                insert( BSON( "a" << 9 << "b" << 6 ) );
                insert( BSON( "a" << 11 << "b" << 1 ) );
                insert( BSON( "a" << 11 << "b" << 4 ) );
                check( BSON( "a" << BSON( "$gte" << 1 << "$lte" << 10 ) << "b" << BSON( "$in" << BSON_ARRAY( 4 << 6 ) ) ) );
            }
            virtual BSONObj idx() const { return BSON( "a" << 1 << "b" << 1 ); }
        };

        /**
         * IndexCursor::advance() may skip to new index positions multiple times.  A cutoff (tested
         * here) has been implemented to avoid excessive iteration in such cases.  See SERVER-3448.
         */
        class AbortImplicitScan : public Base {
        public:
            void run() {
                _c.dropCollection( ns() );
                // Set up a compound index with some data.
                BSONObj idxPattern( BSON( "a" << 1 << "b" << 1 ) );
                _c.ensureIndex( ns(), idxPattern );
                for( int i = 0; i < 300; ++i ) {
                    _c.insert( ns(), BSON( "a" << i << "b" << i ) );
                }
                _c.insert( ns(), BSON( "a" << 300 << "b" << 30 ) );

                // Set up a cursor on the { a:1, b:1 } index, the same cursor that would be created
                // for the query { b:30 }.  Because this query has no constraint on 'a' (the
                // first field of the compound index), the cursor will examine every distinct value
                // of 'a' in the index and check for an index key with that value for 'a' and 'b'
                // equal to 30.
                FieldRangeSet frs( ns(), BSON( "b" << 30 ), true, true );
                boost::shared_ptr<FieldRangeVector> frv( new FieldRangeVector( frs, idxPattern, shared_ptr<KeyGenerator>(), 1 ) );
                Client::Transaction transaction(DB_SERIALIZABLE);
                Client::WriteContext ctx( ns(), mongo::unittest::EMPTY_STRING );
                {
                    shared_ptr<Cursor> c( Cursor::make( getCollection( ns() ),
                                                                  getCollection( ns() )->idx(1),
                                                                  frv,
                                                                  0, 1 ) );

                    // IndexCursor::init() and IndexCursor::advance() attempt to advance the cursor to
                    // the next matching key, which may entail examining many successive distinct values
                    // of 'a' having no index key where b equals 30.  To prevent excessive iteration
                    // within init() and advance(), examining distinct 'a' values is aborted once an
                    // nscanned cutoff is reached.  We test here that this cutoff is applied, and that
                    // if it is applied before a matching key is found, then
                    // IndexCursor::currentMatches() returns false appropriately.

                    ASSERT( c->ok() );
                    // The starting iterate found by IndexCursor::init() does not match.  This is a key
                    // before the {'':30,'':30} key, because init() is aborted prematurely.
                    ASSERT( !c->currentMatches() );
                    // And init() stopped iterating before scanning the whole index (with ~300 keys).
                    ASSERT( c->nscanned() < 200 );

                    ASSERT( c->advance() );
                    // The next iterate matches (this is the {'':30,'':30} key).
                    ASSERT( c->currentMatches() );

                    int oldNscanned = c->nscanned();
                    ASSERT( c->advance() );
                    // Check that nscanned has increased ...
                    ASSERT( c->nscanned() > oldNscanned );
                    // ... but that advance() stopped iterating before the whole index (with ~300 keys)
                    // was scanned.
                    ASSERT( c->nscanned() < 200 );
                    // Because advance() is aborted prematurely, the current iterate does not match.
                    ASSERT( !c->currentMatches() );

                    // Iterate through the remainder of the index.
                    bool foundLastMatch = false;
                    while( c->advance() ) {
                        bool bMatches = ( c->current()[ "b" ].number() == 30 );
                        // The current iterate only matches if it has the proper 'b' value.
                        ASSERT_EQUALS( bMatches, c->currentMatches() );
                        if ( bMatches ) {
                            foundLastMatch = true;
                        }
                    }
                    // Check that the final match, on key {'':300,'':30}, is found.
                    ASSERT( foundLastMatch );
                }
                transaction.commit();
            }
        };

        class RequestMatcherFalse : public QueryPlanSelectionPolicy {
            virtual string name() const { return "RequestMatcherFalse"; }
            virtual bool requestMatcher() const { return false; }
        } _requestMatcherFalse;

        /**
         * An IndexCursor typically moves from one index match to another when its advance() method
         * is called.  However, to prevent excessive iteration advance() may bail out early before
         * the next index match is identified (SERVER-3448).  The IndexCursor must indicate that
         * these iterates are not matches in matchesCurrent() to prevent them from being matched
         * when requestMatcher == false.
         */
        class DontMatchOutOfIndexBoundsDocuments : public Base {
        public:
            void run() {
                _c.dropCollection( ns() );
                _c.ensureIndex( ns(), BSON( "a" << 1 ) );
                // Save 'a' values 0, 0.5, 1.5, 2.5 ... 97.5, 98.5, 99.
                _c.insert( ns(), BSON( "a" << 0 ) );
                _c.insert( ns(), BSON( "a" << 99 ) );
                for( int i = 0; i < 99; ++i ) {
                    _c.insert( ns(), BSON( "a" << ( i + 0.5 ) ) );
                }
                // Query 'a' values $in 0, 1, 2, ..., 99.
                BSONArrayBuilder inVals;
                for( int i = 0; i < 100; ++i ) {
                    inVals << i;
                }
                BSONObj query = BSON( "a" << BSON( "$in" << inVals.arr() ) );
                int matchCount = 0;
                Client::Transaction transaction(DB_SERIALIZABLE);
                {
                    Client::ReadContext ctx( ns(), mongo::unittest::EMPTY_STRING );
                    boost::shared_ptr<Cursor> c = getOptimizedCursor( ns(),
                                                                      query,
                                                                      BSONObj(),
                                                                      _requestMatcherFalse );
                    // The IndexCursor attempts to find each of the values 0, 1, 2, ... etc in the
                    // index.  Because the values 0.5, 1.5, etc are present in the index, the
                    // IndexCursor will explicitly look for all the values in the $in list during
                    // successive calls to advance().  Because there are a large number of $in values to
                    // iterate over, IndexCursor::advance() will bail out on intermediate values of 'a'
                    // (for example 20.5) that do not match the query if nscanned increases by more than
                    // 20.  We test here that these intermediate results are not matched.  Only the two
                    // correct matches a:0 and a:99 are matched.
                    while( c->ok() ) {
                        ASSERT( !c->matcher() );
                        if ( c->currentMatches() ) {
                            double aVal = c->current()[ "a" ].number();
                            // Only the expected values of a are matched.
                            ASSERT( aVal == 0 || aVal == 99 );
                            ++matchCount;
                        }
                        c->advance();
                    }
                    // Only the two expected documents a:0 and a:99 are matched.
                    ASSERT_EQUALS( 2, matchCount );
                }
                transaction.commit();
            }
        };

        /**
         * When using a multikey index, two constraints on the same field cannot be intersected for
         * a non $elemMatch query (SERVER-958).  For example, using a single key index on { a:1 }
         * the query { a:{ $gt:0, $lt:5 } } would generate the field range [[ 0, 5 ]].  But for a
         * multikey index the field range is [[ 0, max_number ]].  In this case, the field range
         * does not exactly represent the query, so a Matcher is required.
         */
        class MatcherRequiredTwoConstraintsSameField : public Base {
        public:
            void run() {
                _c.dropCollection( ns() );
                _c.ensureIndex( ns(), BSON( "a" << 1 ) );
                _c.insert( ns(), BSON( "_id" << 0 << "a" << BSON_ARRAY( 1 << 2 ) ) );
                _c.insert( ns(), BSON( "_id" << 1 << "a" << 9 ) );
                Client::Transaction transaction(DB_SERIALIZABLE);
                {
                    Client::ReadContext ctx( ns(), mongo::unittest::EMPTY_STRING );
                    boost::shared_ptr<Cursor> c =
                            getOptimizedCursor( ns(),
                                                BSON( "a" << GT << 0 << LT << 5 ),
                                                BSONObj(),
                                                _requestMatcherFalse );
                    while( c->ok() ) {
                        // A Matcher is provided even though 'requestMatcher' is false.
                        ASSERT( c->matcher() );
                        if ( c->currentMatches() ) {
                            // Even though a:9 is in the field range [[ 0, max_number ]], that result
                            // does not match because the Matcher rejects it.  Only the _id:0 document
                            // matches.
                            ASSERT_EQUALS( 0, c->current()[ "_id" ].number() );
                        }
                        c->advance();
                    }
                }
                transaction.commit();
            }
        };

        /**
         * When using a multikey index, two constraints on fields with a shared parent cannot be
         * intersected for a non $elemMatch query (SERVER-958).  For example, using a single key
         * compound index on { 'a.b':1, 'a.c':1 } the query { 'a.b':2, 'a.c':2 } would generate the
         * field range vector [ [[ 2, 2 ]], [[ 2, 2 ]] ].  But for a multikey index the field range
         * vector is [ [[ 2, 2 ]], [[ minkey, maxkey ]] ].  In this case, the field range does not
         * exactly represent the query, so a Matcher is required.
         */
        class MatcherRequiredTwoConstraintsDifferentFields : public Base {
        public:
            void run() {
                _c.dropCollection( ns() );
                _c.ensureIndex( ns(), BSON( "a.b" << 1 << "a.c" << 1 ) );
                _c.insert( ns(), BSON( "a" << BSON_ARRAY( BSON( "b" << 2 << "c" << 3 ) <<
                                                          BSONObj() ) ) );
                Client::Transaction transaction(DB_SERIALIZABLE);
                {
                    Client::ReadContext ctx( ns(), mongo::unittest::EMPTY_STRING );
                    boost::shared_ptr<Cursor> c =
                            getOptimizedCursor( ns(),
                                                                  BSON( "a.b" << 2 << "a.c" << 2 ),
                                                                  BSONObj(),
                                                                  _requestMatcherFalse );
                    while( c->ok() ) {
                        // A Matcher is provided even though 'requestMatcher' is false.
                        ASSERT( c->matcher() );
                        // Even though { a:[ { b:2, c:3 } ] } is matched by the field range vector
                        // [ [[ 2, 2 ]], [[ minkey, maxkey ]] ], that resut is not matched because the
                        // Matcher rejects the document.
                        ASSERT( !c->currentMatches() );
                        c->advance();
                    }                
                }
                transaction.commit();
            }
        };

        /**
         * The upper bound of a $gt:string query is the empty object.  This upper bound must be
         * exclusive so that empty objects do not match without a Matcher.
         */
        class TypeBracketedUpperBoundWithoutMatcher : public Base {
        public:
            void run() {
                _c.dropCollection( ns() );
                _c.ensureIndex( ns(), BSON( "a" << 1 ) );
                _c.insert( ns(), BSON( "_id" << 0 << "a" << "a" ) );
                _c.insert( ns(), BSON( "_id" << 1 << "a" << BSONObj() ) );
                Client::Transaction transaction(DB_SERIALIZABLE);
                {
                    Client::ReadContext ctx( ns(), mongo::unittest::EMPTY_STRING );
                    boost::shared_ptr<Cursor> c =
                            getOptimizedCursor( ns(),
                                                                  BSON( "a" << GTE << "" ),
                                                                  BSONObj(),
                                                                  _requestMatcherFalse );
                    while( c->ok() ) {
                        ASSERT( !c->matcher() );
                        if ( c->currentMatches() ) {
                            // Only a:'a' matches, not a:{}.
                            ASSERT_EQUALS( 0, c->current()[ "_id" ].number() );
                        }
                        c->advance();
                    }
                }
                transaction.commit();
            }
        };

        /**
         * The lower bound of a $lt:date query is the bson value 'true'.  This lower bound must be
         * exclusive so that 'true' values do not match without a Matcher.
         */
        class TypeBracketedLowerBoundWithoutMatcher : public Base {
        public:
            void run() {
                _c.dropCollection( ns() );
                _c.ensureIndex( ns(), BSON( "a" << 1 ) );
                _c.insert( ns(), BSON( "_id" << 0 << "a" << Date_t( 1 ) ) );
                _c.insert( ns(), BSON( "_id" << 1 << "a" << true ) );
                Client::Transaction transaction(DB_SERIALIZABLE);
                {
                    Client::ReadContext ctx( ns(), mongo::unittest::EMPTY_STRING );
                    boost::shared_ptr<Cursor> c =
                            getOptimizedCursor( ns(),
                                                                  BSON( "a" << LTE << Date_t( 1 ) ),
                                                                  BSONObj(),
                                                                  _requestMatcherFalse );
                    while( c->ok() ) {
                        ASSERT( !c->matcher() );
                        if ( c->currentMatches() ) {
                            // Only a:Date_t( 1 ) matches, not a:true.
                            ASSERT_EQUALS( 0, c->current()[ "_id" ].number() );
                        }
                        c->advance();
                    }                
                }
                transaction.commit();
            }
        };
        
    } // namespace IndexCursor
    
    namespace ClientCursor {

        using mongo::ClientCursor;
        
        static const char * const ns() { return "unittests.cursortests.clientcursor"; }
        DBDirectClient client;

        namespace Pin {

            class Base {
            public:
                Base() :
                    _transaction(DB_SERIALIZABLE),
                    _ctx( ns(), mongo::unittest::EMPTY_STRING ),
                    _cursor( BasicCursor::make( getCollection(ns()) ) ) {
                    ASSERT( _cursor );
                    _clientCursor.reset( new ClientCursor( 0, _cursor, ns() ) );
                }
                ~Base() {
                    _transaction.commit();
                }
            protected:
                CursorId cursorid() const { return _clientCursor->cursorid(); }
            private:
                Client::Transaction _transaction;
                Client::WriteContext _ctx;
                boost::shared_ptr<Cursor> _cursor;
                ClientCursor::Holder _clientCursor;
            };
            
            /** Pin pins a ClientCursor over its lifetime. */
            class PinCursor : public Base {
            public:
                void run() {
                    assertNotPinned();
                    {
                        ClientCursor::Pin pin( cursorid() );
                        assertPinned();
                        ASSERT_THROWS( erase(), AssertionException );
                    }
                    assertNotPinned();
                    ASSERT( erase() );
                }
            private:
                void assertPinned() const {
                    ASSERT( ClientCursor::find( cursorid() ) );
                }
                void assertNotPinned() const {
                    ASSERT_THROWS( ClientCursor::find( cursorid() ), AssertionException );
                }
                bool erase() const {
                    return ClientCursor::erase( cursorid() );
                }
            };
            
            /** A ClientCursor cannot be pinned twice. */
            class PinTwice : public Base {
            public:
                void run() {
                    ClientCursor::Pin pin( cursorid() );
                    ASSERT_THROWS( pinCursor(), AssertionException );
                }
            private:
                void pinCursor() const {
                    ClientCursor::Pin pin( cursorid() );
                }
            };
            
            /** Pin behaves properly if its ClientCursor is destroyed early. */
            class CursorDeleted : public Base {
            public:
                void run() {
                    ClientCursor::Pin pin( cursorid() );
                    ASSERT( pin.c() );
                    // Delete the pinned cursor.
                    ClientCursor::invalidate( ns() );
                    ASSERT( !pin.c() );
                    // pin is destroyed safely, even though its ClientCursor was already destroyed.
                }
            };
            
        } // namespace Pin

    } // namespace ClientCursor
    
    class All : public Suite {
    public:
        All() : Suite( "cursor" ) {}

        void setupTests() {
            add<IndexCursor::MultiRangeForward>();
            add<IndexCursor::MultiRangeGap>();
            add<IndexCursor::MultiRangeReverse>();
            add<IndexCursor::EqEq>();
            add<IndexCursor::EqRange>();
            add<IndexCursor::EqIn>();
            add<IndexCursor::RangeEq>();
            add<IndexCursor::RangeIn>();
            add<IndexCursor::AbortImplicitScan>();
            add<IndexCursor::DontMatchOutOfIndexBoundsDocuments>();
            add<IndexCursor::MatcherRequiredTwoConstraintsSameField>();
            add<IndexCursor::MatcherRequiredTwoConstraintsDifferentFields>();
            add<IndexCursor::TypeBracketedUpperBoundWithoutMatcher>();
            add<IndexCursor::TypeBracketedLowerBoundWithoutMatcher>();
            add<ClientCursor::Pin::PinCursor>();
            add<ClientCursor::Pin::PinTwice>();
            add<ClientCursor::Pin::CursorDeleted>();
        }
    } myall;
} // namespace CursorTests
