// cusrortests.cpp // cursor related unit tests
//

/**
 *    Copyright (C) 2009 10gen Inc.
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
#include "mongo/dbtests/dbtests.h"

namespace CursorTests {

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
                // orphan idxSpec for this test
                IndexSpec *idxSpec = new IndexSpec( BSON( "a" << 1 ) );
                return new FieldRangeVector( s, *idxSpec, direction );
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
                Client::WriteContext tc(ns);
                {
                    scoped_ptr<IndexCursor> _c( new IndexCursor( nsdetails( ns ), &nsdetails( ns )->idx(1), frv, 0, 1 ) );
                    IndexCursor &c = *_c.get();
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
                Client::WriteContext tc(ns);
                {
                    scoped_ptr<IndexCursor> _c( new IndexCursor(nsdetails( ns ), &nsdetails( ns )->idx(1), frv, 0, 1 ) );
                    IndexCursor &c = *_c.get();
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
                Client::WriteContext ctx( ns );
                {
                    scoped_ptr<IndexCursor> _c( new IndexCursor( nsdetails( ns ), &nsdetails( ns )->idx(1), frv, 0, -1 ) );
                    IndexCursor& c = *_c.get();
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
                Client::WriteContext ctx( ns() );
                FieldRangeSet frs( ns(), spec, true, true );
                // orphan spec for this test.
                IndexSpec *idxSpec = new IndexSpec( idx() );
                boost::shared_ptr< FieldRangeVector > frv( new FieldRangeVector( frs, *idxSpec, direction() ) );
                {
                    NamespaceDetails *d = nsdetails(ns());
                    int i = d->findIndexByKeyPattern(idx());
                    verify(i >= 0);
                    scoped_ptr<IndexCursor> c( new IndexCursor( d, &d->idx( i ), frv, 0, direction() ) );
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
        
        class AbortImplicitScan : public Base {
        public:
            void run() {
                IndexSpec idx( BSON( "a" << 1 << "b" << 1 ) );
                _c.ensureIndex( ns(), idx.keyPattern );
                for( int i = 0; i < 300; ++i ) {
                    _c.insert( ns(), BSON( "a" << i << "b" << 5 ) );
                }
                FieldRangeSet frs( ns(), BSON( "b" << 3 ), true, true );
                boost::shared_ptr<FieldRangeVector> frv( new FieldRangeVector( frs, idx, 1 ) );
                Client::Transaction transaction(DB_SERIALIZABLE);
                Client::WriteContext ctx( ns() );
                {
                    scoped_ptr<IndexCursor> c( new IndexCursor( nsdetails( ns() ), &nsdetails( ns() )->idx(1), frv, 0, 1 ) );
                    long long initialNscanned = c->nscanned();
                    ASSERT( initialNscanned < 200 );
                    ASSERT( c->ok() );
                    c->advance();
                    ASSERT( c->nscanned() > initialNscanned );
                    ASSERT( c->nscanned() < 200 );
                    ASSERT( c->ok() );
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
                    _ctx( ns() ),
                    _cursor( Helpers::findTableScan( ns(), BSONObj() ) ) {
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
            add<ClientCursor::Pin::PinCursor>();
            add<ClientCursor::Pin::PinTwice>();
            add<ClientCursor::Pin::CursorDeleted>();
        }
    } myall;
} // namespace CursorTests
