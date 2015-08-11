// namespacetests.cpp : namespace.{h,cpp} unit tests.
//

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
#include "mongo/db/json.h"
#include "mongo/db/queryutil.h"

#include "mongo/dbtests/dbtests.h"

namespace NamespaceTests {

    using boost::shared_ptr;

    namespace IndexDetailsTests {
        class Base {
            Lock::GlobalWrite lk;
            Client::Context _context;
            shared_ptr<IndexInterface> _idx;
        public:
            IndexInterface &idx() { return *_idx; }
            Base() : lk(mongo::unittest::EMPTY_STRING), _context(ns()), _idx() {
            }
            virtual ~Base() {
            }
        protected:
            void create() {
                _idx.reset(new IndexInterface(info()));
            }
            virtual bool isSparse() const {
                return false;
            }
            BSONObj info() const {
                BSONObjBuilder builder;
                builder.append( "ns", ns() );
                builder.append( "name", "testIndex" );
                builder.append( "key", key() );
                builder.append( "sparse", isSparse() );
                return builder.obj();
            }
            static const char* ns() {
                return "unittests.indexdetailstests";
            }
            virtual BSONObj key() const {
                BSONObjBuilder k;
                k.append( "a", 1 );
                return k.obj();
            }
            void _getKeysFromObject( const BSONObj &obj, BSONObjSet &keys ) {
                idx().getKeysFromObject( obj, keys );
            }
            BSONObj aDotB() const {
                BSONObjBuilder k;
                k.append( "a.b", 1 );
                return k.obj();
            }
            BSONObj aAndB() const {
                BSONObjBuilder k;
                k.append( "a", 1 );
                k.append( "b", 1 );
                return k.obj();
            }
            static vector< int > shortArray() {
                vector< int > a;
                a.push_back( 1 );
                a.push_back( 2 );
                a.push_back( 3 );
                return a;
            }
            static BSONObj simpleBC( int i ) {
                BSONObjBuilder b;
                b.append( "b", i );
                b.append( "c", 4 );
                return b.obj();
            }
            static void checkSize( int expected, const BSONObjSet  &objs ) {
                ASSERT_EQUALS( BSONObjSet::size_type( expected ), objs.size() );
            }
            static void assertEquals( const BSONObj &a, const BSONObj &b ) {
                if ( a.woCompare( b ) != 0 ) {
                    out() << "expected: " << a.toString()
                          << ", got: " << b.toString() << endl;
                }
                ASSERT( a.woCompare( b ) == 0 );
            }
            BSONObj nullObj() const {
                BSONObjBuilder b;
                b.appendNull( "" );
                return b.obj();
            }
        };

        class GetKeysFromObjectSimple : public Base {
        public:
            void run() {
                create();  
                BSONObjBuilder b, e;
                b.append( "b", 4 );
                b.append( "a", 5 );
                e.append( "", 5 );
                BSONObjSet keys;
                _getKeysFromObject( b.done(), keys );
                checkSize( 1, keys );
                assertEquals( e.obj(), *keys.begin() );
            }
        };

        class GetKeysFromObjectDotted : public Base {
        public:
            void run() {
                create();  
                BSONObjBuilder a, e, b;
                b.append( "b", 4 );
                a.append( "a", b.done() );
                a.append( "c", "foo" );
                e.append( "", 4 );
                BSONObjSet keys;
                _getKeysFromObject( a.done(), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( e.obj(), *keys.begin() );
            }
        private:
            virtual BSONObj key() const {
                return aDotB();
            }
        };

        class GetKeysFromArraySimple : public Base {
        public:
            void run() {
                create();  
                BSONObjBuilder b;
                b.append( "a", shortArray()) ;

                BSONObjSet keys;
                _getKeysFromObject( b.done(), keys );
                checkSize( 3, keys );
                int j = 1;
                for ( BSONObjSet::iterator i = keys.begin(); i != keys.end(); ++i, ++j ) {
                    BSONObjBuilder b;
                    b.append( "", j );
                    assertEquals( b.obj(), *i );
                }
            }
        };

        class GetKeysFromArrayFirstElement : public Base {
        public:
            void run() {
                create();  
                BSONObjBuilder b;
                b.append( "a", shortArray() );
                b.append( "b", 2 );

                BSONObjSet keys;
                _getKeysFromObject( b.done(), keys );
                checkSize( 3, keys );
                int j = 1;
                for ( BSONObjSet::iterator i = keys.begin(); i != keys.end(); ++i, ++j ) {
                    BSONObjBuilder b;
                    b.append( "", j );
                    b.append( "", 2 );
                    assertEquals( b.obj(), *i );
                }
            }
        private:
            virtual BSONObj key() const {
                return aAndB();
            }
        };

        class GetKeysFromArraySecondElement : public Base {
        public:
            void run() {
                create();  
                BSONObjBuilder b;
                b.append( "first", 5 );
                b.append( "a", shortArray()) ;

                BSONObjSet keys;
                _getKeysFromObject( b.done(), keys );
                checkSize( 3, keys );
                int j = 1;
                for ( BSONObjSet::iterator i = keys.begin(); i != keys.end(); ++i, ++j ) {
                    BSONObjBuilder b;
                    b.append( "", 5 );
                    b.append( "", j );
                    assertEquals( b.obj(), *i );
                }
            }
        private:
            virtual BSONObj key() const {
                BSONObjBuilder k;
                k.append( "first", 1 );
                k.append( "a", 1 );
                return k.obj();
            }
        };

        class GetKeysFromSecondLevelArray : public Base {
        public:
            void run() {
                create();  
                BSONObjBuilder b;
                b.append( "b", shortArray() );
                BSONObjBuilder a;
                a.append( "a", b.done() );

                BSONObjSet keys;
                _getKeysFromObject( a.done(), keys );
                checkSize( 3, keys );
                int j = 1;
                for ( BSONObjSet::iterator i = keys.begin(); i != keys.end(); ++i, ++j ) {
                    BSONObjBuilder b;
                    b.append( "", j );
                    assertEquals( b.obj(), *i );
                }
            }
        private:
            virtual BSONObj key() const {
                return aDotB();
            }
        };

        class ParallelArraysBasic : public Base {
        public:
            void run() {
                create();  
                BSONObjBuilder b;
                b.append( "a", shortArray() );
                b.append( "b", shortArray() );

                BSONObjSet keys;
                ASSERT_THROWS( _getKeysFromObject( b.done(), keys ),
                                  UserException );
            }
        private:
            virtual BSONObj key() const {
                return aAndB();
            }
        };

        class ArraySubobjectBasic : public Base {
        public:
            void run() {
                create(); 
                vector< BSONObj > elts;
                for ( int i = 1; i < 4; ++i )
                    elts.push_back( simpleBC( i ) );
                BSONObjBuilder b;
                b.append( "a", elts );

                BSONObjSet keys;
                _getKeysFromObject( b.done(), keys );
                checkSize( 3, keys );
                int j = 1;
                for ( BSONObjSet::iterator i = keys.begin(); i != keys.end(); ++i, ++j ) {
                    BSONObjBuilder b;
                    b.append( "", j );
                    assertEquals( b.obj(), *i );
                }
            }
        private:
            virtual BSONObj key() const {
                return aDotB();
            }
        };

        class ArraySubobjectMultiFieldIndex : public Base {
        public:
            void run() {
                create();  
                vector< BSONObj > elts;
                for ( int i = 1; i < 4; ++i )
                    elts.push_back( simpleBC( i ) );
                BSONObjBuilder b;
                b.append( "a", elts );
                b.append( "d", 99 );

                BSONObjSet keys;
                _getKeysFromObject( b.done(), keys );
                checkSize( 3, keys );
                int j = 1;
                for ( BSONObjSet::iterator i = keys.begin(); i != keys.end(); ++i, ++j ) {
                    BSONObjBuilder c;
                    c.append( "", j );
                    c.append( "", 99 );
                    assertEquals( c.obj(), *i );
                }
            }
        private:
            virtual BSONObj key() const {
                BSONObjBuilder k;
                k.append( "a.b", 1 );
                k.append( "d", 1 );
                return k.obj();
            }
        };

        class ArraySubobjectSingleMissing : public Base {
        public:
            void run() {
                create(); 
                vector< BSONObj > elts;
                BSONObjBuilder s;
                s.append( "foo", 41 );
                elts.push_back( s.obj() );
                for ( int i = 1; i < 4; ++i )
                    elts.push_back( simpleBC( i ) );
                BSONObjBuilder b;
                b.append( "a", elts );
                BSONObj obj = b.obj();
                
                BSONObjSet keys;
                _getKeysFromObject( obj, keys );
                checkSize( 4, keys );
                BSONObjSet::iterator i = keys.begin();
                assertEquals( nullObj(), *i++ ); // see SERVER-3377
                for ( int j = 1; j < 4; ++i, ++j ) {
                    BSONObjBuilder b;
                    b.append( "", j );
                    assertEquals( b.obj(), *i );
                }
            }
        private:
            virtual BSONObj key() const {
                return aDotB();
            }
        };

        class ArraySubobjectMissing : public Base {
        public:
            void run() {
                create(); 
                vector< BSONObj > elts;
                BSONObjBuilder s;
                s.append( "foo", 41 );
                for ( int i = 1; i < 4; ++i )
                    elts.push_back( s.done() );
                BSONObjBuilder b;
                b.append( "a", elts );

                BSONObjSet keys;
                _getKeysFromObject( b.done(), keys );
                checkSize( 1, keys );
                assertEquals( nullObj(), *keys.begin() );
            }
        private:
            virtual BSONObj key() const {
                return aDotB();
            }
        };

        class MissingField : public Base {
        public:
            void run() {
                create(); 
                BSONObjSet keys;
                _getKeysFromObject( BSON( "b" << 1 ), keys );
                checkSize( 1, keys );
                assertEquals( nullObj(), *keys.begin() );
            }
        private:
            virtual BSONObj key() const {
                return BSON( "a" << 1 );
            }
        };

        class SubobjectMissing : public Base {
        public:
            void run() {
                create(); 
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[1,2]}" ), keys );
                checkSize( 1, keys );
                assertEquals( nullObj(), *keys.begin() );
            }
        private:
            virtual BSONObj key() const {
                return aDotB();
            }
        };

        class CompoundMissing : public Base {
        public:
            void run() {
                create(); 

                {
                    BSONObjSet keys;
                    _getKeysFromObject( fromjson( "{x:'a',y:'b'}" ) , keys );
                    checkSize( 1 , keys );
                    assertEquals( BSON( "" << "a" << "" << "b" ) , *keys.begin() );
                }

                {
                    BSONObjSet keys;
                    _getKeysFromObject( fromjson( "{x:'a'}" ) , keys );
                    checkSize( 1 , keys );
                    BSONObjBuilder b;
                    b.append( "" , "a" );
                    b.appendNull( "" );
                    assertEquals( b.obj() , *keys.begin() );
                }

            }

        private:
            virtual BSONObj key() const {
                return BSON( "x" << 1 << "y" << 1 );
            }

        };

        class ArraySubelementComplex : public Base {
        public:
            void run() {
                create();  
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[{b:[2]}]}" ), keys );
                checkSize( 1, keys );
                assertEquals( BSON( "" << 2 ), *keys.begin() );
            }
        private:
            virtual BSONObj key() const {
                return aDotB();
            }
        };

        class ParallelArraysComplex : public Base {
        public:
            void run() {
                create();  
                BSONObjSet keys;
                ASSERT_THROWS( _getKeysFromObject( fromjson( "{a:[{b:[1],c:[2]}]}" ), keys ),
                                  UserException );
            }
        private:
            virtual BSONObj key() const {
                return fromjson( "{'a.b':1,'a.c':1}" );
            }
        };

        class AlternateMissing : public Base {
        public:
            void run() {
                create();  
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[{b:1},{c:2}]}" ), keys );
                checkSize( 2, keys );
                BSONObjSet::iterator i = keys.begin();
                {
                    BSONObjBuilder e;
                    e.appendNull( "" );
                    e.append( "", 2 );
                    assertEquals( e.obj(), *i++ );
                }

                {
                    BSONObjBuilder e;
                    e.append( "", 1 );
                    e.appendNull( "" );
                    assertEquals( e.obj(), *i++ );
                }
            }
        private:
            virtual BSONObj key() const {
                return fromjson( "{'a.b':1,'a.c':1}" );
            }
        };

        class MultiComplex : public Base {
        public:
            void run() {
                create();                
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[{b:1},{b:[1,2,3]}]}" ), keys );
                checkSize( 3, keys );
            }
        private:
            virtual BSONObj key() const {
                return aDotB();
            }
        };

        class EmptyArray : Base {
        public:
            void run() {
                create();  

                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[1,2]}" ), keys );
                checkSize(2, keys );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[1]}" ), keys );
                checkSize(1, keys );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:null}" ), keys );
                checkSize(1, keys );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[]}" ), keys );
                checkSize(1, keys );
                ASSERT_EQUALS( Undefined, keys.begin()->firstElement().type() );
                keys.clear();
            }
        };
 
        class DoubleArray : Base {
        public:
            void run() {
                create();                
                
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[1,2]}" ), keys );
                checkSize(2, keys );
                BSONObjSet::const_iterator i = keys.begin();
                ASSERT_EQUALS( BSON( "" << 1 << "" << 1 ), *i );
                ++i;
                ASSERT_EQUALS( BSON( "" << 2 << "" << 2 ), *i );
                keys.clear();
            }
            
        protected:
            BSONObj key() const {
                return BSON( "a" << 1 << "a" << 1 );
            }
        };
        
        class DoubleEmptyArray : Base {
        public:
            void run() {
                create();

                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[]}" ), keys );
                checkSize(1, keys );
                ASSERT_EQUALS( fromjson( "{'':undefined,'':undefined}" ), *keys.begin() );
                keys.clear();
            }
            
        protected:
            BSONObj key() const {
                return BSON( "a" << 1 << "a" << 1 );
            }
        };

        class MultiEmptyArray : Base {
        public:
            void run() {
                create(); 

                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:1,b:[1,2]}" ), keys );
                checkSize(2, keys );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:1,b:[1]}" ), keys );
                checkSize(1, keys );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:1,b:null}" ), keys );
                //cout << "YO : " << *(keys.begin()) << endl;
                checkSize(1, keys );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:1,b:[]}" ), keys );
                checkSize(1, keys );
                //cout << "YO : " << *(keys.begin()) << endl;
                BSONObjIterator i( *keys.begin() );
                ASSERT_EQUALS( NumberInt , i.next().type() );
                ASSERT_EQUALS( Undefined , i.next().type() );
                keys.clear();
            }

        protected:
            BSONObj key() const {
                return aAndB();
            }
        };
        
        class NestedEmptyArray : Base {
        public:
            void run() {
                create();     	
                
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':null}" ), *keys.begin() );
                keys.clear();
            }
        protected:
            BSONObj key() const { return BSON( "a.b" << 1 ); }
        };
        
		class MultiNestedEmptyArray : Base {
        public:
            void run() {
                create();	
                
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':null,'':null}" ), *keys.begin() );
                keys.clear();
            }
        protected:
            BSONObj key() const { return BSON( "a.b" << 1 << "a.c" << 1 ); }
        };
        
        class UnevenNestedEmptyArray : public Base {
        public:
            void run() {
                create(); 	
                
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':undefined,'':null}" ), *keys.begin() );
                keys.clear();
                
                _getKeysFromObject( fromjson( "{a:[{b:1}]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':{b:1},'':1}" ), *keys.begin() );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[{b:[]}]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':{b:[]},'':undefined}" ), *keys.begin() );
                keys.clear();
            }
        protected:
            BSONObj key() const { return BSON( "a" << 1 << "a.b" << 1 ); }            
        };

        class ReverseUnevenNestedEmptyArray : public Base {
        public:
            void run() {
                create();	
                
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':null,'':undefined}" ), *keys.begin() );
                keys.clear();
            }
        protected:
            BSONObj key() const { return BSON( "a.b" << 1 << "a" << 1 ); }            
        };

        class SparseBase : public Base {
            virtual bool isSparse() const {
                return true;
            }
        };
        
        class SparseReverseUnevenNestedEmptyArray : public SparseBase {
        public:
            void run() {
                create();	
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':null,'':undefined}" ), *keys.begin() );
                keys.clear();
            }
        protected:
            BSONObj key() const { return BSON( "a.b" << 1 << "a" << 1 ); }            
        };
        
        class SparseEmptyArray : public SparseBase {
        public:
            void run() {
                create();	
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:1}" ), keys );
                checkSize( 0, keys );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[]}" ), keys );
                checkSize( 0, keys );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[{c:1}]}" ), keys );
                checkSize( 0, keys );
                keys.clear();
            }
        protected:
            BSONObj key() const { return BSON( "a.b" << 1 ); }            
        };

        class SparseEmptyArraySecond : public SparseBase {
        public:
            void run() {
                create();	
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:1}" ), keys );
                checkSize( 0, keys );
                keys.clear();
                
                _getKeysFromObject( fromjson( "{a:[]}" ), keys );
                checkSize( 0, keys );
                keys.clear();
                
                _getKeysFromObject( fromjson( "{a:[{c:1}]}" ), keys );
                checkSize( 0, keys );
                keys.clear();
            }
        protected:
            BSONObj key() const { return BSON( "z" << 1 << "a.b" << 1 ); }
        };
        
        class NonObjectMissingNestedField : public Base {
        public:
            void run() {
                create();	
                
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':null}" ), *keys.begin() );
                keys.clear();
                
                _getKeysFromObject( fromjson( "{a:[1]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':null}" ), *keys.begin() );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[1,{b:1}]}" ), keys );
                checkSize( 2, keys );
                BSONObjSet::const_iterator c = keys.begin();
                ASSERT_EQUALS( fromjson( "{'':null}" ), *c );
                ++c;
                ASSERT_EQUALS( fromjson( "{'':1}" ), *c );
                keys.clear();
            }
        protected:
            BSONObj key() const { return BSON( "a.b" << 1 ); }
        };

        class SparseNonObjectMissingNestedField : public Base {
        public:
            bool isSparse() const { return true; }
            void run() {
                create();	
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[]}" ), keys );
                checkSize( 0, keys );
                keys.clear();
                
                _getKeysFromObject( fromjson( "{a:[1]}" ), keys );
                checkSize( 0, keys );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[1,{b:1}]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':1}" ), *keys.begin() );
                keys.clear();
            }
        protected:
            BSONObj key() const { return BSON( "a.b" << 1 ); }
        };
        
        class IndexedArrayIndex : public Base {
        public:
            void run() {
                create();	
                
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[1]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( BSON( "" << 1 ), *keys.begin() );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[[1]]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':[1]}" ), *keys.begin() );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[[]]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':undefined}" ), *keys.begin() );
                keys.clear();
                
                _getKeysFromObject( fromjson( "{a:{'0':1}}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( BSON( "" << 1 ), *keys.begin() );
                keys.clear();

                ASSERT_THROWS( _getKeysFromObject( fromjson( "{a:[{'0':1}]}" ), keys ), UserException );

                ASSERT_THROWS( _getKeysFromObject( fromjson( "{a:[1,{'0':2}]}" ), keys ), UserException );
            }
        protected:
            BSONObj key() const { return BSON( "a.0" << 1 ); }
        };

        class DoubleIndexedArrayIndex : public Base {
        public:
            void run() {
                create();	
                
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[[1]]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':1}" ), *keys.begin() );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[[]]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':null}" ), *keys.begin() );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':null}" ), *keys.begin() );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[[[]]]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':undefined}" ), *keys.begin() );
                keys.clear();
            }
        protected:
            BSONObj key() const { return BSON( "a.0.0" << 1 ); }
        };
        
        class ObjectWithinArray : public Base {
        public:
            void run() {
                create();	
                
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[{b:1}]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':1}" ), *keys.begin() );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[{b:[1]}]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':1}" ), *keys.begin() );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[{b:[[1]]}]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':[1]}" ), *keys.begin() );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[[{b:1}]]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':1}" ), *keys.begin() );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[[{b:[1]}]]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':1}" ), *keys.begin() );
                keys.clear();

                _getKeysFromObject( fromjson( "{a:[[{b:[[1]]}]]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':[1]}" ), *keys.begin() );
                keys.clear();
                
                _getKeysFromObject( fromjson( "{a:[[{b:[]}]]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':undefined}" ), *keys.begin() );
                keys.clear();
            }
        protected:
            BSONObj key() const { return BSON( "a.0.b" << 1 ); }
        };

        class ArrayWithinObjectWithinArray : public Base {
        public:
            void run() {
                create();	
                
                BSONObjSet keys;
                _getKeysFromObject( fromjson( "{a:[{b:[1]}]}" ), keys );
                checkSize( 1, keys );
                ASSERT_EQUALS( fromjson( "{'':1}" ), *keys.begin() );
                keys.clear();
            }
        protected:
            BSONObj key() const { return BSON( "a.0.b.0" << 1 ); }
        };
        
        class Suitability : public Base {
        public:
            void run() {
                create();
                FieldRangeSet frs1( "", BSON( "a" << 2 << "b" << 3 ), true , true );
                FieldRangeSet frs2( "", BSON( "b" << 3 ), true , true );
                ASSERT_EQUALS( IndexDetails::HELPFUL,
                               idx().suitability( frs1, BSONObj() ) );
                ASSERT_EQUALS( IndexDetails::USELESS,
                               idx().suitability( frs2, BSONObj() ) );
                ASSERT_EQUALS( IndexDetails::HELPFUL,
                               idx().suitability( frs2, BSON( "a" << 1 ) ) );
            }
        protected:
            BSONObj key() const { return BSON( "a" << 1 ); }
        };
        
        /** Lexical rather than numeric comparison should be used to determine index suitability. */
        class NumericFieldSuitability : public Base {
        public:
            void run() {
                create();
                FieldRangeSet frs1( "", BSON( "1" << 2 ), true , true );
                FieldRangeSet frs2( "", BSON( "01" << 3), true , true );
                FieldRangeSet frs3( "", BSONObj() , true , true );
                ASSERT_EQUALS( IndexDetails::HELPFUL,
                               idx().suitability( frs1, BSONObj() ) );
                ASSERT_EQUALS( IndexDetails::USELESS,
                               idx().suitability( frs2, BSON( "01" << 1 ) ) );
                ASSERT_EQUALS( IndexDetails::HELPFUL,
                               idx().suitability( frs3, BSON( "1" << 1 ) ) );                
            }
        protected:
            BSONObj key() const { return BSON( "1" << 1 ); }
        };

        /** A missing field is represented as null in an index. */
        class IndexMissingField : public Base {
        public:
            void run() {
                create();
                ASSERT_EQUALS( jstNULL, idx().missingField().type() );
            }
        protected:
            BSONObj key() const { return BSON( "a" << 1 ); }
        };
        
    } // namespace IndexDetailsTests

    namespace CollectionTests {

        class Base {
            const char *ns_;
            Client::Transaction _transaction;
            Lock::GlobalWrite lk;
            Client::Context _context;
        public:
            Base( const char *ns = "unittests.CollectionTests" ) : ns_( ns ) , _transaction(DB_SERIALIZABLE), lk(mongo::unittest::EMPTY_STRING), _context( ns ) {}
            virtual ~Base() {
                if ( !nsd() )
                    return;
                _transaction.commit();
                string errmsg;
                BSONObjBuilder result;
                Client::Transaction droptxn(DB_SERIALIZABLE);
                nsd()->drop(errmsg, result);
                droptxn.commit();
            }
            Client::Context &ctx() {
                return _context;
            }
        protected:
            void create() {
                Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
                string err;
                ASSERT( userCreateNS( ns(), fromjson( spec() ), err, false ) );
            }
            virtual string spec() const {
                return "{\"capped\":true,\"size\":512,\"$nExtents\":1}";
            }
            static int min( int a, int b ) {
                return a < b ? a : b;
            }
            const char *ns() const {
                return ns_;
            }
            Collection *nsd() {
                if (getCollection(ns()) == NULL) {
                    create();
                }
                return getCollection( ns() );
            }
            static BSONObj bigObj(bool bGenID=false) {
                BSONObjBuilder b;
				if (bGenID)
					b.appendOID("_id", 0, true);
                string as( 187, 'a' );
                b.append( "a", as );
                return b.obj();
            }
        };

        // This isn't a particularly useful test, and because it doesn't clean up
        // after itself, /tmp/unittest needs to be cleared after running.
        //        class BigCollection : public Base {
        //        public:
        //            BigCollection() : Base( "CollectionTests_BigCollection" ) {}
        //            void run() {
        //                
        //                ASSERT_EQUALS( 2, nExtents() );
        //            }
        //        private:
        //            virtual string spec() const {
        //                // NOTE 256 added to size in _userCreateNS()
        //                long long big = MongoDataFile::maxSize() - DataFileHeader::HeaderSize;
        //                stringstream ss;
        //                ss << "{\"capped\":true,\"size\":" << big << "}";
        //                return ss.str();
        //            }
        //        };

        class CachedPlanBase : public Base {
        public:
            CachedPlanBase() :
                _fieldRangeSet( ns(), BSON( "a" << 1 ), true, true ),
                _pattern( _fieldRangeSet, BSONObj() ) {
                
            }
        protected:
            void assertCachedIndexKey( const BSONObj &indexKey ) {
                ASSERT_EQUALS( indexKey,
                              nsd()->getQueryCache().cachedQueryPlanForPattern( _pattern ).indexKey() );
            }
            void registerIndexKey( const BSONObj &indexKey ) {
                nsd()->getQueryCache().registerCachedQueryPlanForPattern
                        ( _pattern,
                         CachedQueryPlan( indexKey, 1, CandidatePlanCharacter( true, false ) ) );                
            }
            FieldRangeSet _fieldRangeSet;
            QueryPattern _pattern;
        };
        
        /**
         * setIndexIsMultikey() sets the multikey flag for an index and clears the query plan
         * cache.
         */
        class SetIndexIsMultikey : public CachedPlanBase {
        public:
            void run() {
                string err;
                userCreateNS( ns(), BSONObj(), err, false );
                ASSERT( nsd() != NULL );
                DBDirectClient client;
                client.ensureIndex( ns(), BSON( "a" << 1 ) );
                registerIndexKey( BSON( "a" << 1 ) );

                ASSERT( !nsd()->isMultikey( 1 ) );
                bool dummy;
                Collection* cl = nsd();
                CollectionBase* cd = cl->as<CollectionBase>();
                cd->setIndexIsMultikey( 1, &dummy );
                cl->noteMultiKeyChanged(); // this is what now clears the query cache
                ASSERT( nsd()->isMultikey( 1 ) );
                assertCachedIndexKey( BSONObj() );
                
                registerIndexKey( BSON( "a" << 1 ) );
                cd->setIndexIsMultikey( 1, &dummy );
                assertCachedIndexKey( BSON( "a" << 1 ) );
            }
        };

        /** clearQueryCache() clears the query plan cache. */
        class ClearQueryCache : public CollectionTests::CachedPlanBase {
        public:
            void run() {
                // Register a query plan in the query plan cache.
                registerIndexKey( BSON( "a" << 1 ) );
                assertCachedIndexKey( BSON( "a" << 1 ) );
                
                // The query plan is cleared.
                nsd()->getQueryCache().clearQueryCache();
                assertCachedIndexKey( BSONObj() );
            }
        };                                                                                         
        
    } // namespace CollectionTests

    class All : public Suite {
    public:
        All() : Suite( "namespace" ) {
        }

        void setupTests() {
            add< IndexDetailsTests::GetKeysFromObjectSimple >();
            add< IndexDetailsTests::GetKeysFromObjectDotted >();
            add< IndexDetailsTests::GetKeysFromArraySimple >();
            add< IndexDetailsTests::GetKeysFromArrayFirstElement >();
            add< IndexDetailsTests::GetKeysFromArraySecondElement >();
            add< IndexDetailsTests::GetKeysFromSecondLevelArray >();
            add< IndexDetailsTests::ParallelArraysBasic >();
            add< IndexDetailsTests::ArraySubobjectBasic >();
            add< IndexDetailsTests::ArraySubobjectMultiFieldIndex >();
            add< IndexDetailsTests::ArraySubobjectSingleMissing >();
            add< IndexDetailsTests::ArraySubobjectMissing >();
            add< IndexDetailsTests::ArraySubelementComplex >();
            add< IndexDetailsTests::ParallelArraysComplex >();
            add< IndexDetailsTests::AlternateMissing >();
            add< IndexDetailsTests::MultiComplex >();
            add< IndexDetailsTests::EmptyArray >();
            add< IndexDetailsTests::DoubleArray >();
            add< IndexDetailsTests::DoubleEmptyArray >();
            add< IndexDetailsTests::MultiEmptyArray >();
            add< IndexDetailsTests::NestedEmptyArray >();
            add< IndexDetailsTests::MultiNestedEmptyArray >();
            add< IndexDetailsTests::UnevenNestedEmptyArray >();
            add< IndexDetailsTests::ReverseUnevenNestedEmptyArray >();
            add< IndexDetailsTests::SparseReverseUnevenNestedEmptyArray >();
            add< IndexDetailsTests::SparseEmptyArray >();
            add< IndexDetailsTests::SparseEmptyArraySecond >();
            add< IndexDetailsTests::NonObjectMissingNestedField >();
            add< IndexDetailsTests::SparseNonObjectMissingNestedField >();
            add< IndexDetailsTests::IndexedArrayIndex >();
            add< IndexDetailsTests::DoubleIndexedArrayIndex >();
            add< IndexDetailsTests::ObjectWithinArray >();
            add< IndexDetailsTests::ArrayWithinObjectWithinArray >();
            add< IndexDetailsTests::MissingField >();
            add< IndexDetailsTests::SubobjectMissing >();
            add< IndexDetailsTests::CompoundMissing >();
            add< IndexDetailsTests::Suitability >();
            add< IndexDetailsTests::NumericFieldSuitability >();
            add< IndexDetailsTests::IndexMissingField >();
            add< CollectionTests::SetIndexIsMultikey >();
            add< CollectionTests::ClearQueryCache >();
        }
    } myall;
} // namespace NamespaceTests

