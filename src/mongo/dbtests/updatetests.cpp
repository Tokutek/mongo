// updatetests.cpp : unit tests relating to update requests
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

#include "pch.h"
#include "mongo/client/dbclientcursor.h"

#include "mongo/db/instance.h"
#include "mongo/db/json.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/update_internal.h"

#include "dbtests.h"

namespace UpdateTests {

    class ClientBase {
    public:
        // NOTE: Not bothering to backup the old error record.
        ClientBase() {
            mongo::lastError.reset( new LastError() );
        }
        ~ClientBase() {
            mongo::lastError.release();
        }
    protected:
        static void insert( const char *ns, BSONObj o ) {
            client_.insert( ns, o );
        }
        static void update( const char *ns, BSONObj q, BSONObj o, bool upsert = 0 ) {
            client_.update( ns, Query( q ), o, upsert );
        }
        static bool error() {
            return !client_.getPrevError().getField( "err" ).isNull();
        }
        DBDirectClient &client() const { return client_; }
    private:
        static DBDirectClient client_;
    };
    DBDirectClient ClientBase::client_;

    class Fail : public ClientBase {
    public:
        virtual ~Fail() {}
        void run() {
            prep();
            ASSERT( !error() );
            doIt();
            ASSERT( error() );
        }
    protected:
        const char *ns() { return "unittests.UpdateTests_Fail"; }
        virtual void prep() {
            insert( ns(), fromjson( "{a:1}" ) );
        }
        virtual void doIt() = 0;
    };

    class ModId : public Fail {
        void doIt() {
            update( ns(), BSONObj(), fromjson( "{$set:{'_id':4}}" ) );
        }
    };

    class ModNonmodMix : public Fail {
        void doIt() {
            update( ns(), BSONObj(), fromjson( "{$set:{a:4},z:3}" ) );
        }
    };

    class InvalidMod : public Fail {
        void doIt() {
            update( ns(), BSONObj(), fromjson( "{$awk:{a:4}}" ) );
        }
    };

    class ModNotFirst : public Fail {
        void doIt() {
            update( ns(), BSONObj(), fromjson( "{z:3,$set:{a:4}}" ) );
        }
    };

    class ModDuplicateFieldSpec : public Fail {
        void doIt() {
            update( ns(), BSONObj(), fromjson( "{$set:{a:4},$inc:{a:1}}" ) );
        }
    };

    class IncNonNumber : public Fail {
        void doIt() {
            update( ns(), BSONObj(), fromjson( "{$inc:{a:'d'}}" ) );
        }
    };

    class PushAllNonArray : public Fail {
        void doIt() {
            insert( ns(), fromjson( "{a:[1]}" ) );
            update( ns(), BSONObj(), fromjson( "{$pushAll:{a:'d'}}" ) );
        }
    };

    class PullAllNonArray : public Fail {
        void doIt() {
            insert( ns(), fromjson( "{a:[1]}" ) );
            update( ns(), BSONObj(), fromjson( "{$pullAll:{a:'d'}}" ) );
        }
    };

    class IncTargetNonNumber : public Fail {
        void doIt() {
            insert( ns(), BSON( "a" << "a" ) );
            update( ns(), BSON( "a" << "a" ), fromjson( "{$inc:{a:1}}" ) );
        }
    };

    class SetBase : public ClientBase {
    public:
        ~SetBase() {
            client().dropCollection( ns() );
        }
    protected:
        const char *ns() { return "unittests.updatetests.SetBase"; }
    };

    class SetNum : public SetBase {
    public:
        void run() {
            client().insert( ns(), BSON( "a" << 1 ) );
            client().update( ns(), BSON( "a" << 1 ), BSON( "$set" << BSON( "a" << 4 ) ) );
            ASSERT( !client().findOne( ns(), BSON( "a" << 4 ) ).isEmpty() );
        }
    };

    class SetString : public SetBase {
    public:
        void run() {
            client().insert( ns(), BSON( "a" << "b" ) );
            client().update( ns(), BSON( "a" << "b" ), BSON( "$set" << BSON( "a" << "c" ) ) );
            ASSERT( !client().findOne( ns(), BSON( "a" << "c" ) ).isEmpty() );
        }
    };

    class SetStringDifferentLength : public SetBase {
    public:
        void run() {
            client().insert( ns(), BSON( "a" << "b" ) );
            client().update( ns(), BSON( "a" << "b" ), BSON( "$set" << BSON( "a" << "cd" ) ) );
            ASSERT( !client().findOne( ns(), BSON( "a" << "cd" ) ).isEmpty() );
        }
    };

    class SetStringToNum : public SetBase {
    public:
        void run() {
            client().insert( ns(), BSON( "a" << "b" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "a" << 5 ) ) );
            ASSERT( !client().findOne( ns(), BSON( "a" << 5 ) ).isEmpty() );
        }
    };

    class SetStringToNumInPlace : public SetBase {
    public:
        void run() {
            client().insert( ns(), BSON( "a" << "bcd" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "a" << 5.0 ) ) );
            ASSERT( !client().findOne( ns(), BSON( "a" << 5.0 ) ).isEmpty() );
        }
    };

    class SetOnInsertFromEmpty : public SetBase {
    public:
        void run() {
            // Try with upsert false first.
            client().insert( ns(), BSONObj() /* empty document */);
            client().update( ns(), Query(), BSON( "$setOnInsert" << BSON( "a" << 1 ) ), false );
            ASSERT( client().findOne( ns(), BSON( "a" << 1 ) ).isEmpty() );

            // Then with upsert true.
            client().update( ns(), Query(), BSON( "$setOnInsert" << BSON( "a" << 1 ) ), true );
            ASSERT( client().findOne( ns(), BSON( "a" << 1 ) ).isEmpty() );

        }
    };

    class SetOnInsertFromNonExistent : public SetBase {
    public:
        void run() {
            // Try with upsert false first.
            client().update( ns(), Query(), BSON( "$setOnInsert" << BSON( "a" << 1 ) ), false );
            ASSERT( client().findOne( ns(), BSON( "a" << 1 ) ).isEmpty() );

            // Then with upsert true.
            client().update( ns(), Query(), BSON( "$setOnInsert" << BSON( "a" << 1 ) ), true );
            ASSERT( !client().findOne( ns(), BSON( "a" << 1 ) ).isEmpty() );

        }
    };

    class SetOnInsertFromNonExistentWithQuery : public SetBase {
    public:
        void run() {
            Query q("{a:1}");

            // Try with upsert false first.
            client().update( ns(), q, BSON( "$setOnInsert" << BSON( "b" << 1 ) ), false );
            ASSERT( client().findOne( ns(), BSON( "a" << 1 ) ).isEmpty() );

            // Then with upsert true.
            client().update( ns(), q, BSON( "$setOnInsert" << BSON( "b" << 1 ) ), true );
            ASSERT( !client().findOne( ns(), BSON( "a" << 1 << "b" << 1) ).isEmpty() );

        }
    };

    class SetOnInsertFromNonExistentWithQueryOverField : public SetBase {
    public:
        void run() {
            Query q("{a:1}"); // same field that we'll setOnInsert on

            // Try with upsert false first.
            client().update( ns(), q, BSON( "$setOnInsert" << BSON( "a" << 2 ) ), false );
            ASSERT( client().findOne( ns(), BSON( "a" << 1 ) ).isEmpty() );

            // Then with upsert true.
            client().update( ns(), q, BSON( "$setOnInsert" << BSON( "a" << 2 ) ), true );
            ASSERT( !client().findOne( ns(), BSON( "a" << 2 ) ).isEmpty() );

        }
    };

    class SetOnInsertMissingField : public SetBase {
    public:
        void run() {
            BSONObj res = fromjson("{'_id':0, a:1}");
            client().insert( ns(), res );
            client().update( ns(), Query(), BSON( "$setOnInsert" << BSON( "b" << 1 ) ) );
            ASSERT( client().findOne( ns(), BSON( "a" << 1 ) ).woCompare( res ) == 0 );
        }
    };

    class SetOnInsertExisting : public SetBase {
    public:
        void run() {
            client().insert( ns(), BSON( "a" << 1 ) );
            client().update( ns(), Query(), BSON( "$setOnInsert" << BSON( "a" << 2 ) ) );
            ASSERT( !client().findOne( ns(), BSON( "a" << 1 ) ).isEmpty() );
        }
    };

    class SetOnInsertMixed : public SetBase {
    public:
        void run() {
            // Try with upsert false first.
            client().update( ns(), Query(), BSON( "$set" << BSON( "a" << 1 ) <<
                                                  "$setOnInsert" << BSON( "b" << 2 ) ), false );
            ASSERT( client().findOne( ns(), BSON( "a" << 1 << "b" << 2 ) ).isEmpty() );

            // Then with upsert true.
            client().update( ns(), Query(), BSON( "$set" << BSON( "a" << 1 ) <<
                                                  "$setOnInsert" << BSON( "b" << 2 ) ), true );
            ASSERT( !client().findOne( ns(), BSON( "a" << 1 << "b" << 2 ) ).isEmpty() );
        }
    };

    class SetOnInsertMissingParent : public SetBase {
    public:
        void run() {
            // In a mod that uses dontApply, we should be careful not to create a
            // parent unneccesarily.
            BSONObj initial = fromjson( "{'_id':0}" );
            BSONObj final = fromjson( "{'_id':0, d:1}" );
            client().insert( ns(), initial );
            client().update( ns(), initial, BSON( "$setOnInsert" << BSON( "a.b" << 1 ) <<
                                                  "$set" << BSON( "d" << 1 ) ) );
            ASSERT_EQUALS( client().findOne( ns(), initial ), final );
        }
    };

    class ModDotted : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{a:{b:4}}" ) );
            client().update( ns(), Query(), BSON( "$inc" << BSON( "a.b" << 10 ) ) );
            ASSERT( !client().findOne( ns(), BSON( "a.b" << 14 ) ).isEmpty() );
            client().update( ns(), Query(), BSON( "$set" << BSON( "a.b" << 55 ) ) );
            ASSERT( !client().findOne( ns(), BSON( "a.b" << 55 ) ).isEmpty() );
        }
    };

    class SetInPlaceDotted : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{a:{b:'cdef'}}" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "a.b" << "llll" ) ) );
            ASSERT( !client().findOne( ns(), BSON( "a.b" << "llll" ) ).isEmpty() );
        }
    };

    class SetRecreateDotted : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:{b:'cdef'}}" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "a.b" << "lllll" ) ) );
            ASSERT( client().findOne( ns(), BSON( "a.b" << "lllll" ) ).woCompare( fromjson( "{'_id':0,a:{b:'lllll'}}" ) ) == 0 );
        }
    };

    class SetMissingDotted : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0}" ) );
            client().update( ns(), BSONObj(), BSON( "$set" << BSON( "a.b" << "lllll" ) ) );
            ASSERT( client().findOne( ns(), BSON( "a.b" << "lllll" ) ).woCompare( fromjson( "{'_id':0,a:{b:'lllll'}}" ) ) == 0 );
        }
    };

    class SetAdjacentDotted : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:{c:4}}" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "a.b" << "lllll" ) ) );
            ASSERT_EQUALS( client().findOne( ns(), BSON( "a.b" << "lllll" ) ) , fromjson( "{'_id':0,a:{b:'lllll',c:4}}" ) );
        }
    };

    class IncMissing : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0}" ) );
            client().update( ns(), Query(), BSON( "$inc" << BSON( "f" << 3.0 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,f:3}" ) ) == 0 );
        }
    };

    class MultiInc : public SetBase {
    public:

        string s() {
            stringstream ss;
            auto_ptr<DBClientCursor> cc = client().query( ns() , Query().sort( BSON( "_id" << 1 ) ) );
            bool first = true;
            while ( cc->more() ) {
                if ( first ) first = false;
                else ss << ",";

                BSONObj o = cc->next();
                ss << o["x"].numberInt();
            }
            return ss.str();
        }

        void run() {
            client().insert( ns(), BSON( "_id" << 1 << "x" << 1 ) );
            client().insert( ns(), BSON( "_id" << 2 << "x" << 5 ) );

            ASSERT_EQUALS( "1,5" , s() );

            client().update( ns() , BSON( "_id" << 1 ) , BSON( "$inc" << BSON( "x" << 1 ) ) );
            ASSERT_EQUALS( "2,5" , s() );

            client().update( ns() , BSONObj() , BSON( "$inc" << BSON( "x" << 1 ) ) );
            ASSERT_EQUALS( "3,5" , s() );

            client().update( ns() , BSONObj() , BSON( "$inc" << BSON( "x" << 1 ) ) , false , true );
            ASSERT_EQUALS( "4,6" , s() );

        }
    };

    class UnorderedNewSet : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0}" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "f.g.h" << 3.0 << "f.g.a" << 2.0 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,f:{g:{a:2,h:3}}}" ) ) == 0 );
        }
    };

    class UnorderedNewSetAdjacent : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0}" ) );
            client().update( ns(), BSONObj(), BSON( "$set" << BSON( "f.g.h.b" << 3.0 << "f.g.a.b" << 2.0 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,f:{g:{a:{b:2},h:{b:3}}}}" ) ) == 0 );
        }
    };

    class ArrayEmbeddedSet : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,z:[4,'b']}" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "z.0" << "a" ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0,z:['a','b']}" ) );
        }
    };

    class AttemptEmbedInExistingNum : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:1}" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "a.b" << 1 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,a:1}" ) ) == 0 );
        }
    };

    class AttemptEmbedConflictsWithOtherSet : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0}" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "a" << 2 << "a.b" << 1 ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0}" ) );
        }
    };

    class ModMasksEmbeddedConflict : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:{b:2}}" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "a" << 2 << "a.b" << 1 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,a:{b:2}}" ) ) == 0 );
        }
    };

    class ModOverwritesExistingObject : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:{b:2}}" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "a" << BSON( "c" << 2 ) ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,a:{c:2}}" ) ) == 0 );
        }
    };

    class InvalidEmbeddedSet : public Fail {
    public:
        virtual void doIt() {
            client().update( ns(), Query(), BSON( "$set" << BSON( "a." << 1 ) ) );
        }
    };

    class UpsertMissingEmbedded : public SetBase {
    public:
        void run() {
            client().update( ns(), Query(), BSON( "$set" << BSON( "a.b" << 1 ) ), true );
            ASSERT( !client().findOne( ns(), QUERY( "a.b" << 1 ) ).isEmpty() );
        }
    };

    class Push : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1]}" ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << 5 ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0,a:[1,5]}" ) );
        }
    };

    class PushInvalidEltType : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:1}" ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << 5 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,a:1}" ) ) == 0 );
        }
    };

    class PushConflictsWithOtherMod : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1]}" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "a" << 1 ) <<"$push" << BSON( "a" << 5 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,a:[1]}" ) ) == 0 );
        }
    };

    class PushFromNothing : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0}" ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << 5 ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0,a:[5]}" ) );
        }
    };

    class PushFromEmpty : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[]}" ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << 5 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,a:[5]}" ) ) == 0 );
        }
    };

    class PushInsideNothing : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0}" ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a.b" << 5 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,a:{b:[5]}}" ) ) == 0 );
        }
    };

    class CantPushInsideOtherMod : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0}" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "a" << BSONObj() ) << "$push" << BSON( "a.b" << 5 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0}" ) ) == 0 );
        }
    };

    class CantPushTwice : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[]}" ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << 4 ) << "$push" << BSON( "a" << 5 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,a:[]}" ) ) == 0 );
        }
    };

    class SetEncapsulationConflictsWithExistingType : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:{b:4}}" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "a.b.c" << 4.0 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,a:{b:4}}" ) ) == 0 );
        }
    };

    class CantPushToParent : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:{b:4}}" ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << 4.0 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,a:{b:4}}" ) ) == 0 );
        }
    };

    class PushEachSimple : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1]}" ) );
            // { $push : { a : { $each : [ 2, 3 ] } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( 2 << 3 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0,a:[1,2,3]}" ) );
        }

    };

    class PushEachFromEmpty : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[]}" ) );
            // { $push : { a : { $each : [ 1, 2, 3 ] } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( 1 << 2 << 3 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0,a:[1,2,3]}" ) );
        }

    };

    class PushSliceBelowFull : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1]}" ) );
            // { $push : { a : { $each : [ 2 ] , $slice : -3 } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( 2 ) << "$slice" << -3 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0,a:[1,2]}" ) );
        }
    };

    class PushSliceReachedFullExact : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1]}" ) );
            // { $push : { a : { $each : [ 2 ] , $slice : -2 } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( 2 ) << "$slice" << -2 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0,a:[1,2]}" ) );
        }
    };

    class PushSliceReachedFullWithEach : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1]}" ) );
            // { $push : { a : { $each : [ 2 , 3 ] , $slice : -2 } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( 2 << 3 ) << "$slice" << -2 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0,a:[2,3]}" ) );
        }
    };

    class PushSliceReachedFullWithBoth : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1,2]}" ) );
            // { $push : { a : { $each : [ 3 ] , $slice : -2 } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( 3 ) << "$slice" << -2 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0,a:[2,3]}" ) );
        }
    };

    class PushSliceToZero : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1,2]}" ) );
            // { $push : { a : { $each : [ 3 ] , $slice : 0 } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( 3 ) << "$slice" << 0 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0,a:[]}" ) );
        }
    };

    class PushSliceToZeroFromNothing : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0}" ) );
            // { $push : { a : { $each : [ 3 ] , $slice : 0 } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( 3 ) << "$slice" << 0 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0,a:[]}" ) );
        }
    };

    class PushSliceFromNothing : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0}" ) );
            // { $push : { a : { $each : [ 1 , 2 ] , $slice : -3 } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( 1 << 2 ) << "$slice" << -3 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0,a:[1,2]}" ) );
        }
    };

    class PushSliceLongerThanSliceFromNothing : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0}" ) );
            // { $push : { a : { $each : [ 1 , 2 , 3 ] , $slice : -2 } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( 1 << 2 << 3 ) << "$slice" << -2 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0,a:[2,3]}" ) );
        }
    };

    class PushSliceFromEmpty : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[]}" ) );
            // { $push : { a : { $each : [ 1 ] , $slice : -3 } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( 1 ) << "$slice" << -3 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0,a:[1]}" ) );
        }
    };

    class PushSliceLongerThanSliceFromEmpty : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[]}" ) );
            // { $push : { a : { $each : [ 1 , 2 , 3 ] , $slice : -2 } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( 1 << 2 << 3 ) << "$slice" << -2 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson( "{'_id':0,a:[2,3]}" ) );
        }
    };

    class PushSliceTwoFields : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1,2],b:[3,4]}" ) );
            // { $push: { a: { $each: [ 5 ] , $slice : -2 }, { b: $each: [ 6 ] , $slice: -1 } } }
            BSONObj objA = BSON( "$each" << BSON_ARRAY( 5 ) << "$slice" << -2 );
            BSONObj objB = BSON( "$each" << BSON_ARRAY( 6 ) << "$slice" << -1 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << objA << "b" << objB ) ) );
            ASSERT_EQUALS( client().findOne( ns(), Query() ) , fromjson("{'_id':0,a:[2,5],b:[6]}"));
        }
    };

    class PushSliceAndNormal : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1,2],b:[3]}" ) );
            // { $push : { a : { $each : [ 5 ] , $slice : -2 } , { b : 4 } }
            BSONObj objA = BSON( "$each" << BSON_ARRAY( 5 ) << "$slice" << -2 );
            client().update( ns(), Query(), BSON("$push" << BSON("a" << objA << "b" << 4)));
            ASSERT_EQUALS(client().findOne(ns(), Query()) , fromjson("{'_id':0,a:[2,5],b:[3,4]}"));
        }
    };

    class PushSliceTwoFieldsConflict : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1],b:[3]}" ) );
            // { $push: { a: { $each: [ 5 ] , $slice: -2 } , { a: $each: [ 6 ] , $slice: -1 } } }
            BSONObj objA = BSON( "$each" << BSON_ARRAY( 5 ) << "$slice" << -2 );
            BSONObj other = BSON( "$each" << BSON_ARRAY( 6 ) << "$slice" << -1 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << objA << "a" << other ) ) );
            ASSERT(client().findOne( ns(), Query()).woCompare(fromjson("{'_id':0,a:[1],b:[3]}"))==0);
        }
    };

    class PushSliceAndNormalConflict : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1],b:[3]}" ) );
            // { $push : { a : { $each : [ 5 ] , $slice : -2 } , { a : 4 } } }
            BSONObj objA = BSON( "$each" << BSON_ARRAY( 5 ) << "$slice" << -2 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << objA << "a" << 4 ) ) );
            ASSERT(client().findOne( ns(), Query()).woCompare(fromjson("{'_id':0,a:[1],b:[3]}"))==0);
        }
    };

    class PushSliceInvalidEachType : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1,2]}" ) );
            // { $push : { a : { $each : 3 , $slice : -2 } } }
            BSONObj pushObj = BSON( "$each" << 3 << "$slice" << -2 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT( client().findOne(ns(), Query()).woCompare(fromjson("{'_id':0,a:[1,2]}")) == 0);
        }
    };

    class PushSliceInvalidSliceType : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1,2]}" ) );
            // { $push : { a : { $each : [ 3 ], $slice : [ -2 ] } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY(3) << "$slice" << BSON_ARRAY(-2) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare(fromjson("{'_id':0,a:[1,2]}")) == 0);
        }
    };

    class PushSliceInvalidSliceValue : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1,2]}" ) );
            // { $push : { a : { $each : [ 3 ], $slice : 2 } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY(3) << "$slice" << 2 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare(fromjson("{'_id':0,a:[1,2]}")) == 0);
        }
    };


    class PushSliceInvalidSliceDouble : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1,2]}" ) );
            // { $push : { a : { $each : [ 3 ], $slice : -2.1 } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY(3) << "$slice" << -2.1 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare(fromjson("{'_id':0,a:[1,2]}")) == 0);
        }
    };

    class PushSliceValidSliceDouble : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1,2]}" ) );
            // { $push : { a : { $each : [ 3 ], $slice : -2.0 } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY(3) << "$slice" << -2.0 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT_EQUALS(client().findOne(ns(), Query()) , fromjson("{'_id':0,a:[2,3]}"));
        }
    };

    class PushSliceInvalidSlice : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:[1,2]}" ) );
            // { $push : { a : { $each : [ 3 ], $xxxx :  2 } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY(3) << "$xxxx" << 2 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "a" << pushObj ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare(fromjson("{'_id':0,a:[1,2]}")) == 0);
        }
    };

    //
    // We'd like to test the ability of $push with $sort in the following sequence of tests. We
    // try to enumerate all the possibilities of where the final element would come from: the
    // document, the $push itself, or both.
    //

    class PushSortBase : public ClientBase {
    public:
        ~PushSortBase() {
            client().dropCollection( ns() );
        }

    protected:
        enum UpdateType {
            // Sorts ascending and slices the back of the array.
            TOPK_ASC = 0,

            // Sorts descending and slices the front of the array.
            TOPK_DESC = 1,

            // Sorts ascending and slices the back of the array.
            BOTTOMK_ASC = 2,

            // Sorts descending and slices the front of the array.
            BOTTOMK_DESC = 3
        };

        const char* ns() {
            return "unittest.updatetests.PushSortBase";
        }

        void setParams( const BSONArray& fields,
                        const BSONArray& values,
                        const BSONArray& sort,
                        int size ) {
            _fieldNames = fields;
            _fieldValues = values;
            _sortFields = sort;
            _sliceSize = size;
        }

        /**
         * Generates the update expression portion of an update command given one of the
         * possible types of update.
         */
        BSONObj getUpdate( int updateType ) {
            BSONObjBuilder updateBuilder;
            BSONObjBuilder pushBuilder( updateBuilder.subobjStart( "$push" ) );
            BSONObjBuilder fieldBuilder( pushBuilder.subobjStart( "x" ) );

            // Builds $each: [ {a:1,b:1,...}, {a:2,b:2,...}, ... ]
            BSONArrayBuilder eachBuilder( fieldBuilder.subarrayStart( "$each" ) );
            BSONObjIterator itVal( _fieldValues );
            while ( itVal.more() ) {
                BSONObjBuilder eachObjBuilder;
                BSONElement val = itVal.next();
                BSONObjIterator itName( _fieldNames );
                while ( itName.more() ) {
                    BSONElement name = itName.next();
                    eachObjBuilder.append( name.String(), val.Int() );
                }
                eachBuilder.append( eachObjBuilder.done() );
            }
            eachBuilder.done();

            // Builds $slice portion.
            fieldBuilder.append( "$slice",
                                 updateType < 2 ? -_sliceSize : _sliceSize);

            // Builds $sort: <sort pattern> portion
            BSONObjBuilder patternBuilder( fieldBuilder.subobjStart( "$sort" ) );
            BSONObjIterator itSort( _sortFields );
            while ( itSort.more() ) {
                BSONElement sortField = itSort.next();
                patternBuilder.append( sortField.String(),
                                       updateType%2 ? -1 : 1 );
            }
            patternBuilder.done();

            fieldBuilder.done();
            pushBuilder.done();

            return updateBuilder.obj();
        }

        void check( BSONObj expected ) {
            std::cout << expected.toString() << std::endl;
            std::cout << client().findOne( ns(), Query() ) << std::endl;
            ASSERT( client().findOne( ns(), Query() ).woCompare( expected ) == 0 );
        }

    private:
        BSONArray _fieldNames;
        BSONArray _fieldValues;
        BSONArray _sortFields;
        int _sliceSize;
    };

    class PushSortBelowFull : public PushSortBase {
    public:
        void run() {
            // With the following parameters
            //            fields in              values in
            //          the each array           each array       field to sort   size
            setParams( BSON_ARRAY( "a" << "b" ), BSON_ARRAY( 2 ), BSON_ARRAY( "b" ), 3 );

            // Generates the four variations below (but for now we're only using negative slice).
            // TOPK_ASC:     $push: { x: { $each: [ {a:2,b:2} ], $slice:-3, $sort: { b:1 } } }
            // TOPK_DESC:    $push: { x: { $each: [ {a:2,b:2} ], $slice:-3, $sort: { b:-1 } } }
            // BOTTOMK_ASC:  $push: { x: { $each: [ {a:2,b:2} ], $slice:3, $sort: { b:1 } } }
            // BOTTOMK_DESC: $push: { x: { $each: [ {a:2,b:2} ], $slice:3, $sort: { b:-1 } } }

            for ( int i = 0; i < 2; i++ ) { // i < 4 when we have positive $slice
                client().dropCollection( ns() );
                client().insert( ns(), fromjson( "{'_id':0,x:[{a:1,b:1}]}" ) );

                BSONObj result;
                BSONObj expected;
                switch ( i ) {
                case TOPK_ASC:
                case BOTTOMK_ASC:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected = fromjson( "{'_id':0,x:[{a:1,b:1},{a:2,b:2}]}" );
                    ASSERT_EQUALS( result, expected );
                    break;

                case TOPK_DESC:
                case BOTTOMK_DESC:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected =  fromjson( "{'_id':0,x:[{a:2,b:2},{a:1,b:1}]}" ) ;
                    ASSERT_EQUALS( result, expected );
                    break;
                }
            }
        }
    };

    class PushSortReachedFullExact : public PushSortBase {
    public:
        void run() {
            // With the following parameters
            //            fields in           values in
            //          the each array        each array       field to sort   size
            setParams(BSON_ARRAY( "a"<<"b" ), BSON_ARRAY( 2 ), BSON_ARRAY( "b" ), 2 );

            // Generates the four variations below (but for now we're only using negative slice).
            // TOPK_ASC:     $push: { x: { $each: [ {a:2,b:2} ], $slice:-2, $sort: { b:1 } } }
            // TOPK_DESC:    $push: { x: { $each: [ {a:2,b:2} ], $slice:-2, $sort: { b:-1 } } }
            // BOTTOMK_ASC:  $push: { x: { $each: [ {a:2,b:2} ], $slice:2, $sort: { b:1 } } }
            // BOTTOMK_DESC: $push: { x: { $each: [ {a:2,b:2} ], $slice:2, $sort: { b:-1 } } }

            for ( int i = 0; i < 2; i++ ) {  // i < 4 when we have positive $slice
                client().dropCollection( ns() );
                client().insert( ns(), fromjson( "{'_id':0,x:[{a:1,b:1}]}" ) );

                BSONObj result;
                BSONObj expected;
                switch (i) {
                case TOPK_ASC:
                case BOTTOMK_ASC:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected = fromjson( "{'_id':0,x:[{a:1,b:1},{a:2,b:2}]}" );
                    ASSERT_EQUALS( result, expected );
                    break;

                case TOPK_DESC:
                case BOTTOMK_DESC:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected = fromjson( "{'_id':0,x:[{a:2,b:2},{a:1,b:1}]}" );
                    ASSERT_EQUALS( result, expected );
                    break;
                }
            }
        }
    };

    class PushSortReachedFullWithBoth : public PushSortBase {
    public:
        void run() {
            // With the following parameters
            //            fields in            values in
            //          the each array         each array       field to sort   size
            setParams( BSON_ARRAY( "a"<<"b" ), BSON_ARRAY( 2 ), BSON_ARRAY( "b" ), 2 );

            // Generates the four variations below (but for now we're only using negative slice).
            // TOPK_ASC:     $push: { x: { $each: [ {a:2,b:2} ], $slice:-2, $sort: { b:1 } } }
            // TOPK_DESC:    $push: { x: { $each: [ {a:2,b:2} ], $slice:-2, $sort: { b:-1 } } }
            // BOTTOMK_ASC:  $push: { x: { $each: [ {a:2,b:2} ], $slice:2, $sort: { b:1 } } }
            // BOTTOMK_DESC: $push: { x: { $each: [ {a:2,b:2} ], $slice:2, $sort: { b:-1 } } }

            for ( int i = 0; i < 2; i++ ) {  // i < 4 when we have positive $slice
                client().dropCollection( ns() );
                client().insert( ns(), fromjson( "{'_id':0,x:[{a:1,b:1},{a:3,b:3}]}" ) );

                BSONObj result;
                BSONObj expected;
                switch ( i ) {
                case TOPK_ASC:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected = fromjson( "{'_id':0,x:[{a:2,b:2},{a:3,b:3}]}" );
                    ASSERT_EQUALS( result, expected );
                    break;

                case TOPK_DESC:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected = fromjson( "{'_id':0,x:[{a:2,b:2},{a:1,b:1}]}" );
                    ASSERT_EQUALS( result, expected );
                    break;

                case BOTTOMK_ASC:
                case BOTTOMK_DESC:
                    // Implement me.
                    break;
                }
            }
        }
    };

    class PushSortToZero : public PushSortBase {
    public:
        void run() {
            // With the following parameters
            //            fields in           values in
            //          the each array        each array      field to sort      size
            setParams( BSON_ARRAY( "a"<<"b" ), BSON_ARRAY( 2 ), BSON_ARRAY( "b" ), 0 );

            // Generates the four variations below (but for now we're only using negative slice).
            // TOPK_ASC:     $push: { x: { $each: [ {a:2,b:2} ], $slice:0, $sort: { b:1 } } }
            // TOPK_DESC:    $push: { x: { $each: [ {a:2,b:2} ], $slice:0, $sort: { b:-1 } } }
            // BOTTOMK_ASC:  $push: { x: { $each: [ {a:2,b:2} ], $slice:0, $sort: { b:1 } } }
            // BOTTOMK_DESC: $push: { x: { $each: [ {a:2,b:2} ], $slice:0, $sort: { b:-1 } } }

            for ( int i = 0; i < 2; i++ ) {  // i < 4 when we have positive $slice
                client().dropCollection( ns() );
                client().insert( ns(), fromjson( "{'_id':0,x:[{a:1,b:1},{a:3,b:3}]}" ) );

                BSONObj result;
                BSONObj expected;
                switch ( i ) {
                default:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected = fromjson( "{'_id':0,x:[]}" );
                    ASSERT_EQUALS( result, expected );
                    break;
                }
            }
        }
    };

    class PushSortToZeroFromNothing : public PushSortBase {
    public:
        void run() {
            // With the following parameters
            //            fields in           values in
            //          the each array        each array      field to sort       size
            setParams( BSON_ARRAY( "a"<<"b" ), BSON_ARRAY( 2 ), BSON_ARRAY( "b" ), 0 );

            // Generates the four variations below (but for now we're only using negative slice).
            // TOPK_ASC:     $push: { x: { $each: [ {a:2,b:2} ], $slice:0, $sort: { b:1 } } }
            // TOPK_DESC:    $push: { x: { $each: [ {a:2,b:2} ], $slice:0, $sort: { b:-1 } } }
            // BOTTOMK_ASC:  $push: { x: { $each: [ {a:2,b:2} ], $slice:0, $sort: { b:1 } } }
            // BOTTOMK_DESC: $push: { x: { $each: [ {a:2,b:2} ], $slice:0, $sort: { b:-1 } } }

            for ( int i = 0; i < 2; i++ ) {  // i < 4 when we have positive $slice
                client().dropCollection( ns() );
                client().insert( ns(), fromjson( "{'_id':0}" ) );

                BSONObj result;
                BSONObj expected;
                switch ( i ) {
                default:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected = fromjson( "{'_id':0,x:[]}" );
                    ASSERT_EQUALS( result, expected );
                    break;
                }
            }
        }
    };

    class PushSortFromNothing : public PushSortBase {
    public:
        void run() {
            // With the following parameters
            //            fields in           values in
            //          the each array        each array            field to sort     size
            setParams(BSON_ARRAY( "a"<<"b" ), BSON_ARRAY( 2 << 1 ), BSON_ARRAY( "b" ), 2 );

            // Generates the four variations below (but for now we're only using negative slice).
            // <genarr> = [ {a:2,b:2}, {a:1,b:1} ]
            // Generates the four variations below
            // TOPK_ASC:     $push: { x: { $each: [ <genarray> ], $slice:-2, $sort: { b:1 } } }
            // TOPK_DESC:    $push: { x: { $each: [ <genarray> ], $slice:-2, $sort: { b:-1 } } }
            // BOTTOMK_ASC:  $push: { x: { $each: [ <genarray> ], $slice:2, $sort: { b:1 } } }
            // BOTTOMK_DESC: $push: { x: { $each: [ <genarray> ], $slice:2, $sort: { b:-1 } } }

            for ( int i = 0; i < 2; i++ ) {  // i < 4 when we have positive $slice
                client().dropCollection( ns() );
                client().insert( ns(), fromjson( "{'_id':0}" ) );

                BSONObj result;
                BSONObj expected;
                switch (i) {
                case TOPK_ASC:
                case BOTTOMK_ASC:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected = fromjson( "{'_id':0,x:[{a:1,b:1},{a:2,b:2}]}" );
                    ASSERT_EQUALS( result, expected );
                    break;

                case TOPK_DESC:
                case BOTTOMK_DESC:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected = fromjson( "{'_id':0,x:[{a:2,b:2},{a:1,b:1}]}" );
                    ASSERT_EQUALS( result, expected );
                    break;
                }
            }
        }
    };

    class PushSortLongerThanSliceFromNothing : public PushSortBase {
    public:
        void run() {
            // With the following parameters
            //            fields in           values in
            //          the each array        each array                field to sort     size
            setParams(BSON_ARRAY( "a"<<"b" ), BSON_ARRAY( 2 << 1 << 3), BSON_ARRAY( "b" ), 2 );

            // Generates the four variations below (but for now we're only using negative slice).
            // <genarr> = [ {a:2,b:2}, {a:1,b:1}, {a:3,b:3} ]
            // TOPK_ASC:     $push: { x: { $each: [ <genarray> ], $slice:-2, $sort: { b:1 } } }
            // TOPK_DESC:    $push: { x: { $each: [ <genarray> ], $slice:-2, $sort: { b:-1 } } }
            // BOTTOMK_ASC:  $push: { x: { $each: [ <genarray> ], $slice:2, $sort: { b:1 } } }
            // BOTTOMK_DESC: $push: { x: { $each: [ <genarray> ], $slice:2, $sort: { b:-1 } } }

            for ( int i = 0; i < 2; i++ ) {  // i < 4 when we have positive $slice
                client().dropCollection( ns() );
                client().insert( ns(), fromjson( "{'_id':0}" ) );

                BSONObj result;
                BSONObj expected;
                switch (i) {
                case TOPK_ASC:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected = fromjson( "{'_id':0,x:[{a:2,b:2},{a:3,b:3}]}" );
                    ASSERT_EQUALS( result, expected );
                    break;

                case TOPK_DESC:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected = fromjson( "{'_id':0,x:[{a:2,b:2},{a:1,b:1}]}" );
                    ASSERT_EQUALS( result, expected );
                    break;

                case BOTTOMK_ASC:
                case BOTTOMK_DESC:
                    // Implement me.
                    break;
                }
            }
        }
    };

    class PushSortFromEmpty : public PushSortBase {
    public:
        void run() {
            // With the following parameters
            //            fields in           values in
            //          the each array        each array            field to sort     size
            setParams(BSON_ARRAY( "a"<<"b" ), BSON_ARRAY( 2 << 1 ), BSON_ARRAY( "b" ), 2 );

            // Generates the four variations below (but for now we're only using negative slice).
            // <genarr> = [ {a:2,b:2}, {a:1,b:1} ]
            // TOPK_ASC:     $push: { x: { $each: [ <genarray> ], $slice:-2, $sort: { b:1 } } }
            // TOPK_DESC:    $push: { x: { $each: [ <genarray> ], $slice:-2, $sort: { b:-1 } } }
            // BOTTOMK_ASC:  $push: { x: { $each: [ <genarray> ], $slice:2, $sort: { b:1 } } }
            // BOTTOMK_DESC: $push: { x: { $each: [ <genarray> ], $slice:2, $sort: { b:-1 } } }

            for ( int i = 0; i < 2; i++ ) {  // i < 4 when we have positive $slice
                client().dropCollection( ns() );
                client().insert( ns(), fromjson( "{'_id':0,x:[]}" ) );

                BSONObj result;
                BSONObj expected;
                switch (i) {
                case TOPK_ASC:
                case BOTTOMK_ASC:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected = fromjson( "{'_id':0,x:[{a:1,b:1},{a:2,b:2}]}" );
                    ASSERT_EQUALS( result, expected );
                    break;

                case TOPK_DESC:
                case BOTTOMK_DESC:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected = fromjson( "{'_id':0,x:[{a:2,b:2},{a:1,b:1}]}" );
                    ASSERT_EQUALS( result, expected );
                    break;
                }
            }
        }
    };

    class PushSortLongerThanSliceFromEmpty : public PushSortBase {
    public:
        void run() {
            // With the following parameters
            //            fields in           values in
            //          the each array        each array                 field to sort   size
            setParams(BSON_ARRAY( "a"<<"b" ), BSON_ARRAY( 2 << 1 << 3), BSON_ARRAY( "b" ), 2 );

            // Generates the four variations below (but for now we're only using negative slice).
            // <genarr> = [ {a:2,b:2}, {a:1,b:1}, {a:3,b:3} ]
            // TOPK_ASC:     $push: { x: { $each: [ <genarray> ], $slice:-2, $sort: { b:1 } } }
            // TOPK_DESC:    $push: { x: { $each: [ <genarray> ], $slice:-2, $sort: { b:-1 } } }
            // BOTTOMK_ASC:  $push: { x: { $each: [ <genarray> ], $slice:2, $sort: { b:1 } } }
            // BOTTOMK_DESC: $push: { x: { $each: [ <genarray> ], $slice:2, $sort: { b:-1 } } }

            for ( int i = 0; i < 2; i++ ) {  // i < 4 when we have positive $slice
                client().dropCollection( ns() );
                client().insert( ns(), fromjson( "{'_id':0,x:[]}" ) );

                BSONObj result;
                BSONObj expected;
                switch (i) {
                case TOPK_ASC:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected = fromjson( "{'_id':0,x:[{a:2,b:2},{a:3,b:3}]}" );
                    ASSERT_EQUALS( result, expected );
                    break;

                case TOPK_DESC:
                    client().update( ns(), Query(), getUpdate(i) );
                    result = client().findOne( ns(), Query() );
                    expected = fromjson( "{'_id':0,x:[{a:2,b:2},{a:1,b:1}]}" );
                    ASSERT_EQUALS( result, expected );
                    break;

                case BOTTOMK_ASC:
                case BOTTOMK_DESC:
                    // Implement me.
                    break;
                }
            }
        }
    };

    class PushSortSortMixed  {
    public:
        void run() {
            BSONObj objs[3];
            objs[0] = fromjson( "{a:1, b:1}" );
            objs[1] = fromjson( "{a:3, b:1}" );
            objs[2] = fromjson( "{a:2, b:3}" );

            vector<BSONObj> workArea;
            for ( int i = 0; i < 3; i++ ) {
                workArea.push_back( objs[i] );
            }

            sort( workArea.begin(), workArea.end(), ProjectKeyCmp( BSON("b" << 1 << "a" << -1) ) );

            ASSERT_EQUALS( workArea[0], objs[1] );
            ASSERT_EQUALS( workArea[1], objs[0] );
            ASSERT_EQUALS( workArea[2], objs[2] );
        }
    };

    class PushSortSortOutOfOrderFields {
    public:
        void run() {
            BSONObj objs[3];
            objs[0] = fromjson( "{b:1, a:1}" );
            objs[1] = fromjson( "{a:3, b:2}" );
            objs[2] = fromjson( "{b:3, a:2}" );

            vector<BSONObj> workArea;
            for ( int i = 0; i < 3; i++ ) {
                workArea.push_back( objs[i] );
            }

            sort( workArea.begin(), workArea.end(), ProjectKeyCmp( BSON("a" << 1 << "b" << 1) ) );

            ASSERT_EQUALS( workArea[0], objs[0] );
            ASSERT_EQUALS( workArea[1], objs[2] );
            ASSERT_EQUALS( workArea[2], objs[1] );
        }
    };

    class PushSortSortExtraFields {
    public:
        void run() {
            BSONObj objs[3];
            objs[0] = fromjson( "{b:1, c:2, a:1}" );
            objs[1] = fromjson( "{c:1, a:3, b:2}" );
            objs[2] = fromjson( "{b:3, a:2}" );

            vector<BSONObj> workArea;
            for ( int i = 0; i < 3; i++ ) {
                workArea.push_back( objs[i] );
            }

            sort( workArea.begin(), workArea.end(), ProjectKeyCmp( BSON("a" << 1 << "b" << 1) ) );

            ASSERT_EQUALS( workArea[0], objs[0] );
            ASSERT_EQUALS( workArea[1], objs[2] );
            ASSERT_EQUALS( workArea[2], objs[1] );
        }
    };

    class PushSortSortMissingFields {
    public:
        void run() {
            BSONObj objs[3];
            objs[0] = fromjson( "{a:2, b:2}" );
            objs[1] = fromjson( "{a:1}" );
            objs[2] = fromjson( "{a:3, b:3, c:3}" );

            vector<BSONObj> workArea;
            for ( int i = 0; i < 3; i++ ) {
                workArea.push_back( objs[i] );
            }

            sort( workArea.begin(), workArea.end(), ProjectKeyCmp( BSON("b" << 1 << "c" << 1) ) );

            ASSERT_EQUALS( workArea[0], objs[1] );
            ASSERT_EQUALS( workArea[1], objs[0] );
            ASSERT_EQUALS( workArea[2], objs[2] );
        }
    };

    class PushSortSortNestedFields {
    public:
        void run() {
            BSONObj objs[3];
            objs[0] = fromjson( "{a:{b:{c:2, d:0}}}" );
            objs[1] = fromjson( "{a:{b:{c:1, d:2}}}" );
            objs[2] = fromjson( "{a:{b:{c:3, d:1}}}" );

            vector<BSONObj> workArea;
            for ( int i = 0; i < 3; i++ ) {
                workArea.push_back( objs[i] );
            }

            sort( workArea.begin(), workArea.end(), ProjectKeyCmp( fromjson( "{'a.b.d':-1}" ) ) );

            ASSERT_EQUALS( workArea[0], objs[1] );
            ASSERT_EQUALS( workArea[1], objs[2] );
            ASSERT_EQUALS( workArea[2], objs[0] );

            sort( workArea.begin(), workArea.end(), ProjectKeyCmp( fromjson( "{'a.b':1}" ) ) );

            ASSERT_EQUALS( workArea[0], objs[1] );
            ASSERT_EQUALS( workArea[1], objs[0] );
            ASSERT_EQUALS( workArea[2], objs[2] );

        }
    };

    class PushSortInvalidSortPattern : public SetBase {
    public:
        void run() {
            // Sort pattern validation is made during update command checking. Therefore, to
            // catch bad patterns, we have to write updated that use them.

            BSONObj expected = fromjson( "{'_id':0,x:[{a:1}, {a:2}]}" );
            client().insert( ns(), expected );

            // { $push : { x : { $each : [ {a:3} ], $slice:-2, $sort : {a..d:1} } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( BSON( "a" << 3 ) ) <<
                                    "$slice" << -2 <<
                                    "$sort" << BSON( "a..d" << 1 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            BSONObj result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );


            // { $push : { x : { $each : [ {a:3} ], $slice:-2, $sort : {a.:1} } } }
            pushObj = BSON( "$each" << BSON_ARRAY( BSON( "a" << 3 ) ) <<
                                    "$slice" << -2 <<
                                    "$sort" << BSON( "a." << 1 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );

            // { $push : { x : { $each : [ {a:3} ], $slice:-2, $sort : {.b:1} } } }
            pushObj = BSON( "$each" << BSON_ARRAY( BSON( "a" << 3 ) ) <<
                                    "$slice" << -2 <<
                                    "$sort" << BSON( ".b" << 1 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );

            // { $push : { x : { $each : [ {a:3} ], $slice:-2, $sort : {.:1} } } }
            pushObj = BSON( "$each" << BSON_ARRAY( BSON( "a" << 3 ) ) <<
                                    "$slice" << -2 <<
                                    "$sort" << BSON( "." << 1 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );

            // { $push : { x : { $each : [ {a:3} ], $slice:-2, $sort : {'':1} } } }
            pushObj = BSON( "$each" << BSON_ARRAY( BSON( "a" << 3 ) ) <<
                                    "$slice" << -2 <<
                                    "$sort" << BSON( "" << 1 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );
        }
    };

    class PushSortInvalidEachType : public SetBase {
    public:
        void run() {
            BSONObj expected = fromjson( "{'_id':0,x:[{a:1},{a:2}]}" );
            client().insert( ns(), expected );
            // { $push : { x : { $each : [ 3 ], $slice:-2, $sort : {a:1} } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( 3 ) <<
                                    "$slice" << -2 <<
                                    "$sort" << BSON( "a" << 1 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            BSONObj result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );
        }
    };

    class PushSortInvalidBaseArray : public SetBase {
    public:
        void run() {
            BSONObj expected = fromjson( "{'_id':0,x:[1,2]}" );
            client().insert( ns(), expected );
            // { $push : { x : { $each : [ {a:3} ], $slice:-2, $sort : {a:1} } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( BSON( "a" << 3 ) ) <<
                                    "$slice" << -2 <<
                                    "$sort" << BSON( "a" << 1 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            BSONObj result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );
        }
    };

    class PushSortInvalidSortType : public SetBase {
    public:
        void run() {
            BSONObj expected = fromjson( "{'_id':0,x:[{a:1},{a:2}]}" );
            client().insert( ns(), expected );
            // { $push : { x : { $each : [ {a:3} ], $slice:-2, $sort : 2} } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( BSON( "a" << 3 ) ) <<
                                    "$slice" << -2 <<
                                    "$sort" << 2 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            BSONObj result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );
        }
    };

    class PushSortInvalidSortValue : public SetBase {
    public:
        void run() {
            BSONObj expected = fromjson( "{'_id':0,x:[{a:1},{a:2}]}" );
            client().insert( ns(), expected );
            // { $push : { x : { $each : [ {a:3} ], $slice:2, $sort : {a:1} } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( BSON( "a" << 3 ) ) <<
                                    "$slice" << 2 <<
                                    "$sort" << BSON( "a" << 1 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            BSONObj result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );
        }
    };

    class PushSortInvalidSortDouble : public SetBase {
    public:
        void run() {
            BSONObj expected = fromjson( "{'_id':0,x:[{a:1},{a:2}]}" );
            client().insert( ns(), expected );
            // { $push : { x : { $each : [ {a:3} ], $slice:-2.1, $sort : {a:1} } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( BSON( "a" << 3 ) ) <<
                                    "$slice" << -2.1 <<
                                    "$sort" << BSON( "a" << 1 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            BSONObj result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );
        }
    };

    class PushSortValidSortDouble : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,x:[{a:1},{a:2}]}" ) );
            // { $push : { x : { $each : [ {a:3} ], $slice:-2.0, $sort : {a:1} } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( BSON( "a" << 3 ) ) <<
                                    "$slice" << -2.0 <<
                                    "$sort" << BSON( "a" << 1 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            BSONObj expected = fromjson( "{'_id':0,x:[{a:2},{a:3}]}" );
            BSONObj result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );
        }
    };

    class PushSortInvalidSortSort : public SetBase {
    public:
        void run() {
            BSONObj expected = fromjson( "{'_id':0,x:[{a:1},{a:2}]}" );
            client().insert( ns(), expected );
            // { $push : { x : { $each : [ {a:3} ], $slice:-2.0, $sort : [2, 1] } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( BSON( "a" << 3 ) ) <<
                                    "$slice" << -2.0 <<
                                    "$sort" << BSON_ARRAY( 2 << 1 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            BSONObj result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );
        }
    };

    class PushSortInvalidSortSortOrder : public SetBase {
    public:
        void run() {
            BSONObj expected = fromjson( "{'_id':0,x:[{a:1},{a:2}]}" );
            client().insert( ns(), expected );
            // { $push : { x : { $each : [ {a:3} ], $slice:-2, $sort : {a:10} } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( BSON( "a" << 3 ) ) <<
                                    "$slice" << -2 <<
                                    "$sort" << BSON( "a" << 10 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            BSONObj result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );
        }
    };

    class PushSortInvertedSortAndSlice : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,x:[{a:1},{a:3}]}" ) );
            // { $push : { x : { $each : [ {a:2} ], $sort: {a:1}, $slice:-2 } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( BSON( "a" << 2 ) ) <<
                                    "$sort" << BSON( "a" << 1 ) <<
                                    "$slice" << -2.0 );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            BSONObj expected = fromjson( "{'_id':0,x:[{a:2},{a:3}]}" );
            BSONObj result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );

        }
    };

    class PushSortInvalidDuplicatedSort : public SetBase {
    public:
        void run() {
            BSONObj expected = fromjson( "{'_id':0,x:[{a:1},{a:3}]}" );
            client().insert( ns(), expected );
            // { $push : { x : { $each : [ {a:2} ], $sort : {a:1}, $sort: {a:1} } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( BSON( "a" << 2 ) ) <<
                                    "$sort" << BSON( "a" << 1 ) <<
                                    "$sort" << BSON( "a" << 1 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            BSONObj result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );

        }
    };

    class PushSortInvalidMissingSliceTo : public SetBase {
    public:
        void run() {
            BSONObj expected = fromjson( "{'_id':0,x:[{a:1},{a:3}]}" );
            client().insert( ns(), expected );
            // { $push : { x : { $each : [ {a:2} ], $sort : {a:1} } } }
            BSONObj pushObj = BSON( "$each" << BSON_ARRAY( BSON( "a" << 2 ) ) <<
                                    "$sort" << BSON( "a" << 1 ) );
            client().update( ns(), Query(), BSON( "$push" << BSON( "x" << pushObj ) ) );
            BSONObj result = client().findOne( ns(), Query() );
            ASSERT_EQUALS( result, expected );

        }
    };

    class CantIncParent : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:{b:4}}" ) );
            client().update( ns(), Query(), BSON( "$inc" << BSON( "a" << 4.0 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,a:{b:4}}" ) ) == 0 );
        }
    };

    class DontDropEmpty : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:{b:{}}}" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "a.c" << 4.0 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,a:{b:{},c:4}}" ) ) == 0 );
        }
    };

    class InsertInEmpty : public SetBase {
    public:
        void run() {
            client().insert( ns(), fromjson( "{'_id':0,a:{b:{}}}" ) );
            client().update( ns(), Query(), BSON( "$set" << BSON( "a.b.f" << 4.0 ) ) );
            ASSERT( client().findOne( ns(), Query() ).woCompare( fromjson( "{'_id':0,a:{b:{f:4}}}" ) ) == 0 );
        }
    };

    class IndexParentOfMod : public SetBase {
    public:
        void run() {
            client().ensureIndex( ns(), BSON( "a" << 1 ) );
            client().insert( ns(), fromjson( "{'_id':0}" ) );
            client().update( ns(), Query(), fromjson( "{$set:{'a.b':4}}" ) );
            ASSERT_EQUALS( fromjson( "{'_id':0,a:{b:4}}" ) , client().findOne( ns(), Query() ) );
            ASSERT_EQUALS( fromjson( "{'_id':0,a:{b:4}}" ) , client().findOne( ns(), fromjson( "{'a.b':4}" ) ) ); // make sure the index works
        }
    };

    class IndexModSet : public SetBase {
    public:
        void run() {
            client().ensureIndex( ns(), BSON( "a.b" << 1 ) );
            client().insert( ns(), fromjson( "{'_id':0,a:{b:3}}" ) );
            client().update( ns(), Query(), fromjson( "{$set:{'a.b':4}}" ) );
            ASSERT_EQUALS( fromjson( "{'_id':0,a:{b:4}}" ) , client().findOne( ns(), Query() ) );
            ASSERT_EQUALS( fromjson( "{'_id':0,a:{b:4}}" ) , client().findOne( ns(), fromjson( "{'a.b':4}" ) ) ); // make sure the index works
        }
    };


    class PreserveIdWithIndex : public SetBase { // Not using $set, but base class is still useful
    public:
        void run() {
            client().insert( ns(), BSON( "_id" << 55 << "i" << 5 ) );
            client().update( ns(), BSON( "i" << 5 ), BSON( "i" << 6 ) );
            ASSERT( !client().findOne( ns(), Query( BSON( "_id" << 55 ) ).hint
                                       ( "{\"_id\":ObjectId(\"000000000000000000000000\")}" ) ).isEmpty() );
        }
    };

    class CheckNoMods : public SetBase {
    public:
        void run() {
            client().update( ns(), BSONObj(), BSON( "i" << 5 << "$set" << BSON( "q" << 3 ) ), true );
            ASSERT( error() );
        }
    };

    class UpdateMissingToNull : public SetBase {
    public:
        void run() {
            client().insert( ns(), BSON( "a" << 5 ) );
            client().update( ns(), BSON( "a" << 5 ), fromjson( "{$set:{b:null}}" ) );
            ASSERT_EQUALS( jstNULL, client().findOne( ns(), QUERY( "a" << 5 ) ).getField( "b" ).type() );
        }
    };

    /** SERVER-4777 */
    class TwoModsWithinDuplicatedField : public SetBase {
    public:
        void run() {
            client().insert( ns(), BSON( "_id" << 0 << "a" << 1
                                        << "x" << BSONObj() << "x" << BSONObj()
                                        << "z" << 5 ) );
            client().update( ns(), BSONObj(), BSON( "$set" << BSON( "x.b" << 1 << "x.c" << 1 ) ) );
            ASSERT_EQUALS( BSON( "_id" << 0 << "a" << 1
                                << "x" << BSON( "b" << 1 << "c" << 1 ) << "x" << BSONObj()
                                << "z" << 5 ),
                          client().findOne( ns(), BSONObj() ) );
        }
    };

    /** SERVER-4777 */
    class ThreeModsWithinDuplicatedField : public SetBase {
    public:
        void run() {
            client().insert( ns(),
                            BSON( "_id" << 0
                                 << "x" << BSONObj() << "x" << BSONObj() << "x" << BSONObj() ) );
            client().update( ns(), BSONObj(),
                            BSON( "$set" << BSON( "x.b" << 1 << "x.c" << 1 << "x.d" << 1 ) ) );
            ASSERT_EQUALS( BSON( "_id" << 0
                                << "x" << BSON( "b" << 1 << "c" << 1 << "d" << 1 )
                                << "x" << BSONObj() << "x" << BSONObj() ),
                          client().findOne( ns(), BSONObj() ) );
        }
    };

    class TwoModsBeforeExistingField : public SetBase {
    public:
        void run() {
            client().insert( ns(), BSON( "_id" << 0 << "x" << 5 ) );
            client().update( ns(), BSONObj(),
                            BSON( "$set" << BSON( "a" << 1 << "b" << 1 << "x" << 10 ) ) );
            ASSERT_EQUALS( BSON( "_id" << 0 << "a" << 1 << "b" << 1 << "x" << 10 ),
                          client().findOne( ns(), BSONObj() ) );
        }
    };
    
    namespace ModSetTests {

        class internal1 {
        public:
            void run() {
                BSONObj b = BSON( "$inc" << BSON( "x" << 1 << "a.b" << 1 ) );
                ModSet m(b);

                ASSERT( m.haveModForField( "x" ) );
                ASSERT( m.haveModForField( "a.b" ) );
                ASSERT( ! m.haveModForField( "y" ) );
                ASSERT( ! m.haveModForField( "a.c" ) );
                ASSERT( ! m.haveModForField( "a" ) );

                ASSERT( m.haveConflictingMod( "x" ) );
                ASSERT( m.haveConflictingMod( "a" ) );
                ASSERT( m.haveConflictingMod( "a.b" ) );
                ASSERT( ! m.haveConflictingMod( "a.bc" ) );
                ASSERT( ! m.haveConflictingMod( "a.c" ) );
                ASSERT( ! m.haveConflictingMod( "a.a" ) );
            }
        };

        class Base {
        public:

            virtual ~Base() {}


            void test( BSONObj morig , BSONObj in , BSONObj wanted ) {
                BSONObj m = morig.copy();
                ModSet set(m);

                BSONObj out = set.prepare(in)->createNewFromMods();
                ASSERT_EQUALS( wanted , out );
            }
        };

        class inc1 : public Base {
        public:
            void run() {
                BSONObj m = BSON( "$inc" << BSON( "x" << 1 ) );
                test( m , BSON( "x" << 5 )  , BSON( "x" << 6 ) );
                test( m , BSON( "a" << 5 )  , BSON( "a" << 5 << "x" << 1 ) );
                test( m , BSON( "z" << 5 )  , BSON( "x" << 1 << "z" << 5 ) );
            }
        };

        class inc2 : public Base {
        public:
            void run() {
                BSONObj m = BSON( "$inc" << BSON( "a.b" << 1 ) );
                test( m , BSONObj() , BSON( "a" << BSON( "b" << 1 ) ) );
                test( m , BSON( "a" << BSON( "b" << 2 ) ) , BSON( "a" << BSON( "b" << 3 ) ) );

                m = BSON( "$inc" << BSON( "a.b" << 1 << "a.c" << 1 ) );
                test( m , BSONObj() , BSON( "a" << BSON( "b" << 1 << "c" << 1 ) ) );


            }
        };

        class set1 : public Base {
        public:
            void run() {
                test( BSON( "$set" << BSON( "x" << 17 ) ) , BSONObj() , BSON( "x" << 17 ) );
                test( BSON( "$set" << BSON( "x" << 17 ) ) , BSON( "x" << 5 ) , BSON( "x" << 17 ) );

                test( BSON( "$set" << BSON( "x.a" << 17 ) ) , BSON( "z" << 5 ) , BSON( "x" << BSON( "a" << 17 )<< "z" << 5 ) );
            }
        };

        class push1 : public Base {
        public:
            void run() {
                test( BSON( "$push" << BSON( "a" << 5 ) ) , fromjson( "{a:[1]}" ) , fromjson( "{a:[1,5]}" ) );
            }
        };

        class PositionalWithoutElemMatchKey {
        public:
            void run() {
                BSONObj querySpec = BSONObj();
                BSONObj modSpec = BSON( "$set" << BSON( "a.$" << 1 ) );
                ModSet modSet( modSpec );

                // A positional operator must be replaced with an array index before calling
                // prepare().
                ASSERT_THROWS( modSet.prepare( querySpec ), UserException );
            }
        };

        class PositionalWithoutNestedElemMatchKey {
        public:
            void run() {
                BSONObj querySpec = BSONObj();
                BSONObj modSpec = BSON( "$set" << BSON( "a.b.c.$.e.f" << 1 ) );
                ModSet modSet( modSpec );

                // A positional operator must be replaced with an array index before calling
                // prepare().
                ASSERT_THROWS( modSet.prepare( querySpec ), UserException );
            }
        };

        class DbrefPassesPositionalValidation {
        public:
            void run() {
                BSONObj querySpec = BSONObj();
                BSONObj modSpec = BSON( "$set" << BSON( "a.$ref" << "foo" << "a.$id" << 0 ) );
                ModSet modSet( modSpec );

                // A positional operator must be replaced with an array index before calling
                // prepare(), but $ prefixed fields encoding dbrefs are allowed.
                modSet.prepare( querySpec ); // Does not throw.
            }
        };

        class CreateNewFromQueryExcludeNot {
        public:
            void run() {
                BSONObj querySpec = BSON( "a" << BSON( "$not" << BSON( "$lt" << 1 ) ) );
                BSONObj modSpec = BSON( "$set" << BSON( "b" << 1 ) );
                ModSet modSet( modSpec );

                // Because a $not operator is applied to the 'a' field, the 'a' field is excluded
                // from the resulting document.
                ASSERT_EQUALS( BSON( "b" << 1 ), modSet.createNewFromQuery( querySpec ) );
            }
        };
    };

    namespace Invertible {
        class SimpleInc {
        public:
            void run() {
                BSONObj mods = BSON("$inc" << BSON("a" << 1));
                ASSERT_EQUALS(invertUpdateMods(mods),
                              BSON("$inc" << BSON("a" << -1)));

                mods = BSON("$inc" << BSON("a" << 0));
                ASSERT_EQUALS(invertUpdateMods(mods),
                              BSON("$inc" << BSON("a" << 0)));

                mods = BSON("$inc" << BSON("a" << -1));
                ASSERT_EQUALS(invertUpdateMods(mods),
                              BSON("$inc" << BSON("a" << 1)));

                mods = BSON("$inc" << BSON("a" << 12345678));
                ASSERT_EQUALS(invertUpdateMods(mods),
                              BSON("$inc" << BSON("a" << -12345678)));
            }
        };
        class DoubleInc {
        public:
            void run() {
                BSONObj mods = BSON("$inc" << BSON("a" << 1 << "b" << -1));
                ASSERT_EQUALS(invertUpdateMods(mods),
                              BSON("$inc" << BSON("a" << -1 << "b" << 1)));
            }
        };
        class RepeatedInc {
        public:
            void run() {
                BSONObj mods = BSON("$inc" << BSON("a" << 1) << "$inc" << BSON("b" << 1));
                ASSERT_EQUALS(invertUpdateMods(mods),
                              BSON("$inc" << BSON("a" << -1) << "$inc" << BSON("b" << -1)));
            }
        };
        class MixedInc {
        public:
            void run() {
                BSONObj mods = BSON("$inc" << BSON("a" << 1) << "$inc" << BSON("b" << -1));
                ASSERT_EQUALS(invertUpdateMods(mods),
                              BSON("$inc" << BSON("a" << -1) << "$inc" << BSON("b" << 1)));

                mods = BSON("$inc" << BSON("a" << -1) << "$inc" << BSON("b" << 1));
                ASSERT_EQUALS(invertUpdateMods(mods),
                              BSON("$inc" << BSON("a" << 1) << "$inc" << BSON("b" << -1)));
            }
        };
    };

    namespace basic {
        class Base : public ClientBase {
        protected:

            virtual const char * ns() = 0;
            virtual void dotest() = 0;

            void insert( const BSONObj& o ) {
                client().insert( ns() , o );
            }

            void update( const BSONObj& m ) {
                client().update( ns() , BSONObj() , m );
            }

            BSONObj findOne() {
                return client().findOne( ns() , BSONObj() );
            }

            void test( const char* initial , const char* mod , const char* after ) {
                test( fromjson( initial ) , fromjson( mod ) , fromjson( after ) );
            }


            void test( const BSONObj& initial , const BSONObj& mod , const BSONObj& after ) {
                client().dropCollection( ns() );
                insert( initial );
                update( mod );
                ASSERT_EQUALS( after , findOne() );
                client().dropCollection( ns() );
            }

        public:

            Base() {}
            virtual ~Base() {
            }

            void run() {
                client().dropCollection( ns() );

                dotest();

                client().dropCollection( ns() );
            }
        };

        class SingleTest : public Base {
            virtual BSONObj initial() = 0;
            virtual BSONObj mod() = 0;
            virtual BSONObj after() = 0;

            void dotest() {
                test( initial() , mod() , after() );
            }

        };

        class inc1 : public SingleTest {
            virtual BSONObj initial() {
                return BSON( "_id" << 1 << "x" << 1 );
            }
            virtual BSONObj mod() {
                return BSON( "$inc" << BSON( "x" << 2 ) );
            }
            virtual BSONObj after() {
                return BSON( "_id" << 1 << "x" << 3 );
            }
            virtual const char * ns() {
                return "unittests.inc1";
            }

        };

        class inc2 : public SingleTest {
            virtual BSONObj initial() {
                return BSON( "_id" << 1 << "x" << 1 );
            }
            virtual BSONObj mod() {
                return BSON( "$inc" << BSON( "x" << 2.5 ) );
            }
            virtual BSONObj after() {
                return BSON( "_id" << 1 << "x" << 3.5 );
            }
            virtual const char * ns() {
                return "unittests.inc2";
            }

        };

        class inc3 : public SingleTest {
            virtual BSONObj initial() {
                return BSON( "_id" << 1 << "x" << 537142123123LL );
            }
            virtual BSONObj mod() {
                return BSON( "$inc" << BSON( "x" << 2 ) );
            }
            virtual BSONObj after() {
                return BSON( "_id" << 1 << "x" << 537142123125LL );
            }
            virtual const char * ns() {
                return "unittests.inc3";
            }

        };

        class inc4 : public SingleTest {
            virtual BSONObj initial() {
                return BSON( "_id" << 1 << "x" << 537142123123LL );
            }
            virtual BSONObj mod() {
                return BSON( "$inc" << BSON( "x" << 2LL ) );
            }
            virtual BSONObj after() {
                return BSON( "_id" << 1 << "x" << 537142123125LL );
            }
            virtual const char * ns() {
                return "unittests.inc4";
            }

        };

        class inc5 : public SingleTest {
            virtual BSONObj initial() {
                return BSON( "_id" << 1 << "x" << 537142123123LL );
            }
            virtual BSONObj mod() {
                return BSON( "$inc" << BSON( "x" << 2.0 ) );
            }
            virtual BSONObj after() {
                return BSON( "_id" << 1 << "x" << 537142123125LL );
            }
            virtual const char * ns() {
                return "unittests.inc5";
            }

        };

        class inc6 : public Base {

            virtual const char * ns() {
                return "unittests.inc6";
            }


            virtual BSONObj initial() { return BSONObj(); }
            virtual BSONObj mod() { return BSONObj(); }
            virtual BSONObj after() { return BSONObj(); }

            void dotest() {
                long long start = numeric_limits<int>::max() - 5;
                long long max   = numeric_limits<int>::max() + 5ll;

                client().insert( ns() , BSON( "x" << (int)start ) );
                ASSERT( findOne()["x"].type() == NumberInt );

                while ( start < max ) {
                    update( BSON( "$inc" << BSON( "x" << 1 ) ) );
                    start += 1;
                    ASSERT_EQUALS( start , findOne()["x"].numberLong() ); // SERVER-2005
                }

                ASSERT( findOne()["x"].type() == NumberLong );
            }
        };

        class bit1 : public Base {
            const char * ns() {
                return "unittests.bit1";
            }
            void dotest() {
                test( BSON( "_id" << 1 << "x" << 3 ) , BSON( "$bit" << BSON( "x" << BSON( "and" << 2 ) ) ) , BSON( "_id" << 1 << "x" << ( 3 & 2 ) ) );
                test( BSON( "_id" << 1 << "x" << 1 ) , BSON( "$bit" << BSON( "x" << BSON( "or" << 4 ) ) ) , BSON( "_id" << 1 << "x" << ( 1 | 4 ) ) );
                test( BSON( "_id" << 1 << "x" << 3 ) , BSON( "$bit" << BSON( "x" << BSON( "and" << 2 << "or" << 8 ) ) ) , BSON( "_id" << 1 << "x" << ( ( 3 & 2 ) | 8 ) ) );
                test( BSON( "_id" << 1 << "x" << 3 ) , BSON( "$bit" << BSON( "x" << BSON( "or" << 2 << "and" << 8 ) ) ) , BSON( "_id" << 1 << "x" << ( ( 3 | 2 ) & 8 ) ) );

            }
        };

        class unset : public Base {
            const char * ns() {
                return "unittests.unset";
            }
            void dotest() {
                test( "{_id:1,x:1}" , "{$unset:{x:1}}" , "{_id:1}" );
            }
        };

        class setswitchint : public Base {
            const char * ns() {
                return "unittests.int1";
            }
            void dotest() {
                test( BSON( "_id" << 1 << "x" << 1 ) , BSON( "$set" << BSON( "x" << 5.6 ) ) , BSON( "_id" << 1 << "x" << 5.6 ) );
                test( BSON( "_id" << 1 << "x" << 5.6 ) , BSON( "$set" << BSON( "x" << 1 ) ) , BSON( "_id" << 1 << "x" << 1 ) );
            }
        };


    };


    class All : public Suite {
    public:
        All() : Suite( "update" ) {
        }
        void setupTests() {
            add< ModId >();
            add< ModNonmodMix >();
            add< InvalidMod >();
            add< ModNotFirst >();
            add< ModDuplicateFieldSpec >();
            add< IncNonNumber >();
            add< PushAllNonArray >();
            add< PullAllNonArray >();
            add< IncTargetNonNumber >();
            add< SetNum >();
            add< SetString >();
            add< SetStringDifferentLength >();
            add< SetStringToNum >();
            add< SetStringToNumInPlace >();
            add< SetOnInsertFromEmpty >();
            add< SetOnInsertFromNonExistent >();
            add< SetOnInsertFromNonExistentWithQuery >();
            add< SetOnInsertFromNonExistentWithQueryOverField >();
            add< SetOnInsertMissingField >();
            add< SetOnInsertExisting >();
            add< SetOnInsertMixed >();
            add< SetOnInsertMissingParent >();
            add< ModDotted >();
            add< SetInPlaceDotted >();
            add< SetRecreateDotted >();
            add< SetMissingDotted >();
            add< SetAdjacentDotted >();
            add< IncMissing >();
            add< MultiInc >();
            add< UnorderedNewSet >();
            add< UnorderedNewSetAdjacent >();
            add< ArrayEmbeddedSet >();
            add< AttemptEmbedInExistingNum >();
            add< AttemptEmbedConflictsWithOtherSet >();
            add< ModMasksEmbeddedConflict >();
            add< ModOverwritesExistingObject >();
            add< InvalidEmbeddedSet >();
            add< UpsertMissingEmbedded >();
            add< Push >();
            add< PushInvalidEltType >();
            add< PushConflictsWithOtherMod >();
            add< PushFromNothing >();
            add< PushFromEmpty >();
            add< PushInsideNothing >();
            add< CantPushInsideOtherMod >();
            add< CantPushTwice >();
            add< SetEncapsulationConflictsWithExistingType >();
            add< CantPushToParent >();
            add< PushEachSimple >();
            add< PushEachFromEmpty >();
            add< PushSliceBelowFull >();
            add< PushSliceReachedFullExact >();
            add< PushSliceReachedFullWithEach >();
            add< PushSliceReachedFullWithBoth >();
            add< PushSliceToZero >();
            add< PushSliceToZeroFromNothing >();
            add< PushSliceFromNothing >();
            add< PushSliceLongerThanSliceFromNothing >();
            add< PushSliceFromEmpty >();
            add< PushSliceLongerThanSliceFromEmpty >();
            add< PushSliceTwoFields >();
            add< PushSliceAndNormal >();
            add< PushSliceTwoFieldsConflict >();
            add< PushSliceAndNormalConflict >();
            add< PushSliceInvalidEachType >();
            add< PushSliceInvalidSliceType >();
            add< PushSliceInvalidSliceValue >();
            add< PushSliceInvalidSliceDouble >();
            add< PushSliceValidSliceDouble >();
            add< PushSliceInvalidSlice >();
            add< PushSortBelowFull >();
            add< PushSortReachedFullExact >();
            add< PushSortReachedFullWithBoth >();
            add< PushSortToZero >();
            add< PushSortToZeroFromNothing >();
            add< PushSortFromNothing >();
            add< PushSortLongerThanSliceFromNothing >();
            add< PushSortFromEmpty >();
            add< PushSortLongerThanSliceFromEmpty >();
            add< PushSortSortMixed >();
            add< PushSortSortOutOfOrderFields >();
            add< PushSortSortExtraFields >();
            add< PushSortSortMissingFields >();
            add< PushSortSortNestedFields >();
            add< PushSortInvalidSortPattern >();
            add< PushSortInvalidEachType >();
            add< PushSortInvalidBaseArray >();
            add< PushSortInvalidSortType >();
            add< PushSortInvalidSortValue >();
            add< PushSortInvalidSortDouble >();
            add< PushSortValidSortDouble >();
            add< PushSortInvalidSortSort >();
            add< PushSortInvalidSortSortOrder >();
            add< PushSortInvertedSortAndSlice >();
            add< PushSortInvalidDuplicatedSort >();
            add< PushSortInvalidMissingSliceTo >();
            add< CantIncParent >();
            add< DontDropEmpty >();
            add< InsertInEmpty >();
            add< IndexParentOfMod >();
            add< IndexModSet >();
            add< PreserveIdWithIndex >();
            add< CheckNoMods >();
            add< UpdateMissingToNull >();
            add< TwoModsWithinDuplicatedField >();
            add< ThreeModsWithinDuplicatedField >();
            add< TwoModsBeforeExistingField >();

            add< ModSetTests::internal1 >();
            add< ModSetTests::inc1 >();
            add< ModSetTests::inc2 >();
            add< ModSetTests::set1 >();
            add< ModSetTests::push1 >();

            add< ModSetTests::PositionalWithoutElemMatchKey >();
            add< ModSetTests::PositionalWithoutNestedElemMatchKey >();
            add< ModSetTests::DbrefPassesPositionalValidation >();
            add< ModSetTests::CreateNewFromQueryExcludeNot >();
            add< Invertible::SimpleInc>();
            add< Invertible::DoubleInc>();
            add< Invertible::RepeatedInc>();
            add< Invertible::MixedInc>();

            add< basic::inc1 >();
            add< basic::inc2 >();
            add< basic::inc3 >();
            add< basic::inc4 >();
            add< basic::inc5 >();
            add< basic::inc6 >();
            add< basic::bit1 >();
            add< basic::unset >();
            add< basic::setswitchint >();
        }
    } myall;

} // namespace UpdateTests

