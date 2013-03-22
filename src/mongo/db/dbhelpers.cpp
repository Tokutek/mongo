// dbhelpers.cpp

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

#include <fstream>

#include "mongo/pch.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/db.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/json.h"
#include "mongo/db/cursor.h"
#include "mongo/db/oplog.h"
#include "mongo/db/queryoptimizercursor.h"
#include "mongo/db/repl_block.h"
#include "mongo/db/idgen.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/oplog_helpers.h"

#include <boost/filesystem/convenience.hpp>
#include <boost/filesystem/operations.hpp>

namespace mongo {

    const BSONObj reverseNaturalObj = BSON( "$natural" << -1 );

    void Helpers::ensureIndex(const char *ns, BSONObj keyPattern, bool unique, const char *name) {
        NamespaceDetails *d = nsdetails(ns);
        if( d == 0 )
            return;

        {
            NamespaceDetails::IndexIterator i = d->ii();
            while( i.more() ) {
                if( i.next().keyPattern().woCompare(keyPattern) == 0 )
                    return;
            }
        }

        if( d->nIndexes() >= NamespaceDetails::NIndexesMax ) {
            problem() << "Helper::ensureIndex fails, MaxIndexes exceeded " << ns << '\n';
            return;
        }

        string system_indexes = cc().database()->name + ".system.indexes";

        BSONObjBuilder b;
        b.append("name", name);
        b.append("ns", ns);
        b.append("key", keyPattern);
        b.appendBool("unique", unique);
        BSONObj o = b.done();

        insertObject(system_indexes.c_str(), o);
    }

    /* fetch a single object from collection ns that matches query
       set your db SavedContext first
    */
    bool Helpers::findOne(const char *ns, const BSONObj &query, BSONObj& result, bool requireIndex) {
        BSONObj obj = findOne( ns, query, requireIndex );
        if ( obj.isEmpty() )
            return false;
        result = obj;
        return true;
    }

    /* fetch a single object from collection ns that matches query
       set your db SavedContext first
    */
    BSONObj Helpers::findOne(const char *ns, const BSONObj &query, bool requireIndex) {
        shared_ptr<Cursor> c =
            NamespaceDetailsTransient::getCursor( ns , query, BSONObj(),
                                                  requireIndex ?
                                                  QueryPlanSelectionPolicy::indexOnly() :
                                                  QueryPlanSelectionPolicy::any() );
        while( c->ok() ) {
            if ( c->currentMatches() && !c->getsetdup( c->currPK() ) ) {
                return c->current();
            }
            c->advance();
        }
        return BSONObj();
    }


    bool Helpers::findById( const char *ns, BSONObj query, BSONObj& result ) {
        Lock::assertAtLeastReadLocked(ns);
        NamespaceDetails *d = nsindex(ns)->details(ns);
        if ( ! d ) {
            return false;
        } else {
            return d->findById(query, result);
        }
    }

    shared_ptr<Cursor> Helpers::findTableScan(const char *ns, const BSONObj &order) {
        BSONElement el = order.getField("$natural"); // e.g., { $natural : -1 }

        NamespaceDetails *d = nsdetails(ns);
        if ( el.number() >= 0 ) {
            return shared_ptr<Cursor>( new BasicCursor(d) );
        } else {
            // "reverse natural order"
            if ( !d->isCapped() ) {
                return shared_ptr<Cursor>( new ReverseCursor(d) );
            }
            else {
                // TODO: Capped collections
                //return shared_ptr<Cursor>( new ReverseCappedCursor(d) );
                ::abort();
                return shared_ptr<Cursor>( new ReverseCursor(d) );
            }
        }
    }

    vector<BSONObj> Helpers::findAll( const string& ns , const BSONObj& query ) {
        Lock::assertAtLeastReadLocked( ns );

        vector<BSONObj> all;

        Client::Context tx( ns );
        
        shared_ptr<Cursor> c = NamespaceDetailsTransient::getCursor( ns.c_str(), query );

        while( c->ok() ) {
            if ( c->currentMatches() && !c->getsetdup( c->currPK() ) ) {
                all.push_back( c->current() );
            }
            c->advance();
        }

        return all;
    }

    bool Helpers::isEmpty(const char *ns, bool doAuth) {
        Client::Context context(ns, dbpath, doAuth);
        shared_ptr<Cursor> c = findTableScan(ns, BSONObj());
        return !c->ok();
    }

    /* Get the first object from a collection.  Generally only useful if the collection
       only ever has a single object -- which is a "singleton collection.

       Returns: true if object exists.
    */
    bool Helpers::getSingleton(const char *ns, BSONObj& result) {
        Client::Context context(ns);

        shared_ptr<Cursor> c = findTableScan(ns, BSONObj());
        if ( !c->ok() ) {
            context.getClient()->curop()->done();
            return false;
        }

        result = c->current();
        context.getClient()->curop()->done();
        return true;
    }

    bool Helpers::getFirst(const char *ns, BSONObj& result) {
        return getSingleton(ns, result);
    }

    bool Helpers::getLast(const char *ns, BSONObj& result) {
        Client::Context ctx(ns);
        shared_ptr<Cursor> c = findTableScan(ns, reverseNaturalObj);
        if( !c->ok() )
            return false;
        result = c->current();
        return true;
    }

    void Helpers::upsert( const string& ns , const BSONObj& o, bool fromMigrate ) {
        BSONElement e = o["_id"];
        verify( e.type() );
        BSONObj id = e.wrap();

        OpDebug debug;
        Client::Context context(ns);
        updateObjects(ns.c_str(), o, /*pattern=*/id, /*upsert=*/true, /*multi=*/false , /*logtheop=*/true , debug, fromMigrate );
    }

    void Helpers::putSingleton(const char *ns, BSONObj obj) {
        OpDebug debug;
        Client::Context context(ns);
        updateObjects(ns, obj, /*pattern=*/BSONObj(), /*upsert=*/true, /*multi=*/false , /*logtheop=*/true , debug );
        context.getClient()->curop()->done();
    }

    void Helpers::putSingletonGod(const char *ns, BSONObj obj, bool logTheOp) {
        OpDebug debug;
        Client::Context context(ns);
        _updateObjects(/*god=*/true, ns, obj, /*pattern=*/BSONObj(), /*upsert=*/true, /*multi=*/false , logTheOp , debug );
        context.getClient()->curop()->done();
    }

    BSONObj Helpers::toKeyFormat( const BSONObj& o , BSONObj& key ) {
        BSONObjBuilder me;
        BSONObjBuilder k;

        BSONObjIterator i( o );
        while ( i.more() ) {
            BSONElement e = i.next();
            k.append( e.fieldName() , 1 );
            me.appendAs( e , "" );
        }
        key = k.obj();
        return me.obj();
    }

    BSONObj Helpers::modifiedRangeBound( const BSONObj& bound ,
                                         const BSONObj& keyPattern ,
                                         int minOrMax ){
        BSONObjBuilder newBound;

        BSONObjIterator src( bound );
        BSONObjIterator pat( keyPattern );

        while( src.more() ){
            massert( 16341 ,
                     str::stream() << "keyPattern " << keyPattern
                                   << " shorter than bound " << bound ,
                     pat.more() );
            BSONElement srcElt = src.next();
            BSONElement patElt = pat.next();
            massert( 16333 ,
                     str::stream() << "field names of bound " << bound
                                   << " do not match those of keyPattern " << keyPattern ,
                     str::equals( srcElt.fieldName() , patElt.fieldName() ) );
            newBound.appendAs( srcElt , "" );
        }
        while( pat.more() ){
            BSONElement patElt = pat.next();
            verify( patElt.isNumber() );
            if( minOrMax * patElt.numberInt() == 1){
                newBound.appendMaxKey("");
            }
            else {
                newBound.appendMinKey("");
            }
        }
        return newBound.obj();
    }

    long long Helpers::removeRange( const string& ns ,
                                    const BSONObj& min ,
                                    const BSONObj& max ,
                                    const BSONObj& keyPattern ,
                                    bool maxInclusive ,
                                    bool secondaryThrottle ,
                                    bool fromMigrate ) {
        
        Client& c = cc();

        long long numDeleted = 0;
        long long millisWaitingForReplication = 0;

        while ( 1 ) {
            {
                Client::WriteContext ctx(ns);
                scoped_ptr<Cursor> c;
                NamespaceDetails *d = nsdetails( ns.c_str() );
                NamespaceDetailsTransient *nsdt = &NamespaceDetailsTransient::get( ns.c_str() );
                if ( ! d )
                    break;
                    
                {
                    int ii = d->findIndexByKeyPattern( keyPattern );
                    verify( ii >= 0 );
                    
                    IndexDetails& i = d->idx( ii );

                    // Extend min to get (min, MinKey, MinKey, ....)
                    BSONObj newMin = Helpers::modifiedRangeBound( min , keyPattern , -1 );
                    // If upper bound is included, extend max to get (max, MaxKey, MaxKey, ...)
                    // If not included, extend max to get (max, MinKey, MinKey, ....)
                    int minOrMax = maxInclusive ? 1 : -1;
                    BSONObj newMax = Helpers::modifiedRangeBound( max , keyPattern , minOrMax );
                    
                    c.reset( new IndexCursor( d , &i , newMin , newMax , maxInclusive , 1 ) );
                }
                
                if ( ! c->ok() ) {
                    // we're done
                    break;
                }
                
                BSONObj pk = c->currPK();
                BSONObj obj = c->current();
                
                // this is so that we don't have to handle this cursor in the delete code
                c.reset(0);
                
                OpLogHelpers::logDelete(ns.c_str(), obj, fromMigrate, &cc().txn());
                deleteOneObject( d, nsdt, pk, obj);
                numDeleted++;
            }

            Timer secondaryThrottleTime;

            if ( secondaryThrottle ) {
                if ( ! waitForReplication( c.getLastOp(), 2, 60 /* seconds to wait */ ) ) {
                    warning() << "replication to secondaries for removeRange at least 60 seconds behind" << endl;
                }
                millisWaitingForReplication += secondaryThrottleTime.millis();
            }
            
            if ( ! Lock::isLocked() ) {
                int micros = ( 2 * Client::recommendedYieldMicros() ) - secondaryThrottleTime.micros();
                if ( micros > 0 ) {
                    LOG(1) << "Helpers::removeRangeUnlocked going to sleep for " << micros << " micros" << endl;
                    sleepmicros( micros );
                }
            }
                
        }
        
        if ( secondaryThrottle )
            log() << "Helpers::removeRangeUnlocked time spent waiting for replication: "  
                  << millisWaitingForReplication << "ms" << endl;
        
        return numDeleted;
    }

    void Helpers::emptyCollection(const char *ns) {
        Client::Context context(ns);
        deleteObjects(ns, BSONObj(), false);
    }

} // namespace mongo
