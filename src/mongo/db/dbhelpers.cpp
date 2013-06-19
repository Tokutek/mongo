// dbhelpers.cpp

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

#include <fstream>

#include "mongo/pch.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/json.h"
#include "mongo/db/cursor.h"
#include "mongo/db/oplog.h"
#include "mongo/db/queryoptimizercursor.h"
#include "mongo/db/repl_block.h"
#include "mongo/db/database.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/ops/count.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/oplog_helpers.h"

#include <boost/filesystem/convenience.hpp>
#include <boost/filesystem/operations.hpp>

namespace mongo {

    void Helpers::putSingleton(const char *ns, BSONObj obj) {
        OpDebug debug;
        Client::Context context(ns);
        updateObjects(ns, obj, /*pattern=*/BSONObj(), /*upsert=*/true, /*multi=*/false , /*logtheop=*/true , debug );
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
            // for non 1/-1 field values, like {a : "hashed"}, treat order as ascending
            int order = patElt.isNumber() ? patElt.numberInt() : 1;
            if( minOrMax * order == 1 ){
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
                                    bool fromMigrate ) {
        long long numDeleted = 0;

        Client::ReadContext ctx(ns);
        Client::Transaction txn(DB_SERIALIZABLE);

        NamespaceDetails *d = nsdetails( ns.c_str() );
        NamespaceDetailsTransient *nsdt = &NamespaceDetailsTransient::get( ns.c_str() );
        IndexDetails &i = d->idx(d->findIndexByKeyPattern(keyPattern));
        // Extend min to get (min, MinKey, MinKey, ....)
        BSONObj newMin = Helpers::modifiedRangeBound( min , keyPattern , -1 );
        // If upper bound is included, extend max to get (max, MaxKey, MaxKey, ...)
        // If not included, extend max to get (max, MinKey, MinKey, ....)
        int minOrMax = maxInclusive ? 1 : -1;
        BSONObj newMax = Helpers::modifiedRangeBound( max , keyPattern , minOrMax );

        for (IndexCursor c(d, i, newMin, newMax, maxInclusive, 1); c.ok(); c.advance()) {
            BSONObj pk = c.currPK();
            BSONObj obj = c.current();
            OpLogHelpers::logDelete(ns.c_str(), obj, fromMigrate, &cc().txn());
            deleteOneObject(d, nsdt, pk, obj);
            numDeleted++;
        }

        txn.commit();
        return numDeleted;
    }

} // namespace mongo
