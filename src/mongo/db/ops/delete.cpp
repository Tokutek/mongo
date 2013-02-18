// delete.cpp

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
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/oplog.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/ops/delete.h"
#include "mongo/util/stacktrace.h"

namespace mongo {

    void deleteOneObject(NamespaceDetails *d, const BSONObj &pk, const BSONObj &obj) {
        uasserted(16439, "I don't know how to delete objects yet, sorry :(");
    }
    
    /* ns:      namespace, e.g. <database>.<collection>
       pattern: the "where" clause / criteria
       justOne: stop after 1 match
       god:     allow access to system namespaces, and don't yield
    */
    long long deleteObjects(const char *ns, BSONObj pattern, bool justOne, bool logop, bool god) {
        Client::Transaction txn(DB_TXN_SNAPSHOT);

        if( !god ) {
            if ( strstr(ns, ".system.") ) {
                /* note a delete from system.indexes would corrupt the db
                if done here, as there are pointers into those objects in
                NamespaceDetails.
                */
                uassert(12050, "cannot delete from system namespace", legalClientSystemNS( ns , true ) );
            }
            if ( strchr( ns , '$' ) ) {
                log() << "cannot delete from collection with reserved $ in name: " << ns << endl;
                uassert( 10100 ,  "cannot delete from collection with reserved $ in name", strchr(ns, '$') == 0 );
            }
        }

        NamespaceDetails *d = nsdetails( ns );
        if ( ! d )
            return 0;
        uassert( 10101 ,  "can't remove from a capped collection" , ! d->isCapped() );

        shared_ptr< Cursor > creal = NamespaceDetailsTransient::getCursor( ns, pattern );
        if( !creal->ok() )
            return 0;

        shared_ptr< Cursor > cPtr = creal;
        auto_ptr<ClientCursor> cc( new ClientCursor( QueryOption_NoCursorTimeout, cPtr, ns) );

        long long nDeleted = 0;
        while ( cc->ok() ) {
            bool match = creal->currentMatches();

            cc->advance();
            
            if ( ! match )
                continue;

            BSONObj pk = cc->currPK();
            BSONObj key = cc->currKey();
            BSONObj obj = cc->current();

            // SERVER-5198 Advance past the document to be modified, but see SERVER-5725.
            while( cc->ok() && pk == cc->currPK() ) {
                cc->advance();
            }
            
            bool foundAllResults = ( justOne || !cc->ok() );

            if ( logop ) {
                BSONElement e;
                if( obj.getObjectID( e ) ) {
                    BSONObjBuilder b;
                    b.append( e );
                    bool replJustOne = true;
                    logOp( "d", ns, b.done(), 0, &replJustOne );
                }
                else {
                    problem() << "deleted object without id, not logging" << endl;
                }
            }

            deleteOneObject(d, pk, obj);
            nDeleted++;

            if ( foundAllResults ) {
                break;
            }
         
            if( debug && god && nDeleted == 100 ) 
                log() << "warning high number of deletes with god=true which could use significant memory" << endl;
        }

        txn.commit();
        return nDeleted;
    }
}
