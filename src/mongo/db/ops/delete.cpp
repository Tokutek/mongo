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
#include "mongo/db/oplog_helpers.h"

namespace mongo {

    void deleteOneObject(NamespaceDetails *d, NamespaceDetailsTransient *nsdt, const BSONObj &pk, const BSONObj &obj) {
        d->deleteObject(pk, obj);
        if (nsdt != NULL) {
            nsdt->notifyOfWriteOp();
        }
    }
    
    long long _deleteObjects(const char *ns, BSONObj pattern, bool justOne, bool logop) {
        NamespaceDetails *d = nsdetails( ns );
        NamespaceDetailsTransient *nsdt = &NamespaceDetailsTransient::get(ns);
        if ( ! d )
            return 0;

        uassert( 10101 ,  "can't remove from a capped collection" , ! d->isCapped() );

        shared_ptr< Cursor > c = NamespaceDetailsTransient::getCursor( ns, pattern );
        if( !c->ok() )
            return 0;

        shared_ptr< Cursor > cPtr = c;
        auto_ptr<ClientCursor> cc( new ClientCursor( QueryOption_NoCursorTimeout, cPtr, ns) );

        long long nDeleted = 0;
        while ( cc->ok() ) {

            if ( cc->currentIsDup() || !c->currentMatches() ) {
                tokulog(4) << "_deleteObjects skipping " << cc->currPK() << ", dup or doesn't match" << endl;
                cc->advance();
                continue;
            }

            BSONObj pk = cc->currPK().copy();
            BSONObj obj = cc->current().copy();

            while ( cc->ok() && cc->currPK() == pk ) {
                cc->advance();
            }

            tokulog(4) << "_deleteObjects iteration: pk " << pk << ", obj " << obj << endl;

            if ( logop ) {
                OpLogHelpers::logDelete(ns, obj, false, cc().txn());
            }

            deleteOneObject(d, nsdt, pk, obj);
            nDeleted++;

            if ( justOne ) {
                break;
            }
        }
        return nDeleted;
    }

    /* ns:      namespace, e.g. <database>.<collection>
       pattern: the "where" clause / criteria
       justOne: stop after 1 match
    */
    long long deleteObjects(const char *ns, BSONObj pattern, bool justOne, bool logop) {
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

        long long nDeleted = _deleteObjects(ns, pattern, justOne, logop);

        return nDeleted;
    }
}
