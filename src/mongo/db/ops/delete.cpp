// delete.cpp

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
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#include "mongo/pch.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/oplog.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/db/ops/delete.h"
#include "mongo/util/stacktrace.h"
#include "mongo/db/oplog_helpers.h"

namespace mongo {

    void deleteOneObject(NamespaceDetails *d, const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        d->deleteObject(pk, obj, flags);
        d->notifyOfWriteOp();
    }
    
    long long _deleteObjects(const char *ns, BSONObj pattern, bool justOne, bool logop) {
        NamespaceDetails *d = nsdetails( ns );
        if ( ! d )
            return 0;

        uassert( 10101 ,  "can't remove from a capped collection" , ! d->isCapped() );

        shared_ptr< Cursor > c = getOptimizedCursor( ns, pattern );
        if ( !c->ok() ) {
            return 0;
        }

        shared_ptr< Cursor > cPtr = c;
        auto_ptr<ClientCursor> ccc( new ClientCursor( QueryOption_NoCursorTimeout, cPtr, ns) );

        long long nDeleted = 0;
        while ( ccc->ok() ) {

            if ( ccc->currentIsDup() || !c->currentMatches() ) {
                TOKULOG(4) << "_deleteObjects skipping " << ccc->currPK() << ", dup or doesn't match" << endl;
                ccc->advance();
                continue;
            }

            BSONObj pk = ccc->currPK().copy();
            BSONObj obj = ccc->current().copy();

            // Because the query optimizer cursor implementation records cursor
            // matches on advance(), we must advance here before deleting, otherwise
            // the cursor would appear to always point to something that doesn't
            // match the query, ruining it's chances of getting chosen later for
            // a similar query.
            while ( ccc->ok() && ccc->currPK() == pk ) {
                ccc->advance();
            }

            TOKULOG(4) << "_deleteObjects iteration: pk " << pk << ", obj " << obj << endl;

            if ( logop ) {
                OpLogHelpers::logDelete(ns, obj, false, &cc().txn());
            }

            deleteOneObject(d, pk, obj);
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
        if ( NamespaceString::isSystem(ns) ) {
            /* note a delete from system.indexes would corrupt the db
               if done here, as there are pointers into those objects in
               NamespaceDetails.
            */
            uassert(12050, "cannot delete from system namespace", legalClientSystemNS( ns , true ) );
        }
        if ( !NamespaceString::normal(ns) ) {
            log() << "cannot delete from collection with reserved $ in name: " << ns << endl;
            uasserted( 10100 ,  "cannot delete from collection with reserved $ in name" );
        }

        long long nDeleted = _deleteObjects(ns, pattern, justOne, logop);

        return nDeleted;
    }
}
