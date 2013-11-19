//@file update.cpp

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

#include "mongo/db/oplog.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/ops/query.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/update_internal.h"
#include "mongo/db/oplog_helpers.h"

namespace mongo {

    void updateOneObject(NamespaceDetails *d, const BSONObj &pk, 
                         const BSONObj &oldObj, const BSONObj &newObj, 
                         const bool logop, const bool fromMigrate,
                         uint64_t flags) {
        d->updateObject(pk, oldObj, newObj, logop, fromMigrate, flags);
        d->notifyOfWriteOp();
    }

    static void checkNoMods( const BSONObj &o ) {
        BSONObjIterator i( o );
        while( i.moreWithEOO() ) {
            BSONElement e = i.next();
            if ( e.eoo() )
                break;
            uassert( 10154 ,  "Modifiers and non-modifiers cannot be mixed", e.fieldName()[ 0 ] != '$' );
        }
    }

    static void checkTooLarge(const BSONObj& newObj) {
        uassert( 12522 , "$ operator made object too large" , newObj.objsize() <= BSONObjMaxUserSize );
    }

    // Hack to apply an update message supplied by a NamespaceDetails to
    // some row in an in IndexDetails (for fast ydb updates).
    // TODO: Make this interaction cleaner.
    class ApplyUpdateMessage : storage::UpdateCallback {
        BSONObj apply( const BSONObj &oldObj, const BSONObj &msg ) {
            // The update message is simply an update object, supplied by the user.
            const BSONObj &updateObj = msg;
            ModSet mods( updateObj );
            auto_ptr<ModSetState> mss = mods.prepare( oldObj );
            BSONObj newObj = mss->createNewFromMods();
            checkTooLarge( newObj );
            return newObj;
        }
    } _storageUpdateCallback; // installed as the ydb update callback in db.cpp via set_update_callback

    static void updateUsingMods(NamespaceDetails *d, const BSONObj &pk, const BSONObj &obj,
                                ModSetState &mss, const bool modsAreIndexed,
                                const bool logop, const bool fromMigrate) {
        BSONObj newObj = mss.createNewFromMods();
        checkTooLarge( newObj );
        TOKULOG(3) << "updateUsingMods used mod set, transformed " << obj << " to " << newObj << endl;

        updateOneObject( d, pk, obj, newObj, logop, fromMigrate,
                         modsAreIndexed ? 0 : NamespaceDetails::KEYS_UNAFFECTED_HINT );
    }

    static void updateNoMods(NamespaceDetails *d, const BSONObj &pk, const BSONObj &obj, const BSONObj &updateobj,
                             const bool logop, const bool fromMigrate) {
        BSONElementManipulator::lookForTimestamps( updateobj );
        checkNoMods( updateobj );
        TOKULOG(3) << "updateNoMods replacing pk " << pk << ", obj " << obj << " with updateobj " << updateobj << endl;

        updateOneObject( d, pk, obj, updateobj, logop, fromMigrate );
    }

    static void checkBulkLoad(const StringData &ns) {
        uassert(16893, str::stream() <<
                       "Cannot update a collection under-going bulk load: " << ns,
                       ns != cc().bulkLoadNS());
    }

    static void insertAndLog(const char *ns, NamespaceDetails *d, BSONObj &newObj,
                             bool logop, bool fromMigrate) {

        checkNoMods( newObj );
        TOKULOG(3) << "insertAndLog for upsert: " << newObj << endl;

        // We cannot pass NamespaceDetails::NO_UNIQUE_CHECKS because we still need to check secondary indexes.
        // We know if we are in this function that we did a query for the object and it didn't exist yet, so the unique check on the PK won't fail.
        // To prove this to yourself, look at the callers of insertAndLog and see that they return an UpdateResult that says the object didn't exist yet.
        checkBulkLoad(ns);
        insertOneObject(d, newObj);
        if (logop) {
            OpLogHelpers::logInsert(ns, newObj);
        }
    }

    static bool hasClusteringSecondaryKey(NamespaceDetails *d) {
        for (int i = 0; i < d->nIndexesBeingBuilt(); i++) {
            IndexDetails &idx = d->idx(i);
            if (!d->isPKIndex(idx) && idx.clustering()) {
                // has a clustering secondary key
                return true;
            }
        }
        // no clustering secondary keys
        return false;
    }

    /* note: this is only (as-is) called for

             - not multi
             - not mods is indexed
             - not upsert
    */
    static UpdateResult _updateById(const BSONObj &idQuery,
                                    bool isOperatorUpdate,
                                    ModSet* mods,
                                    NamespaceDetails* d,
                                    const BSONObj& updateobj,
                                    bool logop,
                                    bool fromMigrate = false) {

        if ( cmdLine.fastupdates && !mods->isIndexed() &&
             !hasClusteringSecondaryKey(d) ) {
            // Fast update path that skips the _id query.
            // We know no indexes need to be updated so we
            // don't need to read the full object.
            d->updateObjectMods(pk, updateobj);
            d->notifyOfWriteOp();
            debug.fastmod = true;
            // TODO: Log this update for replication.
            return UpdateResult( 0, 0, 0, BSONObj() );
        }

        BSONObj obj;
        ResultDetails queryResult;
        if ( mods && mods->hasDynamicArray() ) {
            queryResult.matchDetails.requestElemMatchKey();
        }

        const bool found = queryByIdHack(d, idQuery,
                                         patternOrig, obj,
                                         &queryResult);
        if ( !found ) {
            // no upsert support in _updateById yet, so we are done.
            return UpdateResult( 0 , 0 , 1 , BSONObj() );
        }

        const BSONObj &pk = idQuery.firstElement().wrap("");

        /* look for $inc etc.  note as listed here, all fields to inc must be this type, you can't set some
           regular ones at the moment. */
        if ( isOperatorUpdate ) {
            ModSet* useMods = mods;
            auto_ptr<ModSet> mymodset;
            if ( queryResult.matchDetails.hasElemMatchKey() && mods->hasDynamicArray() ) {
                useMods = mods->fixDynamicArray( queryResult.matchDetails.elemMatchKey() );
                mymodset.reset( useMods );
            }

            // mod set update, ie: $inc: 10 increments by 10.
            auto_ptr<ModSetState> mss = useMods->prepare( obj, false /* not an insertion */ );
            updateUsingMods( d, pk, obj, *mss, useMods->isIndexed() > 0, logop, fromMigrate );
            return UpdateResult( 1 , 1 , 1 , BSONObj() );

        } // end $operator update

        // replace-style update
        updateNoMods( d, pk, obj, updateobj, logop, fromMigrate );
        return UpdateResult( 1 , 0 , 1 , BSONObj() );
    }

    UpdateResult _updateObjects( const char* ns,
                                 const BSONObj& updateobj,
                                 const BSONObj& patternOrig,
                                 bool upsert,
                                 bool multi,
                                 bool logop ,
                                 OpDebug& debug,
                                 bool fromMigrate,
                                 const QueryPlanSelectionPolicy& planPolicy ) {

        TOKULOG(2) << "update: " << ns
                   << " update: " << updateobj
                   << " query: " << patternOrig
                   << " upsert: " << upsert << " multi: " << multi << endl;

        debug.updateobj = updateobj;

        NamespaceDetails *d = getAndMaybeCreateNS(ns, logop);

        auto_ptr<ModSet> mods;
        const bool isOperatorUpdate = updateobj.firstElementFieldName()[0] == '$';

        if ( isOperatorUpdate ) {
            mods.reset( new ModSet(updateobj, d->indexKeys()) );
        }

        if (d->mayFindById()) {
            const BSONObj idQuery = getSimpleIdQuery(patternOrig);
            if (!idQuery.isEmpty()) {
                UpdateResult result = _updateById( idQuery,
                                                   isOperatorUpdate,
                                                   mods.get(),
                                                   d,
                                                   updateobj,
                                                   patternOrig,
                                                   logop,
                                                   fromMigrate);
                if ( result.existing || ! upsert ) {
                    return result;
                }
            }
        }

        int numModded = 0;
        debug.nscanned = 0;
        shared_ptr<Cursor> c = getOptimizedCursor( ns, patternOrig, BSONObj(), planPolicy );

        if( c->ok() ) {
            set<BSONObj> seenObjects;
            MatchDetails details;
            do {

                debug.nscanned++;

                if ( mods.get() && mods->hasDynamicArray() ) {
                    details.requestElemMatchKey();
                }

                if ( !c->currentMatches( &details ) ) {
                    c->advance();
                    continue;
                }

                BSONObj currPK = c->currPK();
                if ( c->getsetdup( currPK ) ) {
                    c->advance();
                    continue;
                }

                BSONObj currentObj = c->current();

                /* look for $inc etc.  note as listed here, all fields to inc must be this type, you can't set some
                   regular ones at the moment. */
                if ( isOperatorUpdate ) {

                    if ( multi ) {
                        // Make our own copies of the currPK and currentObj before we invalidate
                        // them by advancing the cursor.
                        currPK = currPK.getOwned();
                        currentObj = currentObj.getOwned();

                        // Advance past the document to be modified - SERVER-5198,
                        while ( c->ok() && currPK == c->currPK() ) {
                            c->advance();
                        }

                        // Multi updates need to do their own deduplication because updates may modify the
                        // keys the cursor is in the process of scanning over.
                        if ( seenObjects.count( currPK ) ) {
                            continue;
                        } else {
                            seenObjects.insert( currPK );
                        }
                    }

                    ModSet* useMods = mods.get();

                    auto_ptr<ModSet> mymodset;
                    if ( details.hasElemMatchKey() && mods->hasDynamicArray() ) {
                        useMods = mods->fixDynamicArray( details.elemMatchKey() );
                        mymodset.reset( useMods );
                    }

                    auto_ptr<ModSetState> mss = useMods->prepare( currentObj,
                                                                  false /* not an insertion */ );
                    updateUsingMods( d, currPK, currentObj, *mss, useMods->isIndexed() > 0, logop, fromMigrate );

                    numModded++;
                    if ( ! multi )
                        return UpdateResult( 1 , 1 , numModded , BSONObj() );

                    continue;
                } // end if operator is update

                uassert( 10158 ,  "multi update only works with $ operators" , ! multi );

                updateNoMods( d, currPK, currentObj, updateobj, logop, fromMigrate );

                return UpdateResult( 1 , 0 , 1 , BSONObj() );
            } while ( c->ok() );
        } // endif

        if ( numModded )
            return UpdateResult( 1 , 1 , numModded , BSONObj() );

        if ( upsert ) {
            BSONObj newObj = updateobj;
            if ( updateobj.firstElementFieldName()[0] == '$' ) {
                // upsert of an $operation. build a default object
                BSONObj newObj = mods->createNewFromQuery( patternOrig );
                debug.fastmodinsert = true;
                insertAndLog( ns, d, newObj, logop, fromMigrate );
                return UpdateResult( 0 , 1 , 1 , newObj );
            }
            uassert( 10159 ,  "multi update only works with $ operators" , ! multi );
            debug.upsert = true;
            insertAndLog( ns, d, newObj, logop, fromMigrate );
            return UpdateResult( 0 , 0 , 1 , newObj );
        }

        return UpdateResult( 0 , isOperatorUpdate , 0 , BSONObj() );
    }

    void validateUpdate( const char* ns , const BSONObj& updateobj, const BSONObj& patternOrig ) {
        uassert( 10155 , "cannot update reserved $ collection", NamespaceString::normal(ns) );
        if ( NamespaceString::isSystem(ns) ) {
            /* dm: it's very important that system.indexes is never updated as IndexDetails
               has pointers into it */
            uassert( 10156,
                     str::stream() << "cannot update system collection: "
                                   << ns << " q: " << patternOrig << " u: " << updateobj,
                     legalClientSystemNS( ns , true ) );
        }
    }

    UpdateResult updateObjects( const char* ns,
                                const BSONObj& updateobj,
                                const BSONObj& patternOrig,
                                bool upsert,
                                bool multi,
                                bool logop ,
                                OpDebug& debug,
                                bool fromMigrate,
                                const QueryPlanSelectionPolicy& planPolicy ) {

        validateUpdate( ns , updateobj , patternOrig );

        UpdateResult ur = _updateObjects(ns, updateobj, patternOrig,
                                         upsert, multi, logop,
                                         debug, fromMigrate, planPolicy );
        debug.nupdated = ur.num;
        return ur;
    }

}  // namespace mongo
