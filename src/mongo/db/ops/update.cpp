//@file update.cpp

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

#include "pch.h"

#include "mongo/client/dbclientinterface.h"
#include "mongo/db/oplog.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/idgen.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/update_internal.h"

namespace mongo {

    void updateOneObject(NamespaceDetails *d, NamespaceDetailsTransient *nsdt, const BSONObj &pk, const BSONObj &oldObj, const BSONObj &newObj) {
        // if newObj has no _id field, it should inherit the existing value
        BSONObjBuilder b;
        BSONObj newObjWithId = newObj;
        if ( newObj["_id"].eoo() ) {
            b.append( oldObj["_id"] );
            b.appendElements( newObj );
            newObjWithId = b.done();
        }

        tokulog(3) << "updateOneObject: del pk " << pk << ", obj " << oldObj << " and inserting " << newObjWithId << endl;
        deleteOneObject( d, nsdt, pk, oldObj );
        insertOneObject( d, nsdt, newObjWithId, false );
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

    static void updateUsingMods(NamespaceDetails *d, NamespaceDetailsTransient *nsdt,
            const BSONObj &pk, const BSONObj &obj, ModSetState &mss) {

        BSONObj newObj = mss.createNewFromMods();
        checkTooLarge( newObj );
        tokulog(3) << "updateUsingMods used mod set, transformed " << obj << " to " << newObj << endl;

        updateOneObject( d, nsdt, pk, obj, newObj );
    }

    static void updateNoMods(NamespaceDetails *d, NamespaceDetailsTransient *nsdt,
            const BSONObj &pk, const BSONObj &obj, const BSONObj &updateobj) {

        BSONElementManipulator::lookForTimestamps( updateobj );
        checkNoMods( updateobj );
        tokulog(3) << "updateNoMods replacing pk " << pk << ", obj " << obj << " with updateobj " << updateobj << endl;

        updateOneObject( d, nsdt, pk, obj, updateobj );
    }

    static void insertAndLog(const char *ns, NamespaceDetails *d, NamespaceDetailsTransient *nsdt,
            BSONObj &newObj, bool overwrite, bool logop, bool fromMigrate) {

        checkNoMods( newObj );
        tokulog(3) << "insertAndLog for upsert: " << newObj << endl;

        // Make sure to add an _id field if it doesn't exist.
        newObj = addIdField(newObj);
        insertOneObject(d, nsdt, newObj, overwrite);
        if (logop) {
            logOp("i", ns, newObj, 0, 0, fromMigrate);
        }
    }

    static bool mayUpdateById(NamespaceDetails *d, const BSONObj &patternOrig) {
        if ( isSimpleIdQuery(patternOrig) ) {
            for (int i = 0; i < d->nIndexesBeingBuilt(); i++) {
                IndexDetails &idx = d->idx(i);
                if (idx.info()["clustering"].trueValue()) {
                    return false;
                }
            }
            // We may update by _id, since:
            // - The query is a simple _id query
            // - The modifications do not affect any indexed fields
            // - There are no clustering secondary keys.
            return true;
        }
        return false;
    }

    /* note: this is only (as-is) called for

             - not multi
             - not mods is indexed
             - not upsert
    */
    static UpdateResult _updateById(const BSONObj &pk,
                                    bool isOperatorUpdate,
                                    ModSet* mods,
                                    NamespaceDetails* d,
                                    NamespaceDetailsTransient *nsdt,
                                    const char* ns,
                                    const BSONObj& updateobj,
                                    BSONObj patternOrig,
                                    bool logop,
                                    OpDebug& debug,
                                    bool fromMigrate = false) {

        BSONObj obj;
        {
            tokulog(3) << "_updateById looking for pk " << pk << endl;
            bool found = d->findById( pk, obj, false );
            tokulog(3) << "_updateById findById() got " << obj << endl;
            if ( !found ) {
                // no upsert support in _updateById yet, so we are done.
                return UpdateResult( 0 , 0 , 0 , BSONObj() );
            }
        }

        verify(nsdt);
        nsdt->notifyOfWriteOp();

        /* look for $inc etc.  note as listed here, all fields to inc must be this type, you can't set some
           regular ones at the moment. */
        if ( isOperatorUpdate ) {
            auto_ptr<ModSetState> mss = mods->prepare( obj );

            // mod set update, ie: $inc: 10 increments by 10.
            updateUsingMods( d, nsdt, pk, obj, *mss );

            if ( logop ) {
                DEV verify( mods->size() );

                BSONObj pattern = patternOrig;
                if ( mss->haveArrayDepMod() ) {
                    BSONObjBuilder patternBuilder;
                    patternBuilder.appendElements( pattern );
                    mss->appendSizeSpecForArrayDepMods( patternBuilder );
                    pattern = patternBuilder.obj();
                }

                if ( mss->needOpLogRewrite() ) {
                    logOp("u", ns, mss->getOpLogRewrite(), &pattern, 0, fromMigrate );
                }
                else {
                    logOp("u", ns, updateobj, &pattern, 0, fromMigrate );
                }
            }
            return UpdateResult( 1 , 1 , 1 , BSONObj() );
        } // end $operator update

        // regular update, just replace obj with updateobj, inherting _id if necessary
        updateNoMods( d, nsdt, pk, obj, updateobj );

        if ( logop ) {
            logOp("u", ns, updateobj, &patternOrig, 0, fromMigrate );
        }
        return UpdateResult( 1 , 0 , 1 , BSONObj() );
    }

    UpdateResult _updateObjects( bool su,
                                 const char* ns,
                                 const BSONObj& updateobj,
                                 const BSONObj& patternOrig,
                                 bool upsert,
                                 bool multi,
                                 bool logop ,
                                 OpDebug& debug,
                                 bool fromMigrate,
                                 const QueryPlanSelectionPolicy& planPolicy ) {

        tokulog(2) << "update: " << ns
                   << " update: " << updateobj
                   << " query: " << patternOrig
                   << " upsert: " << upsert << " multi: " << multi << endl;

        debug.updateobj = updateobj;

        NamespaceDetails* d = nsdetails_maybe_create(ns);
        NamespaceDetailsTransient* nsdt = &NamespaceDetailsTransient::get(ns);

        auto_ptr<ModSet> mods;
        const bool isOperatorUpdate = updateobj.firstElementFieldName()[0] == '$';
        bool modsAreIndexed = false;

        if ( isOperatorUpdate ) {
            if ( d->indexBuildInProgress() ) {
                set<string> bgKeys;
                d->inProgIdx().keyPattern().getFieldNames(bgKeys);
                mods.reset( new ModSet(updateobj, nsdt->indexKeys(), &bgKeys) );
            }
            else {
                mods.reset( new ModSet(updateobj, nsdt->indexKeys()) );
            }
            modsAreIndexed = mods->isIndexed();
        }

        if ( planPolicy.permitOptimalIdPlan() && !multi && !modsAreIndexed && mayUpdateById(d, patternOrig) ) {
            int idxNo = d->findIdIndex();
            verify(idxNo >= 0);
            debug.idhack = true;
            IndexDetails &idx = d->idx(idxNo);
            BSONObj pk = idx.getKeyFromQuery(patternOrig);
            tokulog(3) << "_updateObjects using simple _id query, pattern " << patternOrig << ", pk " << pk << endl;
            UpdateResult result = _updateById( pk,
                                               isOperatorUpdate,
                                               mods.get(),
                                               d,
                                               nsdt,
                                               ns,
                                               updateobj,
                                               patternOrig,
                                               logop,
                                               debug,
                                               fromMigrate);
            if ( result.existing || ! upsert ) {
                return result;
            }
            else if ( upsert && ! isOperatorUpdate && ! logop) {
                // this handles repl inserts
                checkNoMods( updateobj );
                debug.upsert = true;
                insertOneObject( d, nsdt, updateobj, upsert );
                return UpdateResult( 0 , 0 , 1 , updateobj );
            }
        }

        int numModded = 0;
        debug.nscanned = 0;
        shared_ptr<Cursor> c =
            NamespaceDetailsTransient::getCursor( ns, patternOrig, BSONObj(), planPolicy );

        if( c->ok() ) {
            set<BSONObj> seenObjects;
            MatchDetails details;
            auto_ptr<ClientCursor> cc;
            do {

                debug.nscanned++;

                if ( mods.get() && mods->hasDynamicArray() ) {
                    // The Cursor must have a Matcher to record an elemMatchKey.  But currently
                    // a modifier on a dynamic array field may be applied even if there is no
                    // elemMatchKey, so a matcher cannot be required.
                    //verify( c->matcher() );
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
                BSONObj pattern = patternOrig;

                if ( logop ) {
                    BSONObjBuilder idPattern;
                    BSONElement id;
                    // NOTE: If the matching object lacks an id, we'll log
                    // with the original pattern.  This isn't replay-safe.
                    // It might make sense to suppress the log instead
                    // if there's no id.
                    if ( currentObj.getObjectID( id ) ) {
                        idPattern.append( id );
                        pattern = idPattern.obj();
                    }
                    else {
                        uassert( 10157 ,  "multi-update requires all modified objects to have an _id" , ! multi );
                    }
                }

                /* look for $inc etc.  note as listed here, all fields to inc must be this type, you can't set some
                    regular ones at the moment. */
                if ( isOperatorUpdate ) {

                    if ( multi ) {
                        // Make our own copies of the currPK and currentObj before we invalidate
                        // them by advancing the cursor.
                        currPK = currPK.copy();
                        currentObj = currentObj.copy();

                        // Advance past the document to be modified. This used to be because of SERVER-5198,
                        // but TokuDB does it because we want to avoid needing to do manual deduplication
                        // of this PK on the next iteration if the current update modifies the next
                        // entry in the index. For example, an index scan over a:1 with mod {$inc: {a:1}}
                        // would cause every other key read to be a duplicate if we didn't advance here.
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
                    bool forceRewrite = false;

                    auto_ptr<ModSet> mymodset;
                    if ( details.hasElemMatchKey() && mods->hasDynamicArray() ) {
                        useMods = mods->fixDynamicArray( details.elemMatchKey() );
                        mymodset.reset( useMods );
                        forceRewrite = true;
                    }

                    auto_ptr<ModSetState> mss = useMods->prepare( currentObj );
                    updateUsingMods( d, nsdt, currPK, currentObj, *mss );

                    if ( logop ) {
                        DEV verify( mods->size() );

                        if ( mss->haveArrayDepMod() ) {
                            BSONObjBuilder patternBuilder;
                            patternBuilder.appendElements( pattern );
                            mss->appendSizeSpecForArrayDepMods( patternBuilder );
                            pattern = patternBuilder.obj();
                        }

                        if ( forceRewrite || mss->needOpLogRewrite() ) {
                            logOp("u", ns, mss->getOpLogRewrite() , &pattern, 0, fromMigrate );
                        }
                        else {
                            logOp("u", ns, updateobj, &pattern, 0, fromMigrate );
                        }
                    }
                    numModded++;
                    if ( ! multi )
                        return UpdateResult( 1 , 1 , numModded , BSONObj() );

                    continue;
                } // end if operator is update

                uassert( 10158 ,  "multi update only works with $ operators" , ! multi );

                updateNoMods( d, nsdt, currPK, currentObj, updateobj );

                if ( logop ) {
                    DEV wassert( !su ); // super used doesn't get logged, this would be bad.
                    logOp("u", ns, updateobj, &pattern, 0, fromMigrate );
                }
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
                insertAndLog( ns, d, nsdt, newObj, upsert, logop, fromMigrate );
                return UpdateResult( 0 , 1 , 1 , newObj );
            }
            uassert( 10159 ,  "multi update only works with $ operators" , ! multi );
            debug.upsert = true;
            insertAndLog( ns, d, nsdt, newObj, upsert, logop, fromMigrate );
            return UpdateResult( 0 , 0 , 1 , newObj );
        }

        return UpdateResult( 0 , isOperatorUpdate , 0 , BSONObj() );
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

        uassert( 10155 , "cannot update reserved $ collection", strchr(ns, '$') == 0 );
        if ( strstr(ns, ".system.") ) {
            /* dm: it's very important that system.indexes is never updated as IndexDetails has pointers into it */
            uassert( 10156,
                     str::stream() << "cannot update system collection: " << ns << " q: " << patternOrig << " u: " << updateobj,
                     legalClientSystemNS( ns , true ) );
        }

        UpdateResult ur = _updateObjects(false, ns, updateobj, patternOrig,
                                         upsert, multi, logop,
                                         debug, fromMigrate, planPolicy );
        debug.nupdated = ur.num;
        return ur;
    }

}  // namespace mongo
