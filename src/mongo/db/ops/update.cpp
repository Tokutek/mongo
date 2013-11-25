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
#include "mongo/db/server_parameters.h"
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

    static void checkNoMods(const BSONObj &obj) {
        for (BSONObjIterator i(obj); i.more(); ) {
            const BSONElement &e = i.next();
            uassert(10154, "Modifiers and non-modifiers cannot be mixed", e.fieldName()[0] != '$');
        }
    }

    static void checkTooLarge(const BSONObj &obj) {
        uassert(12522, "$ operator made object too large", obj.objsize() <= BSONObjMaxUserSize);
    }

    ExportedServerParameter<bool> _fastupdatesParameter(
            ServerParameterSet::getGlobal(), "fastupdates", &cmdLine.fastupdates, true, true);
    ExportedServerParameter<bool> _fastupdatesIgnoreErrorsParameter(
            ServerParameterSet::getGlobal(), "fastupdatesIgnoreErrors", &cmdLine.fastupdatesIgnoreErrors, true, true);

    // Apply an update message supplied by a NamespaceDetails to
    // some row in an in IndexDetails (for fast ydb updates).
    //
    class ApplyUpdateMessage : public storage::UpdateCallback {
        // @param pkQuery - the pk with field names, for proper default obj construction
        //                  in mods.createNewFromQuery().
        BSONObj upsert(const BSONObj &pkQuery, const BSONObj &msg) {
            // Create a new object from the pk and updateobj.
            ModSet mods(msg);
            const BSONObj newObj = mods.createNewFromQuery(pkQuery);
            checkTooLarge(newObj);
            return newObj;
        }
        BSONObj applyMods(const BSONObj &oldObj, const BSONObj &msg) {
            try {
                // The update message is simply an update object, supplied by the user.
                ModSet mods(msg);
                auto_ptr<ModSetState> mss = mods.prepare(oldObj);
                const BSONObj newObj = mss->createNewFromMods();
                checkTooLarge(newObj);
                return newObj;
            } catch (const std::exception &ex) {
                // Applying an update message in this fashion _always_ ignores errors.
                // That is the risk you take when using --fastupdates.
                //
                // We will print such errors to the server's error log no more than once per 5 seconds.
                if (!cmdLine.fastupdatesIgnoreErrors && _loggingTimer.millisReset() > 5000) {
                    problem() << "* Failed to apply \"--fastupdate\" updateobj message! "
                                 "This means an update operation that appeared successful actually failed." << endl;
                    problem() << "* It probably should not be happening in production. To ignore these errors, "
                                 "set the server parameter fastupdatesIgnoreErrors=true" << endl;
                    problem() << "*    doc: " << oldObj << endl;
                    problem() << "*    updateobj: " << msg << endl;
                    problem() << "*    exception: " << ex.what() << endl;
                }
                // IMPORTANT
                // TODO: Put this in some counter so serverStatus can report the number
                //       of silently failing updates for monitoring purposes.
                return oldObj;
            }
        }
    private:
        Timer _loggingTimer;
    } _storageUpdateCallback; // installed as the ydb update callback in db.cpp via set_update_callback

    static void updateUsingMods(NamespaceDetails *d, const BSONObj &pk, const BSONObj &obj,
                                ModSetState &mss, const bool modsAreIndexed,
                                const bool logop, const bool fromMigrate) {
        const BSONObj newObj = mss.createNewFromMods();
        checkTooLarge(newObj);
        updateOneObject(d, pk, obj, newObj, logop, fromMigrate,
                        modsAreIndexed ? 0 : NamespaceDetails::KEYS_UNAFFECTED_HINT);
    }

    static void updateNoMods(NamespaceDetails *d, const BSONObj &pk, const BSONObj &obj, const BSONObj &updateobj,
                             const bool logop, const bool fromMigrate) {
        // This is incredibly un-intiutive, but it takes a const BSONObj
        // and modifies it in-place if a timestamp needs to be set.
        BSONElementManipulator::lookForTimestamps(updateobj);
        checkNoMods(updateobj);
        updateOneObject(d, pk, obj, updateobj, logop, fromMigrate);
    }

    static UpdateResult upsertAndLog(NamespaceDetails *d, const BSONObj &patternOrig,
                                     const BSONObj &updateobj, const bool isOperatorUpdate,
                                     ModSet *mods, const bool logop) {
        const string &ns = d->ns();
        uassert(16893, str::stream() << "Cannot upsert a collection under-going bulk load: " << ns,
                       ns != cc().bulkLoadNS());

        BSONObj newObj = updateobj;
        if (isOperatorUpdate) {
            newObj = mods->createNewFromQuery(patternOrig);
            cc().curop()->debug().fastmodinsert = true;
        } else {
            cc().curop()->debug().upsert = true;
        }

        checkNoMods(newObj);
        insertOneObject(d, newObj);
        if (logop) {
            OpLogHelpers::logInsert(ns.c_str(), newObj);
        }
        return UpdateResult(0, isOperatorUpdate, 1, newObj);
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

    UpdateResult updateByPK(NamespaceDetails *d,
                            const BSONObj &pk, const BSONObj &patternOrig,
                            const BSONObj &updateobj,
                            const bool upsert, const bool fastupdateOk,
                            const bool logop, const bool fromMigrate,
                            uint64_t flags) {
        // Create a mod set for $ style updates.
        scoped_ptr<ModSet> mods;
        const bool isOperatorUpdate = updateobj.firstElementFieldName()[0] == '$';
        if (isOperatorUpdate) {
            mods.reset(new ModSet(updateobj, d->indexKeys()));
        }

        if (fastupdateOk && mods && !mods->isIndexed() &&
            !hasClusteringSecondaryKey(d)) {
            // Fast update path that skips the pk query.
            // We know no indexes need to be updated so we don't read the full object.
            //
            // Further, we specifically do _not_ check if upsert is true because it's
            // implied when using fastupdates.
            cc().curop()->debug().fastmod = true;
            d->updateObjectMods(pk, updateobj, logop, fromMigrate, flags);
            d->notifyOfWriteOp();
            return UpdateResult(0, 1, 1, BSONObj());
        }

        BSONObj obj;
        ResultDetails queryResult;
        if (mods && mods->hasDynamicArray()) {
            queryResult.matchDetails.requestElemMatchKey();
        }

        const bool found = queryByPKHack(d, pk, patternOrig, obj, &queryResult);
        if (!found) {
            if (!upsert) {
                return UpdateResult(0, 0, 0, BSONObj());
            }
            return upsertAndLog(d, patternOrig, updateobj, isOperatorUpdate, mods.get(), logop);
        }

        if (isOperatorUpdate) {
            // operator-style update
            if (queryResult.matchDetails.hasElemMatchKey() && mods->hasDynamicArray()) {
                mods.reset(mods->fixDynamicArray(queryResult.matchDetails.elemMatchKey()));
            }
            auto_ptr<ModSetState> mss = mods->prepare(obj, false /* not an insertion */);
            updateUsingMods(d, pk, obj, *mss, mods->isIndexed() > 0, logop, fromMigrate);
        } else {
            // replace-style update
            updateNoMods(d, pk, obj, updateobj, logop, fromMigrate);
        }
        return UpdateResult(1, isOperatorUpdate, 1, BSONObj());
    }

    // return true if the given updateobj can be 'unapplied'
    // on a replica set member performing rollback.
    //
    // this will be true case for things like $inc X, because
    // its inverse is $inc -X.
    // 
    // it will be false for things like $addToSet(set, X), because
    // there's no way to know for sure if the right thing to do is
    // remove X from the set, or keep the set the same (because X
    // may have already existed prior to the addToSet operation).
    static bool modsAreInvertible(const BSONObj &updateobj) {
        for (BSONObjIterator i(updateobj); i.more(); ) {
            const BSONElement &e = i.next();
            // For now, only pure $inc updates are considered invertible.
            if (!str::equals(e.fieldName(), "$inc")) {
                return false;
            }
        }
        return true;
    }

    BSONObj invertUpdateMods(const BSONObj &updateobj) {
        BSONObjBuilder b(updateobj.objsize());
        for (BSONObjIterator i(updateobj); i.more(); ) {
            const BSONElement &e = i.next();
            verify(str::equals(e.fieldName(), "$inc"));
            BSONObjBuilder inc(b.subobjStart("$inc"));
            for (BSONObjIterator o(e.Obj()); o.more(); ) {
                const BSONElement &fieldToInc = o.next();
                verify(fieldToInc.isNumber());
                const long long invertedValue = -fieldToInc.numberLong();
                inc.append(fieldToInc.fieldName(), invertedValue);
            }
            inc.done();
        }
        return b.obj();
    }

    static UpdateResult _updateObjects(const char *ns,
                                       const BSONObj &updateobj,
                                       const BSONObj &patternOrig,
                                       const bool upsert, const bool multi,
                                       const bool logop, const bool fromMigrate) {
        TOKULOG(2) << "update: " << ns
                   << " update: " << updateobj
                   << " query: " << patternOrig
                   << " upsert: " << upsert << " multi: " << multi << endl;

        NamespaceDetails *d = getAndMaybeCreateNS(ns, logop);

        // Fast-path for simple primary key updates.
        const BSONObj pk = d->getSimplePKFromQuery(patternOrig);
        if (!pk.isEmpty() && !d->isCapped()) { // don't fastupdate capped collections
            // We check here that the fastupdates are okay to do.
            // - cmdline switch must be enabled
            // - NamespaceDetails must ok with it (may not be for some sharded collections)
            // - modifications to the destination object must be invertible (for repl rollback)
            const bool fastupdatesOk = cmdLine.fastupdates && d->fastupdatesOk() && modsAreInvertible(updateobj);
            return updateByPK(d, pk, patternOrig, updateobj,
                              upsert, fastupdatesOk, logop, fromMigrate);
        }

        // Run a regular update using the query optimizer.

        set<BSONObj> seenObjects;
        MatchDetails details;
        auto_ptr<ModSet> mods;

        const bool isOperatorUpdate = updateobj.firstElementFieldName()[0] == '$';
        if (isOperatorUpdate) {
            mods.reset(new ModSet(updateobj, d->indexKeys()));
            if (mods->hasDynamicArray()) {
                details.requestElemMatchKey();
            }
        }

        int numModded = 0;
        cc().curop()->debug().nscanned = 0;
        for (shared_ptr<Cursor> c = getOptimizedCursor(ns, patternOrig); c->ok(); ) {
            cc().curop()->debug().nscanned++;
            BSONObj currPK = c->currPK();
            if (c->getsetdup(currPK)) {
                c->advance();
                continue;
            }
            if (!c->currentMatches(&details)) {
                c->advance();
                continue;
            }

            BSONObj currentObj = c->current();
            if (!isOperatorUpdate) {
                // replace-style update only affects a single matching document
                uassert(10158, "multi update only works with $ operators", !multi);
                updateNoMods(d, currPK, currentObj, updateobj, logop, fromMigrate);
                return UpdateResult(1, 0, 1, BSONObj());
            }

            // operator-style updates may affect many documents
            if (multi) {
                // Advance past the document to be modified - SERVER-5198,
                // First, get owned copies of currPK/currObj, which live in the cursor.
                currPK = currPK.getOwned();
                currentObj = currentObj.getOwned();
                while (c->ok() && currPK == c->currPK()) {
                    c->advance();
                }

                // Multi updates need to do their own deduplication because updates may modify the
                // keys the cursor is in the process of scanning over.
                if ( seenObjects.count(currPK) ) {
                    continue;
                } else {
                    seenObjects.insert(currPK);
                }
            }

            ModSet *useMods = mods.get();
            auto_ptr<ModSet> mymodset;
            if (details.hasElemMatchKey() && mods->hasDynamicArray()) {
                useMods = mods->fixDynamicArray(details.elemMatchKey());
                mymodset.reset(useMods);
            }
            auto_ptr<ModSetState> mss = useMods->prepare(currentObj, false /* not an insertion */);
            updateUsingMods(d, currPK, currentObj, *mss, useMods->isIndexed() > 0, logop, fromMigrate);
            numModded++;

            if (!multi) {
                break;
            }
        }

        if (numModded) {
            // We've modified something, so we're done.
            return UpdateResult(1, 1, numModded, BSONObj());
        }
        if (!upsert) {
            // We haven't modified anything, but we're not trying to upsert, so we're done.
            return UpdateResult(0, isOperatorUpdate, numModded, BSONObj());
        }

        if (!isOperatorUpdate) {
            uassert(10159, "multi update only works with $ operators", !multi);
        }
        // Upsert a new object
        return upsertAndLog(d, patternOrig, updateobj, isOperatorUpdate, mods.get(), logop);
    }

    UpdateResult updateObjects(const char *ns,
                               const BSONObj &updateobj, const BSONObj &patternOrig,
                               const bool upsert, const bool multi,
                               const bool logop, const bool fromMigrate) {
        uassert(10155, "cannot update reserved $ collection", NamespaceString::normal(ns));
        if (NamespaceString::isSystem(ns)) {
            uassert(10156, str::stream() << "cannot update system collection: " << ns <<
                                            " q: " << patternOrig << " u: " << updateobj,
                           legalClientSystemNS(ns , true));
        }

        cc().curop()->debug().updateobj = updateobj;

        UpdateResult ur = _updateObjects(ns, updateobj, patternOrig,
                                         upsert, multi, logop, fromMigrate);

        cc().curop()->debug().nupdated = ur.num;
        return ur;
    }

}  // namespace mongo
