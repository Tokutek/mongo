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

#include "mongo/base/counter.h"
#include "mongo/db/oplog.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/db/collection.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/ops/query.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/update_internal.h"
#include "mongo/db/oplog_helpers.h"

namespace mongo {

    static bool hasClusteringSecondaryKey(Collection *cl) {
        for (int i = 0; i < cl->nIndexesBeingBuilt(); i++) {
            IndexDetails &idx = cl->idx(i);
            if (!cl->isPKIndex(idx) && idx.clustering()) {
                // has a clustering secondary key
                return true;
            }
        }
        // no clustering secondary keys
        return false;
    }

    // if true, updates must log the full pre-image and post-image in the oplog,
    // and cannot log mods in its place
    bool forceLogFullUpdate(Collection* cl, ModSet* useMods) {
        return useMods->hasDynamicArray() || cl->isCapped() || NamespaceString::isSystem(cl->ns());
    }

    bool logOfPreImageRequired(Collection* cl) {
        // if we are dealing with a hidden PK, we must log the
        // full pre-image of the document in the oplog because
        // the new rollback algorithm cannot deal with hidden primary keys
        // Therefore, those documents must be able to use the old
        // rollback algorithm, which is to undo the previous op
        // manually.
        return cl->isPKHidden();
    }

    // returns true if we are not allowed to run Collection::updateObjectMods,
    // and must instead perform an update using the entire pre-image and
    // post-image
    bool doFullUpdate(Collection* cl, bool upsert, ModSet* useMods) {
        bool modsAreIndexed = useMods->isIndexed() > 0;
        if (upsert) {
            return !cl->fastUpsertsOk();
        }

        // adding cl->indexBuildInProgress() as a check below due to #1085
        // This is a little heavyweight, as we whould be able to have modsAreIndexed
        // take hot indexes into account. Unfortunately, that code right now is not
        // factored cleanly enough to do nicely, so we just do the heavyweight check
        // here. Hope to get this properly fixed soon.
        return (forceLogFullUpdate(cl, useMods) ||
            modsAreIndexed ||
            cl->indexBuildInProgress() ||
            hasClusteringSecondaryKey(cl)
            );
    }

    bool updateOneObjectWithMods(Collection *cl, const BSONObj &pk, 
                         const BSONObj &updateobj, const BSONObj& query,
                         const uint32_t fastUpdateFlags,
                         const bool fromMigrate,
                         uint64_t flags,
                         ModSet* useMods)
    {
        if (!doFullUpdate(cl, fastUpdateFlags & UpdateFlags::UPSERT, useMods)) {
            // - operator style update gets applied as an update message
            // - does not maintain sencondary indexes so we can only do it
            // when no indexes were affected
            cl->updateObjectMods(pk, updateobj, query, fastUpdateFlags, fromMigrate, flags, fastUpdateFlags & UpdateFlags::UPSERT);
            cl->notifyOfWriteOp();
            return true;
        }
        return false;
    }

    void updateOneObject(Collection *cl, const BSONObj &pk,
                         const BSONObj &oldObj, BSONObj &newObj,
                         const bool fromMigrate,
                         uint64_t flags) {
        cl->updateObject(pk, oldObj, newObj, fromMigrate, flags);
        cl->notifyOfWriteOp();
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

    static bool fastUpdatesEnabled = false;
    ExportedServerParameter<bool> _fastUpdatesParameter(
            ServerParameterSet::getGlobal(), "fastUpdates", &fastUpdatesEnabled, true, true);
    static bool fastUpdatesIgnoreErrors = false;
    ExportedServerParameter<bool> _fastUpdatesIgnoreErrorsParameter(
            ServerParameterSet::getGlobal(), "fastUpdatesIgnoreErrors", &fastUpdatesIgnoreErrors, true, true);

    static Counter64 fastUpdatesErrors;
    static ServerStatusMetricField<Counter64> fastUpdatesIgnoredErrorsDisplay("fastUpdates.errors", &fastUpdatesErrors);
    static Counter64 fastUpdatesByPKPerformed;
    static ServerStatusMetricField<Counter64> fastUpdatesPerformedPKDisplay("fastUpdates.performed.primaryKey", &fastUpdatesByPKPerformed);
    static Counter64 fastUpdatesBySecPerformed;
    static ServerStatusMetricField<Counter64> fastUpdatesPerformedSecDisplay("fastUpdates.performed.secondaryKey", &fastUpdatesBySecPerformed);
    static Counter64 fastUpdatesPKEligible;
    static ServerStatusMetricField<Counter64> fastUpdatesEligiblePKDisplay("fastUpdates.eligible.primaryKey", &fastUpdatesPKEligible);
    static Counter64 fastUpdatesSecEligible;
    static ServerStatusMetricField<Counter64> fastUpdatesEligibleSecDisplay("fastUpdates.eligible.secondaryKey", &fastUpdatesSecEligible);

    bool ApplyUpdateMessage::applyMods(
        const BSONObj &oldObj,
        const BSONObj &msg,
        const BSONObj& query,
        const uint32_t fastUpdateFlags,
        BSONObj& newObj
        ) 
    {
        // first, a check that only the flags we support
        // are listed here. We don't want a future version's flag to somehow
        // erroneously make it here (e.g., a future upsert flag)
        verify(fastUpdateFlags < UpdateFlags::MAX);
        if (oldObj.isEmpty() && ((fastUpdateFlags & UpdateFlags::UPSERT) == 0)) {
            // if this update message is allowed to not have an old obj
            // we simply return false, otherwise, we uassert
            if (fastUpdateFlags & UpdateFlags::NO_OLDOBJ_OK) {
                return false;
            }
            uasserted(17315, "Got an empty old_val or old_val->data in runUpdateMods, should not happen");
        }
        try {
            ModSet mods(msg);
            // upsert case, that is, we are doing an insertion
            // because the object does not exist. If this was
            // not an upsert, we should have exited
            // this function already
            if (oldObj.isEmpty()) {
                // this should be true thanks to the if clause right
                // above this try block
                verify(fastUpdateFlags & UpdateFlags::UPSERT);
                // upserts must pass in the query
                // tryFastUpdate ensures this
                verify(!query.isEmpty());
                if (!(fastUpdateFlags & UpdateFlags::NON_CLUSTERING)) {
                    newObj = mods.createNewFromQuery(query);
                }
            }
            // normal update case
            else {
                verify(!mods.hasDynamicArray());
                if (!query.isEmpty()) {
                    // note, the mods used should not have hasDynamicArray()
                    // be false, making this code ok. This fact is asserted above
                    ResultDetails queryResult;
                    Matcher matcher(query);
                    bool matches = matcher.matches(oldObj, &queryResult.matchDetails);
                    if (!matches) {
                        return false;
                    }
                }
                // The update message is simply an update object, supplied by the user.
                auto_ptr<ModSetState> mss = mods.prepare(oldObj, false);
                newObj = mss->createNewFromMods();
                checkTooLarge(newObj);
            }
            return true;
        } catch (const std::exception &ex) {
            // if a fast update has been performed, it is possible the user performed
            // an update that throws an error, like incrementing a text field
            // in such cases, we should just print something notifying the user.
            // In all other cases, it shows a serious problem. For example, if this
            // if clause is false and we are running on a secondary, it might
            // be an indication that data is out of sync with the primary.
            if (fastUpdateFlags & UpdateFlags::FAST_UPDATE_PERFORMED) {
                // Applying an update message in this fashion _always_ ignores errors.
                // That is the risk you take when using --fastUpdates.
                //
                // We will print such errors to the server's error log no more than once per 5 seconds.
                if (!fastUpdatesIgnoreErrors && _loggingTimer.millis() > 5000) {
                    _loggingTimer.reset();
                    problem() << "* Failed to apply \"--fastupdate\" updateobj message! "
                                 "This means an update operation that appeared successful actually failed." << endl;
                    problem() << "* It probably should not be happening in production. To ignore these errors, "
                                 "set the server parameter fastUpdatesIgnoreErrors=true" << endl;
                    problem() << "*    doc: " << oldObj << endl;
                    problem() << "*    updateobj: " << msg << endl;
                    problem() << "*    exception: " << ex.what() << endl;
                }
                fastUpdatesErrors.increment(1);
                return false;
            }
            throw;
        }
    }
    ApplyUpdateMessage _storageUpdateCallback; // installed as the ydb update callback in db.cpp via set_update_callback

    static void updateUsingMods(const char *ns, Collection *cl, const BSONObj &pk, const BSONObj &obj,
                                const BSONObj &updateobj, shared_ptr<ModSet> mods, MatchDetails* details,
                                const bool fromMigrate) {
        ModSet *useMods = mods.get();
        auto_ptr<ModSet> mymodset;
        bool hasDynamicArray = mods->hasDynamicArray();
        if (details->hasElemMatchKey() && hasDynamicArray) {
            useMods = mods->fixDynamicArray(details->elemMatchKey());
            mymodset.reset(useMods);
        }

        auto_ptr<ModSetState> mss = useMods->prepare(obj, false /* not an insertion */);
        BSONObj newObj = mss->createNewFromMods();
        checkTooLarge(newObj);

        // attempt to update the object using mods. If not possible, then
        // do the more heavyweight method of using a full pre-image followed
        // by a full post image
        bool didUpdateWithMods = updateOneObjectWithMods(cl, pk, updateobj, BSONObj(), 0, fromMigrate, 0, useMods);
        if (!didUpdateWithMods) {
            bool modsAreIndexed = useMods->isIndexed() > 0;
            uint64_t flags = (modsAreIndexed || cl->indexBuildInProgress()) ? 
                0 : 
                Collection::KEYS_UNAFFECTED_HINT;
            updateOneObject(cl, pk, obj, newObj, fromMigrate, flags);
        }

        // must happen after updateOneObject
        if (forceLogFullUpdate(cl, useMods)) {
            verify(!didUpdateWithMods); // sanity check
            OplogHelpers::logUpdate(ns, pk, obj, newObj, fromMigrate);
        }
        else if (didUpdateWithMods && !logOfPreImageRequired(cl)) {
            // this method logs just the pk and not obj
            // we still need to pass in obj for sharding
            OplogHelpers::logUpdatePKModsWithRow(ns, pk, obj, updateobj, BSONObj(), 0, fromMigrate);
        }
        else {
            OplogHelpers::logUpdateModsWithRow(ns, pk, obj, updateobj, fromMigrate);
        }
    }

    static void updateNoMods(const char *ns, Collection *cl, const BSONObj &pk, const BSONObj &obj, BSONObj &updateobj,
                             const bool fromMigrate) {
        // This is incredibly un-intiutive, but it takes a const BSONObj
        // and modifies it in-place if a timestamp needs to be set.
        BSONElementManipulator::lookForTimestamps(updateobj);
        checkNoMods(updateobj);
        updateOneObject(cl, pk, obj, updateobj, fromMigrate, 0);
        // must happen after updateOneObject
        OplogHelpers::logUpdate(ns, pk, obj, updateobj, fromMigrate);
    }

    static UpdateResult upsertAndLog(Collection *cl, const BSONObj &patternOrig,
                                     const BSONObj &updateobj, const bool isOperatorUpdate,
                                     ModSet *mods, bool fromMigrate) {
        const string &ns = cl->ns();
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
        insertOneObject(cl, newObj);
        OplogHelpers::logInsert(ns.c_str(), newObj, fromMigrate);
        return UpdateResult(0, isOperatorUpdate, 1, newObj);
    }

    static bool canRunFastUpdate(
        Collection *cl,
        const bool upsert,
        ModSet* mods,
        const bool isOperatorUpdate,
        bool* eligible
        )
    {
        *eligible = false;
        if (!isOperatorUpdate) {
            return false;
        }
        verify(mods);
        if (doFullUpdate(cl, upsert, mods) || logOfPreImageRequired(cl) || !cl->fastupdatesOk()) {
            return false;
        }
        verify(!forceLogFullUpdate(cl, mods));
        *eligible = true;
        if (!fastUpdatesEnabled) {
            return false;
        }
        return true;
    }

    static bool tryFastUpdate(const char *ns, Collection *cl,
                            const BSONObj &pk, const BSONObj &query,
                            const BSONObj &updateobj,
                            const bool upsert,
                            const bool fromMigrate,
                            ModSet* mods,
                            const bool isOperatorUpdate,
                            const bool oldObjMayNotExist,
                            bool* eligible)
    {
        if (!canRunFastUpdate(cl, upsert, mods, isOperatorUpdate, eligible)) {
            return false;
        }
        verify(mods);

        // looks like we are good to go
        
        // a little optimization to get rid of the query, if we can
        const bool singleQueryField = query.nFields() == 1; // TODO: Optimize?
        // upserts require the query to be there, so that we
        // can generated the proper document in case
        // it is an insert. We may be able to do without it for upserts,
        // but easier to just put it in.
        // To see upsert's needing it, look at ApplyUpdateMessage::applyMods
        const BSONObj queryToUse = (singleQueryField && !upsert) ? BSONObj() : query;
        uint32_t fastUpdateFlags = UpdateFlags::FAST_UPDATE_PERFORMED;
        if (oldObjMayNotExist) {
            fastUpdateFlags |= UpdateFlags::NO_OLDOBJ_OK;
        }
        if (upsert) {
            fastUpdateFlags |= UpdateFlags::UPSERT;
        }
        bool success = updateOneObjectWithMods(cl, pk, updateobj, queryToUse, fastUpdateFlags, fromMigrate, 0, mods);
        verify(success);
        BSONObj filledPK = cl->fillPKWithFields(pk);
        OplogHelpers::logUpdatePKModsWithRow(ns, pk, filledPK, updateobj, queryToUse, fastUpdateFlags, fromMigrate);
        return true;
    }

    static UpdateResult updateByPK(const char *ns, Collection *cl,
                            const BSONObj &pk, const BSONObj &patternOrig,
                            const BSONObj &updateobj,
                            const bool upsert,
                            const bool fromMigrate) {
        // Create a mod set for $ style updates.
        shared_ptr<ModSet> mods;
        const bool isOperatorUpdate = updateobj.firstElementFieldName()[0] == '$';
        if (isOperatorUpdate) {
            mods.reset(new ModSet(updateobj, cl->indexKeys()));
        }

        // try a fast update, if it succeeds, get out, otherwise,
        // proceed with fetching the pre-image
        bool eligibleForFastUpdate = false;        
        if (tryFastUpdate(ns, cl, pk, patternOrig, updateobj, upsert, fromMigrate, mods.get(), isOperatorUpdate, true, &eligibleForFastUpdate)) {
            fastUpdatesByPKPerformed.increment();
            return UpdateResult(1, isOperatorUpdate, 1, BSONObj());
        }
        if (eligibleForFastUpdate) {
            // track the fact that this update could have been fast if fastUpdates were enabled
            fastUpdatesPKEligible.increment();
        }

        BSONObj obj;
        ResultDetails queryResult;
        if (mods && mods->hasDynamicArray()) {
            queryResult.matchDetails.requestElemMatchKey();
        }
        const bool found = queryByPKHack(cl, pk, patternOrig, obj, &queryResult);
        if (!found) {
            if (!upsert) {
                return UpdateResult(0, 0, 0, BSONObj());
            }
            return upsertAndLog(cl, patternOrig, updateobj, isOperatorUpdate, mods.get(), fromMigrate);
        }

        if (isOperatorUpdate) {
            updateUsingMods(ns, cl, pk, obj, updateobj, mods, &queryResult.matchDetails, fromMigrate);
        } else {
            // replace-style update
            BSONObj copy = updateobj.copy();
            updateNoMods(ns, cl, pk, obj, copy, fromMigrate);
        }
        return UpdateResult(1, isOperatorUpdate, 1, BSONObj());
    }

    static UpdateResult _updateObjects(const char *ns,
                                       const BSONObj &updateobj,
                                       const BSONObj &patternOrig,
                                       const bool upsert, const bool multi,
                                       const bool fromMigrate) {
        TOKULOG(2) << "update: " << ns
                   << " update: " << updateobj
                   << " query: " << patternOrig
                   << " upsert: " << upsert << " multi: " << multi << endl;

        Collection *cl = getOrCreateCollection(ns, true);
        const bool isOperatorUpdate = updateobj.firstElementFieldName()[0] == '$';
        if (!isOperatorUpdate) {
            // replace-style update only affects a single matching document
            uassert(10158, "multi update only works with $ operators", !multi);
        }

        // Fast-path for simple primary key updates.
        //
        // - We don't do it for capped collections since  their documents may not grow,
        // and the fast path doesn't know if docs grow until the update message is applied.
        if (!cl->isCapped()) {
            const BSONObj pk = cl->getSimplePKFromQuery(patternOrig);
            if (!pk.isEmpty()) {
                return updateByPK(ns, cl, pk, patternOrig, updateobj,
                                  upsert, fromMigrate);
            }
        }

        // Run a regular update using the query optimizer.

        set<BSONObj> seenObjects;
        MatchDetails details;
        shared_ptr<ModSet> mods;

        if (isOperatorUpdate) {
            mods.reset(new ModSet(updateobj, cl->indexKeys()));
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
            bool eligibleForFastUpdate = false;
            bool canBeFast = canRunFastUpdate(cl, false, mods.get(), isOperatorUpdate, &eligibleForFastUpdate);

            if (!isOperatorUpdate) {
                verify(!multi); // should be uasserted above
                verify(!canBeFast); // just a sanity check to make sure we are not losing performance
                BSONObj currentObj = c->current();
                BSONObj copy = updateobj.copy();
                updateNoMods(ns, cl, currPK, currentObj, copy, fromMigrate);
                return UpdateResult(1, 0, 1, BSONObj());
            }

            // operator-style updates may affect many documents
            BSONObj currentObj;
            if (!canBeFast) {
                currentObj = c->current();
            }
            if (multi) {
                // Advance past the document to be modified - SERVER-5198,
                // First, get owned copies of currPK/currObj, which live in the cursor.
                currPK = currPK.getOwned();
                if (!canBeFast) {
                    currentObj = currentObj.getOwned();
                }
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

            if (canBeFast) {
                verify(currentObj.isEmpty()); // sanity check
                bool eligible;
                bool ranFast = tryFastUpdate(
                    ns,
                    cl,
                    currPK,
                    BSONObj(), // no query needed
                    updateobj,
                    false,
                    fromMigrate,
                    mods.get(),
                    isOperatorUpdate,
                    false, // old obj must exist in main dictionary
                    &eligible
                    );
                fastUpdatesBySecPerformed.increment();
                verify(ranFast); // must have succeeded because canBeFast is true
            }
            else {
                if (eligibleForFastUpdate) {
                    // track the fact that this update could have been fast if fastUpdates were enabled
                    fastUpdatesSecEligible.increment();
                }
                verify(!currentObj.isEmpty()); // sanity check
                updateUsingMods(ns, cl, currPK, currentObj, updateobj, mods, &details, fromMigrate);
            }
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
        return upsertAndLog(cl, patternOrig, updateobj, isOperatorUpdate, mods.get(), fromMigrate);
    }

    UpdateResult updateObjects(const char *ns,
                               const BSONObj &updateobj, const BSONObj &patternOrig,
                               const bool upsert, const bool multi,
                               const bool fromMigrate) {
        uassert(10155, "cannot update reserved $ collection", NamespaceString::normal(ns));
        if (NamespaceString::isSystem(ns)) {
            uassert(10156, str::stream() << "cannot update system collection: " << ns <<
                                            " q: " << patternOrig << " u: " << updateobj,
                           legalClientSystemNS(ns , true));
        }

        UpdateResult ur = _updateObjects(ns, updateobj, patternOrig,
                                         upsert, multi, fromMigrate);

        cc().curop()->debug().nupdated = ur.num;
        return ur;
    }

}  // namespace mongo
