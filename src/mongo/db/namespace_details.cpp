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

#include "mongo/pch.h"

#include <algorithm>
#include <list>
#include <map>
#include <vector>
#include <utility>

#include <boost/filesystem/operations.hpp>

#include "mongo/base/init.h"
#include "mongo/base/units.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/collection.h"
#include "mongo/db/cursor.h"
#include "mongo/db/database.h"
#include "mongo/db/databaseholder.h"
#include "mongo/db/json.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/db/oplog.h"
#include "mongo/db/relock.h"
#include "mongo/db/txn_context.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/storage/dbt.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/txn.h"
#include "mongo/db/storage/key.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/db/repl/rs_optime.h"
#include "mongo/db/repl/rs.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/s/d_logic.h"
#include "mongo/scripting/engine.h"

namespace mongo {

    static void removeFromNamespacesCatalog(const StringData &ns);
    static void removeFromIndexesCatalog(const StringData &ns, const StringData &name);

    NamespaceIndex *nsindex(const StringData& ns) {
        Database *database = cc().database();
        verify( database );
        DEV {
            StringData db = nsToDatabaseSubstring(ns);
            if ( db != database->name() ) {
                out() << "ERROR: attempt to write to wrong database\n";
                out() << " ns:" << ns << '\n';
                out() << " database->name:" << database->name() << endl;
                verify( db == database->name() );
            }
        }
        return &database->_nsIndex;
    }

    NamespaceDetails *nsdetails(const StringData& ns) {
        return nsindex(ns)->details(ns);
    }

    NamespaceDetails *nsdetails_maybe_create(const StringData& ns, BSONObj options) {
        NamespaceIndex *ni = nsindex(ns);
        if (!ni->allocated()) {
            // Must make sure we loaded any existing namespaces before checking, or we might create one that already exists.
            ni->init(true);
        }
        NamespaceDetails *details = ni->details(ns);
        if (details == NULL) {
            TOKULOG(2) << "Didn't find nsdetails(" << ns << "), creating it." << endl;
            if (!Lock::isWriteLocked(ns)) {
                throw RetryWithWriteLock();
            }

            shared_ptr<NamespaceDetails> new_details( NamespaceDetails::make(ns, options) );
            ni->add_ns(ns, new_details);

            details = ni->details(ns);
            details->addDefaultIndexesToCatalog();

            // Keep the call to 'str()', it allows us to call it in gdb.
            TOKULOG(2) << "Created nsdetails " << options.str() << endl;
        }
        return details;
    }

    /* ------------------------------------------------------------------------- */

    BSONObj NamespaceDetails::indexInfo(const BSONObj &keyPattern, bool unique, bool clustering) const {
        BSONObjBuilder b;
        b.append("ns", _ns);
        b.append("key", keyPattern);
        if (keyPattern == BSON("_id" << 1)) {
            b.append("name", "_id_");
        } else if (keyPattern == BSON("$_" << 1)) {
            b.append("name", "$_");
        } else {
            b.append("name", "primaryKey");
        }
        if (unique) {
            b.appendBool("unique", true);
        }
        if (clustering) {
            b.appendBool("clustering", true);
        }

        BSONElement e;
        e = _options["readPageSize"];
        if (e.ok() && !e.isNull()) {
            b.append(e);
        }
        e = _options["pageSize"];
        if (e.ok() && !e.isNull()) {
            b.append(e);
        }
        e = _options["compression"];
        if (e.ok() && !e.isNull()) {
            b.append(e);
        }
        return b.obj();
    }

    bool isSystemCatalog(const StringData &ns) {
        StringData coll = nsToCollectionSubstring(ns);
        return coll == "system.indexes" || coll == "system.namespaces";
    }
    bool isProfileCollection(const StringData &ns) {
        StringData coll = nsToCollectionSubstring(ns);
        return coll == "system.profile";
    }
    bool isOplogCollection(const StringData &ns) {
        return ns == rsoplog;
    }
    bool isSystemUsersCollection(const StringData &ns) {
        StringData coll = nsToCollectionSubstring(ns);
        return coll == "system.users";
    }

    // Construct a brand new NamespaceDetails with a certain primary key and set of options.
    NamespaceDetails::NamespaceDetails(const StringData &ns, const BSONObj &pkIndexPattern, const BSONObj &options) :
        _ns(ns.toString()),
        _options(options.copy()),
        _pk(pkIndexPattern.copy()),
        _fastupdatesOkState(AtomicWord<int>(-1)),
        _indexBuildInProgress(false),
        _nIndexes(0),
        _multiKeyIndexBits(0),
        _qcWriteCount(0) {

        massert( 10356 ,  str::stream() << "invalid ns: " << ns , NamespaceString::validCollectionName(ns));

        TOKULOG(1) << "Creating NamespaceDetails " << ns << endl;

        // Create the primary key index, generating the info from the pk pattern and options.
        BSONObj info = indexInfo(pkIndexPattern, true, true);
        createIndex(info);

        try {
            // If this throws, it's safe to call close() because we just created the index.
            // Therefore we have a write lock, and nobody else could have any uncommitted
            // modifications to this index, so close() should succeed, and #29 is irrelevant.
            addToNamespacesCatalog(ns, !options.isEmpty() ? &options : NULL);
        }
        catch (...) {
            close();
            throw;
        }
        computeIndexKeys();
    }
    shared_ptr<NamespaceDetails> NamespaceDetails::make(const StringData &ns, const BSONObj &options) {
        if (isOplogCollection(ns)) {
            return shared_ptr<NamespaceDetails>(new OplogCollection(ns, options));
        } else if (isSystemCatalog(ns)) {
            return shared_ptr<NamespaceDetails>(new SystemCatalogCollection(ns, options));
        } else if (isSystemUsersCollection(ns)) {
            Client::CreatingSystemUsersScope scope;
            return shared_ptr<NamespaceDetails>(new SystemUsersCollection(ns, options));
        } else if (isProfileCollection(ns)) {
            // TokuMX doesn't _necessarily_ need the profile to be capped, but vanilla does.
            // We enforce the restriction because it's easier to implement. See SERVER-6937.
            uassert( 16852, "System profile must be a capped collection.", options["capped"].trueValue() );
            return shared_ptr<NamespaceDetails>(new ProfileCollection(ns, options));
        } else if (options["capped"].trueValue()) {
            return shared_ptr<NamespaceDetails>(new CappedCollection(ns, options));
        } else if (options["natural"].trueValue()) {
            return shared_ptr<NamespaceDetails>(new NaturalOrderCollection(ns, options));
        } else {
            return shared_ptr<NamespaceDetails>(new IndexedCollection(ns, options));
        }
    }

    // Construct an existing NamespaceDetails given its serialized from (generated via serialize()).
    NamespaceDetails::NamespaceDetails(const BSONObj &serialized) :
        _ns(serialized["ns"].String()),
        _options(serialized["options"].Obj().copy()),
        _pk(serialized["pk"].Obj().copy()),
        _fastupdatesOkState(AtomicWord<int>(-1)),
        _indexBuildInProgress(false),
        _nIndexes(serialized["indexes"].Array().size()),
        _multiKeyIndexBits(static_cast<uint64_t>(serialized["multiKeyIndexBits"].Long())),
        _qcWriteCount(0) {

        bool reserialize = false;
        std::vector<BSONElement> index_array = serialized["indexes"].Array();
        for (std::vector<BSONElement>::iterator it = index_array.begin(); it != index_array.end(); it++) {
            const BSONObj &info = it->Obj();
            shared_ptr<IndexDetails> idx(IndexDetails::make(info, false));
            if (!idx && cc().upgradingSystemUsers() && isSystemUsersCollection(_ns) &&
                oldSystemUsersKeyPattern == info["key"].Obj()) {
                // This was already dropped, but because of #673 we held on to the info.
                // To fix it, just drop the index info on the floor.
                LOG(0) << "Incomplete upgrade of " << _ns << " indexes detected.  Repairing." << endl;
                reserialize = true;
                size_t idxNum = it - index_array.begin();
                // Removes the nth bit, and shifts any bits higher than it down a slot.
                _multiKeyIndexBits = ((_multiKeyIndexBits & ((1ULL << idxNum) - 1)) |
                                      ((_multiKeyIndexBits >> (idxNum + 1)) << idxNum));
                _nIndexes--;
                continue;
            }
            _indexes.push_back(idx);
        }
        if (reserialize) {
            // Write a clean version of this collection's info to the namespace index, now that we've rectified it.
            nsindex(_ns)->update_ns(_ns, serialize(), true);
        }
        computeIndexKeys();
    }
    shared_ptr<NamespaceDetails> NamespaceDetails::make(const BSONObj &serialized, const bool bulkLoad) {
        const StringData ns = serialized["ns"].Stringdata();
        if (isOplogCollection(ns)) {
            // We may bulk load the oplog since it's an IndexedCollection
            return bulkLoad ? shared_ptr<NamespaceDetails>(new BulkLoadedCollection(serialized)) :
                              shared_ptr<NamespaceDetails>(new OplogCollection(serialized));
        } else if (isSystemCatalog(ns)) {
            massert( 16869, "bug: Should not bulk load a system catalog collection", !bulkLoad );
            return shared_ptr<NamespaceDetails>(new SystemCatalogCollection(serialized));
        } else if (isSystemUsersCollection(ns)) {
            massert( 17002, "bug: Should not bulk load the users collection", !bulkLoad );
            Client::CreatingSystemUsersScope scope;
            return shared_ptr<NamespaceDetails>(new SystemUsersCollection(serialized));
        } else if (isProfileCollection(ns)) {
            massert( 16870, "bug: Should not bulk load the profile collection", !bulkLoad );
            return shared_ptr<NamespaceDetails>(new ProfileCollection(serialized));
        } else if (serialized["options"]["capped"].trueValue()) {
            massert( 16871, "bug: Should not bulk load capped collections", !bulkLoad );
            return shared_ptr<NamespaceDetails>(new CappedCollection(serialized));
        } else if (serialized["options"]["natural"].trueValue()) {
            massert( 16872, "bug: Should not bulk load natural order collections. ", !bulkLoad );
            return shared_ptr<NamespaceDetails>(new NaturalOrderCollection(serialized));
        } else {
            // We only know how to bulk load indexed collections.
            return bulkLoad ? shared_ptr<NamespaceDetails>(new BulkLoadedCollection(serialized)) :
                              shared_ptr<NamespaceDetails>(new IndexedCollection(serialized));
        }
    }

    void NamespaceDetails::close(const bool aborting) {
        if (!aborting) {
            verify(!_indexBuildInProgress);
        }
        for (int i = 0; i < nIndexesBeingBuilt(); i++) {
            IndexDetails &idx = *_indexes[i];
            idx.close();
        }
    }

    // Serialize the information necessary to re-open this NamespaceDetails later.
    BSONObj NamespaceDetails::serialize(const StringData& ns, const BSONObj &options, const BSONObj &pk,
            unsigned long long multiKeyIndexBits, const BSONArray &indexes_array) {
        return BSON("ns" << ns <<
                    "options" << options <<
                    "pk" << pk <<
                    "multiKeyIndexBits" << static_cast<long long>(multiKeyIndexBits) <<
                    "indexes" << indexes_array);
    }
    BSONObj NamespaceDetails::serialize(const bool includeHotIndex) const {
        BSONArrayBuilder indexes_array;
        // Serialize all indexes that exist, including a hot index if it exists.
        for (int i = 0; i < (includeHotIndex ? nIndexesBeingBuilt() : nIndexes()); i++) {
            IndexDetails &idx = *_indexes[i];
            indexes_array.append(idx.info());
        }
        return serialize(_ns, _options, _pk, _multiKeyIndexBits, indexes_array.arr());
    }

    bool NamespaceDetails::fastupdatesOk() {
        const int state = _fastupdatesOkState.loadRelaxed();
        if (state == -1) {
            // need to determine if fastupdates are ok. any number of threads
            // can race to do this - thats fine, they'll all get the same result.
            bool ok = true;
            if (shardingState.needShardChunkManager(_ns)) {
                ShardChunkManagerPtr chunkManager = shardingState.getShardChunkManager(_ns);
                ok = chunkManager == NULL || chunkManager->hasShardKey(_pk);
            }
            _fastupdatesOkState.swap(ok ? 1 : 0);
            return ok;
        } else {
            // result already computed, fastupdates are ok if state is > 0
            dassert(state >= 0);
            return state > 0;
        }
    }

    BSONObj NamespaceDetails::getSimplePKFromQuery(const BSONObj &query) const {
        const int numPKFields = _pk.nFields();
        BSONElement pkElements[numPKFields];
        int numPKElementsFound = 0;
        for (BSONObjIterator queryIterator(query); queryIterator.more(); ) {
            const BSONElement &q = queryIterator.next();
            if (!q.isSimpleType() ||
                (q.type() == Object && q.Obj().firstElementFieldName()[0] == '$')) {
                continue; // not a 'simple' query element
            }
            BSONObjIterator pkIterator(_pk);
            for (int i = 0; i < numPKFields; i++) {
                const BSONElement &p = pkIterator.next();
                if (pkElements[i].ok()) {
                    continue; // already set
                } else if (str::equals(q.fieldName(), p.fieldName())) {
                    pkElements[i] = q;
                    numPKElementsFound++;
                }
            }
        }
        if (numPKElementsFound == numPKFields) {
            // We found a simple element in the query for each part of the pk.
            BSONObjBuilder b;
            for (int i = 0; i < numPKFields; i++) {
                b.appendAs(pkElements[i], "");
            }
            return b.obj();
        }
        return BSONObj();
    }

    BSONObj NamespaceDetails::getValidatedPKFromObject(const BSONObj &obj) const {
        BSONObjSet keys;
        getPKIndex().getKeysFromObject(obj, keys);
        uassert(17205, str::stream() << "primary key " << _pk << " cannot be multi-key",
                       keys.size() == 1); // this enforces no arrays in the primary key
        const BSONObj pk = keys.begin()->getOwned();
        for (BSONObjIterator i(pk); i.more(); ) {
            const BSONElement e = i.next();
            uassert(17208, "can't use a regex for any portion of the primary key",
                           e.type() != RegEx);
            uassert(17210, "can't use undefined for any portion of the primary key",
                           e.type() != Undefined);
        }
        return pk;
    }

    void NamespaceDetails::computeIndexKeys() {
        _indexedPaths.clear();

        for (int i = 0; i < nIndexesBeingBuilt(); i++) {
            const BSONObj &key = _indexes[i]->keyPattern();
            BSONObjIterator o( key );
            while ( o.more() ) {
                const BSONElement e = o.next();
                _indexedPaths.addPath( e.fieldName() );
            }
        }
    }

    void NamespaceDetails::resetTransient() {
        Lock::assertWriteLocked(_ns); 
        clearQueryCache();
        computeIndexKeys();
    }

    void NamespaceDetails::clearQueryCache() {
        QueryCacheRWLock::Exclusive lk(this);
        _qcCache.clear();
        _qcWriteCount = 0;
    }

    void NamespaceDetails::notifyOfWriteOp() {
        if ( _qcCache.empty() ) {
            return;
        }
        if ( ++_qcWriteCount >= 100 ) {
            clearQueryCache();
        }
    }

    CachedQueryPlan NamespaceDetails::cachedQueryPlanForPattern( const QueryPattern &pattern ) {
        map<QueryPattern, CachedQueryPlan>::const_iterator i = _qcCache.find(pattern);
        return i != _qcCache.end() ? i->second : CachedQueryPlan();
    }

    void NamespaceDetails::registerCachedQueryPlanForPattern( const QueryPattern &pattern,
                                            const CachedQueryPlan &cachedQueryPlan ) {
        _qcCache[ pattern ] = cachedQueryPlan;
    }

    int NamespaceDetails::findByPKCallback(const DBT *key, const DBT *value, void *extra) {
        struct findByPKCallbackExtra *info = reinterpret_cast<findByPKCallbackExtra *>(extra);
        try {
            if (key != NULL) {
                struct findByPKCallbackExtra *info = reinterpret_cast<findByPKCallbackExtra *>(extra);
                info->obj = BSONObj(reinterpret_cast<char *>(value->data)).getOwned();
            }
            return 0;
        } catch (std::exception &e) {
            info->ex = &e;
        }
        return -1;
    }

    bool NamespaceDetails::findOne(const BSONObj &query, BSONObj &result, const bool requireIndex) const {
        for (shared_ptr<Cursor> c( getOptimizedCursor(_ns, query, BSONObj(),
                                       requireIndex ? QueryPlanSelectionPolicy::indexOnly() :
                                                      QueryPlanSelectionPolicy::any() ) );
             c->ok(); c->advance()) {
            if ( c->currentMatches() && !c->getsetdup( c->currPK() ) ) {
                result = c->current().copy();
                return true;
            }
        }
        return false;
    }

    bool NamespaceDetails::findByPK(const BSONObj &key, BSONObj &result) const {
        TOKULOG(3) << "NamespaceDetails::findByPK looking for " << key << endl;

        storage::Key sKey(key, NULL);
        DBT key_dbt = sKey.dbt();
        DB *db = getPKIndex().db();

        BSONObj obj;
        struct findByPKCallbackExtra extra(obj);
        const int flags = cc().opSettings().getQueryCursorMode() != DEFAULT_LOCK_CURSOR ?
                          DB_SERIALIZABLE | DB_RMW : 0;
        const int r = db->getf_set(db, cc().txn().db_txn(), flags, &key_dbt,
                                   findByPKCallback, &extra);
        if (extra.ex != NULL) {
            throw *extra.ex;
        }
        if (r != 0 && r != DB_NOTFOUND) {
            storage::handle_ydb_error(r);
        }

        if (!obj.isEmpty()) {
            result = obj;
            return true;
        }
        return false;
    }

    void NamespaceDetails::insertIntoIndexes(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        dassert(!pk.isEmpty());
        dassert(!obj.isEmpty());

        if (isSystemUsersCollection(_ns)) {
            uassertStatusOK(AuthorizationManager::checkValidPrivilegeDocument(nsToDatabaseSubstring(_ns), obj));
        }

        const int n = nIndexesBeingBuilt();
        DB *dbs[n];
        storage::DBTArrays keyArrays(n);
        storage::DBTArrays valArrays(n);
        uint32_t put_flags[n];

        storage::Key sPK(pk, NULL);
        DBT src_key = storage::dbt_make(sPK.buf(), sPK.size());
        DBT src_val = storage::dbt_make(obj.objdata(), obj.objsize());

        for (int i = 0; i < n; i++) {
            const bool isPK = i == 0;
            const bool prelocked = flags & NO_LOCKTREE;
            const bool doUniqueChecks = !(flags & NO_UNIQUE_CHECKS) &&
                                        !(isPK && (flags & NO_PK_UNIQUE_CHECKS));

            IndexDetails &idx = *_indexes[i];
            dbs[i] = idx.db();

            // Primary key uniqueness check will be done at the ydb layer.
            // Secondary key uniqueness checks are done below, if necessary.
            put_flags[i] = (isPK && doUniqueChecks ? DB_NOOVERWRITE : 0) |
                           (prelocked ? DB_PRELOCKED_WRITE : 0);

            // It is not our responsibility to set the multikey bits
            // for a hot index. Further, a hot index cannot be unique,
            if (i >= _nIndexes) {
                continue;
            }

            if (!isPK) {
                BSONObjSet idxKeys;
                idx.getKeysFromObject(obj, idxKeys);
                if (idx.unique() && doUniqueChecks) {
                    for (BSONObjSet::const_iterator o = idxKeys.begin(); o != idxKeys.end(); ++o) {
                        idx.uniqueCheck(*o, pk);
                    }
                }
                if (idxKeys.size() > 1) {
                    setIndexIsMultikey(i);
                }
                // Store the keys we just generated, so we won't do it twice in
                // the generate keys callback. See storage::generate_keys()
                DBT_ARRAY *array = &keyArrays[i];
                storage::dbt_array_clear_and_resize(array, idxKeys.size());
                for (BSONObjSet::const_iterator it = idxKeys.begin(); it != idxKeys.end(); it++) {
                    const storage::Key sKey(*it, &pk);
                    storage::dbt_array_push(array, sKey.buf(), sKey.size());
                }
            }
        }

        DB_ENV *env = storage::env;
        const int r = env->put_multiple(env, dbs[0], cc().txn().db_txn(),
                                        &src_key, &src_val,
                                        n, dbs, keyArrays.arrays(), valArrays.arrays(), put_flags);
        if (r == EINVAL) {
            uasserted( 16900, str::stream() << "Indexed insertion failed." <<
                              " This may be due to keys > 32kb. Check the error log." );
        } else if (r != 0) {
            storage::handle_ydb_error(r);
        }

        // Index usage accounting. If a key was generated for this 
        // operation, then the index was used, otherwise it wasn't.
        // The PK is always used, only secondarys may have keys generated.
        getPKIndex().noteInsert();
        for (int i = 0; i < n; i++) {
            const DBT_ARRAY *array = &keyArrays[i];
            if (array->size > 0) {
                IndexDetails &idx = *_indexes[i];
                dassert(!isPKIndex(idx));
                idx.noteInsert();
            }
        }
    }

    void NamespaceDetails::deleteFromIndexes(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        dassert(!pk.isEmpty());
        dassert(!obj.isEmpty());

        const int n = nIndexesBeingBuilt();
        DB *dbs[n];
        storage::DBTArrays keyArrays(n);
        uint32_t del_flags[n];

        storage::Key sPK(pk, NULL);
        DBT src_key = storage::dbt_make(sPK.buf(), sPK.size());
        DBT src_val = storage::dbt_make(obj.objdata(), obj.objsize());

        for (int i = 0; i < n; i++) {
            const bool isPK = i == 0;
            const bool prelocked = flags & NamespaceDetails::NO_LOCKTREE;
            IndexDetails &idx = *_indexes[i];
            dbs[i] = idx.db();
            del_flags[i] = DB_DELETE_ANY | (prelocked ? DB_PRELOCKED_WRITE : 0);
            DEV {
                // for debug builds, remove the DB_DELETE_ANY flag
                // so that debug builds do a query to make sure the
                // row is there. It is a nice check to ensure correctness
                // on debug builds.
                del_flags[i] &= ~DB_DELETE_ANY;
            }
            if (!isPK) {
                BSONObjSet idxKeys;
                idx.getKeysFromObject(obj, idxKeys);

                if (idxKeys.size() > 1) {
                    verify(isMultikey(i));
                }

                // Store the keys we just generated, so we won't do it twice in
                // the generate keys callback. See storage::generate_keys()
                DBT_ARRAY *array = &keyArrays[i];
                storage::dbt_array_clear_and_resize(array, idxKeys.size());
                for (BSONObjSet::const_iterator it = idxKeys.begin(); it != idxKeys.end(); it++) {
                    const storage::Key sKey(*it, &pk);
                    storage::dbt_array_push(array, sKey.buf(), sKey.size());
                }
            }
        }

        DB_ENV *env = storage::env;
        const int r = env->del_multiple(env, dbs[0], cc().txn().db_txn(),
                                        &src_key, &src_val,
                                        n, dbs, keyArrays.arrays(), del_flags);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }

        // Index usage accounting. If a key was generated for this 
        // operation, then the index was used, otherwise it wasn't.
        // The PK is always used, only secondarys may have keys generated.
        getPKIndex().noteDelete();
        for (int i = 0; i < n; i++) {
            const DBT_ARRAY *array = &keyArrays[i];
            if (array->size > 0) {
                IndexDetails &idx = *_indexes[i];
                dassert(!isPKIndex(idx));
                idx.noteDelete();
            }
        }
    }

    // uasserts on duplicate key
    static bool orderedSetContains(const BSONObjSet &set, const BSONObj &obj) {
        bool contains = false;
        for (BSONObjSet::iterator i = set.begin(); i != set.end(); i++) {
            const int c = i->woCompare(obj);
            if (c >= 0) {
                contains = c == 0;
                break;
            }
        }
        return contains;
    }

    // deletes an object from this namespace, taking care of secondary indexes if they exist
    void NamespaceDetails::deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        deleteFromIndexes(pk, obj, flags);
    }

    void NamespaceDetails::updateObject(const BSONObj &pk, const BSONObj &oldObj, const BSONObj &newObj,
                                        const bool logop, const bool fromMigrate, uint64_t flags) {
        TOKULOG(4) << "NamespaceDetails::updateObject pk "
            << pk << ", old " << oldObj << ", new " << newObj << endl;

        dassert(!pk.isEmpty());
        dassert(!oldObj.isEmpty());
        dassert(!newObj.isEmpty());

        if (isSystemUsersCollection(_ns)) {
            uassertStatusOK(AuthorizationManager::checkValidPrivilegeDocument(nsToDatabaseSubstring(_ns), newObj));
        }

        const int n = nIndexesBeingBuilt();
        DB *dbs[n];
        storage::DBTArrays keyArrays(n * 2);
        storage::DBTArrays valArrays(n);
        uint32_t update_flags[n];

        storage::Key sPK(pk, NULL);
        DBT src_key = storage::dbt_make(sPK.buf(), sPK.size());
        DBT new_src_val = storage::dbt_make(newObj.objdata(), newObj.objsize());
        DBT old_src_val = storage::dbt_make(oldObj.objdata(), oldObj.objsize());

        // Generate keys for each index, prepare data structures for del multiple.
        // We will end up abandoning del multiple if there are any multikey indexes.
        for (int i = 0; i < n; i++) {
            const bool isPK = i == 0;
            const bool prelocked = flags & NamespaceDetails::NO_LOCKTREE;
            const bool doUniqueChecks = !(flags & NO_UNIQUE_CHECKS) &&
                                        !(isPK && (flags & NO_PK_UNIQUE_CHECKS));

            IndexDetails &idx = *_indexes[i];
            dbs[i] = idx.db();
            update_flags[i] = prelocked ? DB_PRELOCKED_WRITE : 0;

            // It is not our responsibility to set the multikey bits
            // for a hot index. Further, a hot index cannot be unique,
            if (i >= _nIndexes) {
                continue;
            }

            // We only need to generate keys etc for secondary indexes when:
            // - The keys may have changed, which is possible if the keys unaffected
            //   hint was not given.
            // - The index is clustering. It doesn't matter if keys have changed because
            //   we need to update the clustering document.
            const bool keysMayHaveChanged = !(flags & NamespaceDetails::KEYS_UNAFFECTED_HINT);
            if (!isPK && (keysMayHaveChanged || idx.clustering())) {
                BSONObjSet oldIdxKeys;
                BSONObjSet newIdxKeys;
                idx.getKeysFromObject(oldObj, oldIdxKeys);
                idx.getKeysFromObject(newObj, newIdxKeys);
                if (idx.unique() && doUniqueChecks && keysMayHaveChanged) {
                    // Only perform the unique check for those keys that actually changed.
                    for (BSONObjSet::iterator o = newIdxKeys.begin(); o != newIdxKeys.end(); ++o) {
                        const BSONObj &k = *o;
                        if (!orderedSetContains(oldIdxKeys, k)) {
                            idx.uniqueCheck(k, pk);
                        }
                    }
                }
                if (newIdxKeys.size() > 1) {
                    setIndexIsMultikey(i);
                }

                // Store the keys we just generated, so we won't do it twice in
                // the generate keys callback. See storage::generate_keys()
                DBT_ARRAY *array = &keyArrays[i];
                storage::dbt_array_clear_and_resize(array, newIdxKeys.size());
                for (BSONObjSet::const_iterator it = newIdxKeys.begin(); it != newIdxKeys.end(); it++) {
                    const storage::Key sKey(*it, &pk);
                    storage::dbt_array_push(array, sKey.buf(), sKey.size());
                }
                array = &keyArrays[i + n];
                storage::dbt_array_clear_and_resize(array, oldIdxKeys.size());
                for (BSONObjSet::const_iterator it = oldIdxKeys.begin(); it != oldIdxKeys.end(); it++) {
                    const storage::Key sKey(*it, &pk);
                    storage::dbt_array_push(array, sKey.buf(), sKey.size());
                }
            }
        }

        // The pk doesn't change, so old_src_key == new_src_key.
        DB_ENV *env = storage::env;
        const int r = env->update_multiple(env, dbs[0], cc().txn().db_txn(),
                                           &src_key, &old_src_val,
                                           &src_key, &new_src_val,
                                           n, dbs, update_flags,
                                           n * 2, keyArrays.arrays(), n, valArrays.arrays());
        if (r == EINVAL) {
            uasserted( 16908, str::stream() << "Indexed insertion (on update) failed." <<
                              " This may be due to keys > 32kb. Check the error log." );
        } else if (r != 0) {
            storage::handle_ydb_error(r);
        }

        if (logop) {
            OpLogHelpers::logUpdate(_ns.c_str(), pk, oldObj, newObj, fromMigrate);
        }
    }

    void NamespaceDetails::updateObjectMods(const BSONObj &pk, const BSONObj &updateObj,
                                            const bool logop, const bool fromMigrate,
                                            uint64_t flags) {
        IndexDetails &pkIdx = getPKIndex();
        pkIdx.updatePair(pk, NULL, updateObj, flags);

        if (logop) {
            OpLogHelpers::logUpdateMods(_ns.c_str(), pk, updateObj, fromMigrate);
        }
    }

    void NamespaceDetails::setIndexIsMultikey(const int idxNum) {
        // Under no circumstasnces should the primary key become multikey.
        verify(idxNum > 0);
        dassert(idxNum < NIndexesMax);
        const unsigned long long x = ((unsigned long long) 1) << idxNum;
        if (_multiKeyIndexBits & x) {
            return;
        }
        if (!Lock::isWriteLocked(_ns)) {
            throw RetryWithWriteLock();
        }

        _multiKeyIndexBits |= x;
        nsindex(_ns)->update_ns(_ns, serialize(), true);
        resetTransient();
    }

    void NamespaceDetails::checkIndexUniqueness(const IndexDetails &idx) {
        IndexScanCursor c(this, idx, 1);
        BSONObj prevKey = c.currKey().getOwned();
        c.advance();
        for ( ; c.ok(); c.advance() ) {
            BSONObj currKey = c.currKey(); 
            if (currKey == prevKey) {
                idx.uassertedDupKey(currKey);
            }
            prevKey = currKey.getOwned();
        }
    }

    // Wrapper for offline (write locked) indexing.
    void NamespaceDetails::createIndex(const BSONObj &info) {
        const string sourceNS = info["ns"].String();

        if (!Lock::isWriteLocked(_ns)) {
            throw RetryWithWriteLock();
        }

        ColdIndexer indexer(this, info);
        indexer.prepare();
        indexer.build();
        indexer.commit();
    }

    void NamespaceDetails::dropIndex(const int idxNum) {
        verify(!_indexBuildInProgress);
        verify(idxNum < (int) _indexes.size());

        IndexDetails &idx = *_indexes[idxNum];

        // Note this ns in the rollback so if this transaction aborts, we'll
        // close this ns, forcing the next user to reload in-memory metadata.
        NamespaceIndexRollback &rollback = cc().txn().nsIndexRollback();
        rollback.noteNs(_ns);

        // Remove this index from the system catalogs
        removeFromNamespacesCatalog(idx.indexNamespace());
        if (nsToCollectionSubstring(_ns) != "system.indexes") {
            removeFromIndexesCatalog(_ns, idx.indexName());
        }

        idx.kill_idx();
        _indexes.erase(_indexes.begin() + idxNum);
        _nIndexes--;
        // Removes the nth bit, and shifts any bits higher than it down a slot.
        _multiKeyIndexBits = ((_multiKeyIndexBits & ((1ULL << idxNum) - 1)) |
                             ((_multiKeyIndexBits >> (idxNum + 1)) << idxNum));
        resetTransient();
        // Updated whatever in memory structures are necessary, now update the nsindex.
        nsindex(_ns)->update_ns(_ns, serialize(), true);
    }

    // Normally, we cannot drop the _id_ index.
    // The parameters mayDeleteIdIndex is here for the case where we call dropIndexes
    // through dropCollection, in which case we are dropping an entire collection,
    // hence the _id_ index will have to go.
    bool NamespaceDetails::dropIndexes(const StringData& name, string &errmsg, BSONObjBuilder &result, bool mayDeleteIdIndex) {
        Lock::assertWriteLocked(_ns);
        TOKULOG(1) << "dropIndexes " << name << endl;

        uassert( 16904, "Cannot drop indexes: a hot index build in progress.",
                        !_indexBuildInProgress );

        ClientCursor::invalidate(_ns);
        const int idxNum = findIndexByName(name);
        if (name == "*") {
            result.append("nIndexesWas", (double) _nIndexes);
            for (int i = 0; i < _nIndexes; ) {
                IndexDetails &idx = *_indexes[i];
                if (mayDeleteIdIndex || (!idx.isIdIndex() && !isPKIndex(idx))) {
                    dropIndex(i);
                } else {
                    i++;
                }
            }
            // Assuming id/pk index isn't multikey
            verify(_multiKeyIndexBits == 0);
            result.append("msg", (mayDeleteIdIndex
                                  ? "indexes dropped for collection"
                                  : "non-_id indexes dropped for collection"));
        } else {
            if (idxNum >= 0) {
                result.append("nIndexesWas", (double) _nIndexes);
                IndexVector::iterator it = _indexes.begin() + idxNum;
                IndexDetails *idx = it->get();
                if ( !mayDeleteIdIndex && (idx->isIdIndex() || isPKIndex(*idx)) ) {
                    errmsg = "may not delete _id or $_ index";
                    return false;
                }
                dropIndex(idxNum);
            } else {
                log() << "dropIndexes: " << name << " not found" << endl;
                errmsg = "index not found";
                return false;
            }
        }

        return true;
    }

    void NamespaceDetails::drop(string &errmsg, BSONObjBuilder &result, const bool mayDropSystem) {
        // Check that we are allowed to drop the namespace.
        StringData database = nsToDatabaseSubstring(_ns);
        verify(database == cc().database()->name());
        if (NamespaceString::isSystem(_ns) && !mayDropSystem) {
            if (nsToCollectionSubstring(_ns) == "system.profile") {
                uassert(10087, "turn off profiling before dropping system.profile collection", cc().database()->profile() == 0);
            } else {
                uasserted(12502, "can't drop system ns");
            }
        }

        // Invalidate cursors, then drop all of the indexes.
        ClientCursor::invalidate(_ns);
        dropIndexes("*", errmsg, result, true);
        verify(_nIndexes == 0);
        removeFromNamespacesCatalog(_ns);

        Top::global.collectionDropped(_ns);
        result.append("ns", _ns);

        // Kill the ns from the nsindex.
        //
        // Will delete "this" NamespaceDetails object, since it's lifetime is managed
        // by a shared pointer in the map we're going to delete from.
        nsindex(_ns)->kill_ns(_ns);
    }

    void NamespaceDetails::optimizeAll() {
        for (int i = 0; i < _nIndexes; i++) {
            IndexDetails &idx = *_indexes[i];
            const bool ascending = Ordering::make(idx.keyPattern()).descending(0);
            const bool isPK = isPKIndex(idx);

            storage::Key leftSKey(ascending ? minKey : maxKey,
                                  isPK ? NULL : &minKey);
            storage::Key rightSKey(ascending ? maxKey : minKey,
                                   isPK ? NULL : &maxKey);
            uint64_t loops_run;
            idx.optimize(rightSKey, leftSKey, true, 0, &loops_run);
        }
    }

    void NamespaceDetails::fillCollectionStats(
        Stats &aggStats,
        BSONObjBuilder *result,
        int scale) const
    {
        Stats stats;
        stats.nIndexes += nIndexes();
        // also sum up some stats of secondary indexes,
        // calculate their total data size and storage size
        BSONArrayBuilder ab;
        for (int i = 0; i < nIndexes(); i++) {
            IndexDetails &idx = *_indexes[i];
            IndexDetails::Stats idxStats = idx.getStats();
            BSONObjBuilder infoBuilder(ab.subobjStart());
            idxStats.appendInfo(infoBuilder, scale);
            infoBuilder.done();
            if (isPKIndex(idx)) {
                stats.count += idxStats.count;
                stats.size += idxStats.dataSize;
                stats.storageSize += idxStats.storageSize;
            } else {
                // Only count secondary indexes here
                stats.indexSize += idxStats.dataSize;
                stats.indexStorageSize += idxStats.storageSize;
            }
        }

        if (result != NULL) {
            // unfortunately, this protocol's format is a little unorthodox
            // TODO: unify this with appendInfo, if we can somehow unify the interface
            result->appendNumber("count", (long long) stats.count);
            result->appendNumber("nindexes", nIndexes());
            result->appendNumber("nindexesbeingbuilt", nIndexesBeingBuilt());
            result->appendNumber("size", (long long) stats.size/scale);
            result->appendNumber("storageSize", (long long) stats.storageSize/scale);
            result->appendNumber("totalIndexSize", (long long) stats.indexSize/scale);
            result->appendNumber("totalIndexStorageSize", (long long) stats.indexStorageSize/scale);
            result->appendArray("indexDetails", ab.done());

            fillSpecificStats(*result, scale);
        }

        // This must happen last in order to scale the values in the result bson above
        aggStats += stats;
    }

    void NamespaceDetails::Stats::appendInfo(BSONObjBuilder &b, int scale) const {
        b.appendNumber("objects", (long long) count);
        b.appendNumber("avgObjSize", count == 0 ? 0.0 : double(size) / double(count));
        b.appendNumber("dataSize", (long long) size / scale);
        b.appendNumber("storageSize", (long long) storageSize / scale);
        b.appendNumber("indexes", (long long) nIndexes);
        b.appendNumber("indexSize", (long long) indexSize / scale);
        b.appendNumber("indexStorageSize", (long long) indexStorageSize / scale);
    }

    void NamespaceDetails::addDefaultIndexesToCatalog() {
        // Either a single primary key or a hidden primary key + _id index.
        // TODO: this is now incorrect in the case of system.users collections, need to fix it and
        //uncomment it:
        //dassert(_nIndexes == 1 || (_nIndexes == 2 && findIdIndex() == 1));
        for (int i = 0; i < nIndexes(); i++) {
            addToIndexesCatalog(_indexes[i]->info());
        }
    }

    bool NamespaceDetails::ensureIndex(const BSONObj &info) {
        const BSONObj keyPattern = info["key"].Obj();
        const int i = findIndexByKeyPattern(keyPattern);
        if (i >= 0) {
            return false;
        }
        createIndex(info);
        return true;
    }

    /* ------------------------------------------------------------------------- */

    bool userCreateNS(const StringData& ns, BSONObj options, string& err, bool logForReplication) {
        StringData coll = ns.substr(ns.find('.') + 1);
        massert( 16451 ,  str::stream() << "invalid ns: " << ns , NamespaceString::validCollectionName(ns));
        StringData cl = nsToDatabaseSubstring( ns );
        if (nsdetails(ns) != NULL) {
            // Namespace already exists
            err = "collection already exists";
            return false;
        }

        if ( cmdLine.configsvr &&
             !( ns.startsWith( "config." ) ||
                ns.startsWith( "local." ) ||
                ns.startsWith( "admin." ) ) ) {
            uasserted(14037, "can't create user databases on a --configsvr instance");
        }

        {
            BSONElement e = options.getField("size");
            if (e.isNumber()) {
                long long size = e.numberLong();
                uassert(10083, "create collection invalid size spec", size >= 0);
            }
        }

        // This creates the namespace as well as its _id index
        nsdetails_maybe_create(ns, options);
        if ( logForReplication ) {
            if ( options.getField( "create" ).eoo() ) {
                BSONObjBuilder b;
                b << "create" << coll;
                b.appendElements( options );
                options = b.obj();
            }
            string logNs = cl.toString() + ".$cmd";
            OpLogHelpers::logCommand(logNs.c_str(), options);
        }
        return true;
    }

    NamespaceDetails *getAndMaybeCreateNS(const StringData& ns, bool logop) {
        NamespaceDetails* details = nsdetails(ns);
        if (details == NULL) {
            string err;
            BSONObj options;
            bool created = userCreateNS(ns, options, err, logop);
            uassert(16745, "failed to create collection", created);
            details = nsdetails(ns);
            uassert(16746, "failed to get collection after creating", details);
        }
        return details;
    }

    /* add a new namespace to the system catalog (<dbname>.system.namespaces).
       options: { capped : ..., size : ... }
    */
    void addToNamespacesCatalog(const StringData& ns, const BSONObj *options) {
        LOG(1) << "New namespace: " << ns << endl;
        StringData coll = nsToCollectionSubstring(ns);
        if (coll.startsWith("system.namespaces")) {
            // system.namespaces holds all the others, so it is not explicitly listed in the catalog.
            return;
        }

        BSONObjBuilder b;
        b.append("name", ns);
        if ( options ) {
            b.append("options", *options);
        }
        BSONObj info = b.done();

        string system_ns = getSisterNS(ns, "system.namespaces");
        NamespaceDetails *d = nsdetails_maybe_create(system_ns);
        insertOneObject(d, info);
    }

    void addToIndexesCatalog(const BSONObj &info) {
        const StringData &indexns = info["ns"].Stringdata();
        if (nsToCollectionSubstring(indexns).startsWith("system.indexes")) {
            // system.indexes holds all the others, so it is not explicitly listed in the catalog.
            return;
        }

        string ns = getSisterNS(indexns, "system.indexes");
        NamespaceDetails *d = nsdetails_maybe_create(ns);
        BSONObj objMod = info;
        insertOneObject(d, objMod);
    }

    static void removeFromNamespacesCatalog(const StringData &ns) {
        StringData coll = nsToCollectionSubstring(ns);
        if (!coll.startsWith("system.namespaces")) {
            string system_namespaces = getSisterNS(cc().database()->name(), "system.namespaces");
            _deleteObjects(system_namespaces.c_str(),
                           BSON("name" << ns), false, false);
        }
    }

    static void removeFromIndexesCatalog(const StringData &ns, const StringData &name) {
        string system_indexes = getSisterNS(cc().database()->name(), "system.indexes");
        BSONObj obj = BSON("ns" << ns << "name" << name);
        TOKULOG(2) << "removeFromIndexesCatalog removing " << obj << endl;
        const int n = _deleteObjects(system_indexes.c_str(), obj, false, false);
        verify(n == 1);
    }

    static BSONObj replaceNSField(const BSONObj &obj, const StringData &to) {
        BSONObjBuilder b;
        BSONObjIterator i( obj );
        while ( i.more() ) {
            BSONElement e = i.next();
            if ( strcmp( e.fieldName(), "ns" ) != 0 ) {
                b.append( e );
            } else {
                b << "ns" << to;
            }
        }
        return b.obj();
    }

    void renameNamespace(const StringData& from, const StringData& to) {
        Lock::assertWriteLocked(from);

        NamespaceDetails *from_details = nsdetails(from);
        verify( from_details != NULL );
        verify( nsdetails(to) == NULL );

        uassert( 16896, "Cannot rename a collection under-going bulk load.",
                        from != cc().bulkLoadNS() );
        uassert( 16918, "Cannot rename a collection with a background index build in progress",
                        !from_details->indexBuildInProgress() );

        // Kill open cursors before we close and rename the namespace
        ClientCursor::invalidate( from );

        string sysIndexes = getSisterNS(from, "system.indexes");
        string sysNamespaces = getSisterNS(from, "system.namespaces");

        // Generate the serialized form of the namespace, and then close it.
        // This will close the underlying dictionaries and allow us to
        // rename them in the environment.
        BSONObj serialized = from_details->serialize();
        bool closed = nsindex(from)->close_ns(from);
        verify(closed);

        // Rename each index in system.indexes and system.namespaces
        {
            BSONObj nsQuery = BSON( "ns" << from );
            vector<BSONObj> indexSpecs;
            {
                // Find all entries in sysIndexes for the from ns
                Client::Context ctx( sysIndexes );
                for (shared_ptr<Cursor> c( getOptimizedCursor( sysIndexes, nsQuery ) );
                     c->ok(); c->advance()) {
                    if (c->currentMatches()) {
                        indexSpecs.push_back( c->current().copy() );
                    }
                }
            }
            for ( vector<BSONObj>::const_iterator it = indexSpecs.begin() ; it != indexSpecs.end(); it++) {
                BSONObj oldIndexSpec = *it;
                string idxName = oldIndexSpec["name"].String();
                string oldIdxNS = IndexDetails::indexNamespace(from, idxName);
                string newIdxNS = IndexDetails::indexNamespace(to, idxName);

                TOKULOG(1) << "renaming " << oldIdxNS << " to " << newIdxNS << endl;
                storage::db_rename(oldIdxNS, newIdxNS);

                BSONObj newIndexSpec = replaceNSField( oldIndexSpec, to );
                removeFromIndexesCatalog(from, idxName);
                removeFromNamespacesCatalog(oldIdxNS);
                addToIndexesCatalog(newIndexSpec);
                addToNamespacesCatalog(newIdxNS, newIndexSpec.isEmpty() ? 0 : &newIndexSpec);
            }
        }

        // Rename the namespace in system.namespaces
        BSONObj newSpec;
        {
            BSONObj oldSpec;
            NamespaceDetails *d = nsdetails(sysNamespaces);
            verify( d != NULL && d->findOne( BSON( "name" << from ), oldSpec ) );
            BSONObjBuilder b;
            BSONObjIterator i( oldSpec.getObjectField( "options" ) );
            while ( i.more() ) {
                BSONElement e = i.next();
                if ( strcmp( e.fieldName(), "create" ) != 0 ) {
                    b.append( e );
                }
                else {
                    b << "create" << to;
                }
            }
            newSpec = b.obj();
            removeFromNamespacesCatalog(from);
            addToNamespacesCatalog(to, newSpec.isEmpty() ? 0 : &newSpec);
        }

        // Update the namespace index
        {
            BSONArrayBuilder newIndexesArray;
            vector<BSONElement> indexes = serialized["indexes"].Array();
            for (vector<BSONElement>::const_iterator it = indexes.begin(); it != indexes.end(); it++) {
                newIndexesArray.append( replaceNSField( it->Obj(), to ) );
            }
            BSONObj newSerialized = NamespaceDetails::serialize( to, newSpec, serialized["pk"].Obj(),
                                                                 serialized["multiKeyIndexBits"].Long(),
                                                                 newIndexesArray.arr());
            // Kill the old entry and replace it with the new name and modified spec.
            // The next user of the newly-named namespace will need to open it.
            NamespaceIndex *ni = nsindex( from );
            ni->kill_ns( from );
            ni->update_ns( to, newSerialized, false );
            verify( nsdetails(to) != NULL );
            verify( nsdetails(from) == NULL );
        }
    }

    void beginBulkLoad(const StringData &ns, const vector<BSONObj> &indexes,
                       const BSONObj &options) {
        uassert( 16873, "Cannot bulk load a collection that already exists.",
                        nsdetails(ns) == NULL );
        uassert( 16998, "Cannot bulk load a system collection",
                        !NamespaceString::isSystem(ns) );
        uassert( 16999, "Cannot bulk load a capped collection",
                        !options["capped"].trueValue() );
        uassert( 17000, "Cannot bulk load a natural order collection",
                        !options["natural"].trueValue() );

        // Don't log the create. The begin/commit/abort load commands are already logged.
        string errmsg;
        const bool created = userCreateNS(ns, options, errmsg, false);
        verify(created);

        NamespaceIndex *ni = nsindex(ns);
        NamespaceDetails *d = ni->details(ns);
        for (vector<BSONObj>::const_iterator i = indexes.begin(); i != indexes.end(); i++) {
            BSONObj info = *i;
            const BSONElement &e = info["ns"];
            if (e.ok()) {
                uassert( 16886, "Each index spec's ns field, if provided, must match the loaded ns.",
                                e.type() == mongo::String && e.Stringdata() == ns );
            } else {
                // Add the ns field if it wasn't provided.
                BSONObjBuilder b;
                b.append("ns", ns);
                b.appendElements(info);
                info = b.obj();
            }
            uassert( 16887, "Each index spec must have a string name field.",
                            info["name"].ok() && info["name"].type() == mongo::String );
            if (d->ensureIndex(info)) {
                addToIndexesCatalog(info);
            }
        }

        // Acquire full table locks on each index so that only this
        // transcation can write to them until the load/txn commits.
        for (int i = 0; i < d->nIndexes(); i++) {
            IndexDetails &idx = d->idx(i);
            idx.acquireTableLock();
        }

        // Now the ns exists. Close it and re-open it in "bulk load" mode.
        const bool closed = ni->close_ns(ns);
        verify(closed);
        const bool opened = ni->open_ns(ns, true);
        verify(opened);
    }

    void commitBulkLoad(const StringData &ns) {
        NamespaceIndex *ni = nsindex(ns);
        const bool closed = ni->close_ns(ns);
        verify(closed);
    }

    void abortBulkLoad(const StringData &ns) {
        NamespaceIndex *ni = nsindex(ns);
        // Close the ns with aborting = true, which will hint to the
        // BulkLoadedCollection that it should abort the load.
        const bool closed = ni->close_ns(ns, true);
        verify(closed);
    }

    bool legalClientSystemNS( const StringData& ns , bool write ) {
        if( ns == "local.system.replset" ) return true;

        StringData collstr = nsToCollectionSubstring(ns);
        if ( collstr == "system.users" ) {
            return true;
        }

        if ( collstr == "system.js" ) {
            if ( write )
                Scope::storedFuncMod();
            return true;
        }

        return false;
    }

} // namespace mongo
