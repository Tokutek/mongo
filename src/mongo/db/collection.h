/**
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

#pragma once

#include <db.h>

#include "mongo/pch.h"

#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_details.h"

namespace mongo {

    class IndexedCollection : public NamespaceDetails {
    private:
        BSONObj determinePrimaryKey(const BSONObj &options);

        const bool _idPrimaryKey;

    public:
        IndexedCollection(const StringData &ns, const BSONObj &options);
        IndexedCollection(const BSONObj &serialized);

        // inserts an object into this namespace, taking care of secondary indexes if they exist
        void insertObject(BSONObj &obj, uint64_t flags);

        void updateObject(const BSONObj &pk, const BSONObj &oldObj, const BSONObj &newObj,
                          const bool logop, const bool fromMigrate,
                          uint64_t flags = 0);

        // Overridden to optimize the case where we have an _id primary key.
        BSONObj getValidatedPKFromObject(const BSONObj &obj) const;

        // Overriden to optimize pk generation for an _id primary key.
        // We just need to look for the _id field and, if it exists
        // and is simple, return a wrapped object.
        BSONObj getSimplePKFromQuery(const BSONObj &query) const;
    };

    class OplogCollection : public IndexedCollection {
    public:
        OplogCollection(const StringData &ns, const BSONObj &options);
        // Important: BulkLoadedCollection relies on this constructor
        // doing nothing more than calling the parent IndexedCollection
        // constructor. If this constructor ever does more, we need to
        // modify BulkLoadedCollection to match behavior for the oplog.
        OplogCollection(const BSONObj &serialized);

        // @return the maximum safe key to read for a tailable cursor.
        BSONObj minUnsafeKey();
    };

    class NaturalOrderCollection : public NamespaceDetails {
    public:
        NaturalOrderCollection(const StringData &ns, const BSONObj &options);
        NaturalOrderCollection(const BSONObj &serialized);

        // insert an object, using a fresh auto-increment primary key
        void insertObject(BSONObj &obj, uint64_t flags);

    protected:
        AtomicWord<long long> _nextPK;
    };

    class SystemCatalogCollection : public NaturalOrderCollection {
    public:
        SystemCatalogCollection(const StringData &ns, const BSONObj &options);
        SystemCatalogCollection(const BSONObj &serialized);

        // strip out the _id field before inserting into a system collection
        void insertObject(BSONObj &obj, uint64_t flags);

    private:
        void createIndex(const BSONObj &info);

        // For consistency with Vanilla MongoDB, the system catalogs have the following
        // fields, in order, if they exist.
        //
        //  { key, unique, ns, name, [everything else] }
        //
        // This code is largely borrowed from prepareToBuildIndex() in Vanilla.
        BSONObj beautify(const BSONObj &obj);
    };

    // Class representing the system catalogs.
    // Used for:
    // - db.system.indexes
    // - db.system.namespaces
    class SystemUsersCollection : public IndexedCollection {
        static BSONObj extendedSystemUsersIndexInfo(const StringData &ns);
    public:
        SystemUsersCollection(const StringData &ns, const BSONObj &options);
        SystemUsersCollection(const BSONObj &serialized);
    };

    // Capped collections have natural order insert semantics but borrow (ie: copy)
    // its document modification strategy from IndexedCollections. The size
    // and count of a capped collection is maintained in memory and kept valid
    // on txn abort through a CappedCollectionRollback class in the TxnContext. 
    //
    // Tailable cursors over capped collections may only read up to one less
    // than the minimum uncommitted primary key to ensure that they never miss
    // any data. This information is communicated through minUnsafeKey(). On
    // commit/abort, the any primary keys inserted into a capped collection are
    // noted so we can properly maintain the min uncommitted key.
    //
    // In the implementation, NaturalOrderCollection::_nextPK and the set of
    // uncommitted primary keys are protected together by _mutex. Trimming
    // work is done under the _deleteMutex.
    class CappedCollection : public NaturalOrderCollection {
    public:
        CappedCollection(const StringData &ns, const BSONObj &options,
                         const bool mayIndexId = true);
        CappedCollection(const BSONObj &serialized);

        void fillSpecificStats(BSONObjBuilder &result, int scale) const;

        bool isCapped() const;

        // @return the maximum safe key to read for a tailable cursor.
        BSONObj minUnsafeKey();

        // run an insertion where the PK is specified
        // Can come from the applier thread on a slave or a cloner 
        void insertObjectIntoCappedWithPK(const BSONObj &pk, const BSONObj &obj, uint64_t flags);

        void insertObjectIntoCappedAndLogOps(const BSONObj &obj, uint64_t flags);

        void insertObject(BSONObj &obj, uint64_t flags);

        void deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags);

        // run a deletion where the PK is specified
        // Can come from the applier thread on a slave
        void deleteObjectFromCappedWithPK(const BSONObj &pk, const BSONObj &obj, uint64_t flags);

        void updateObject(const BSONObj &pk, const BSONObj &oldObj, const BSONObj &newObj,
                          const bool logop, const bool fromMigrate,
                          uint64_t flags);

        void updateObjectMods(const BSONObj &pk, const BSONObj &updateobj,
                              const bool logop, const bool fromMigrate,
                              uint64_t flags);

    protected:
        void _insertObject(const BSONObj &obj, uint64_t flags);

        // Note the commit of a transaction, which simple notes completion under the lock.
        // We don't need to do anything with nDelta and sizeDelta because those changes
        // are already applied to in-memory stats, and this transaction has committed.
        void noteCommit(const BSONObj &minPK, long long nDelta, long long sizeDelta);

        // Note the abort of a transaction, noting completion and updating in-memory stats.
        //
        // The given deltas are signed values that represent changes to the collection.
        // We need to roll back those changes. Therefore, we subtract from the current value.
        void noteAbort(const BSONObj &minPK, long long nDelta, long long sizeDelta);

    private:
        // requires: _mutex is held
        void noteUncommittedPK(const BSONObj &pk);

        BSONObj getNextPK();

        // Note the completion of a transaction by removing its
        // minimum-PK-inserted (if there is one) from the set.
        void noteComplete(const BSONObj &minPK);

        void checkGorged(const BSONObj &obj, bool logop);

        void checkUniqueIndexes(const BSONObj &pk, const BSONObj &obj, bool checkPk);

        // Checks unique indexes and does the actual inserts.
        // Does not check if the collection became gorged.
        void checkUniqueAndInsert(const BSONObj &pk, const BSONObj &obj, uint64_t flags, bool checkPk);

        bool isGorged(long long n, long long size) const;

        void _deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags);

        void trim(int objsize, bool logop);

        // Remove everything from this capped collection
        void empty();

        const long long _maxSize;
        const long long _maxObjects;
        AtomicWord<long long> _currentObjects;
        AtomicWord<long long> _currentSize;
        BSONObj _lastDeletedPK;
        // The set of minimum-uncommitted-PKs for this capped collection.
        // Each transaction that has done inserts has the minimum PK it
        // inserted in this set.
        //
        // Tailable cursors must not read at or past the smallest value in this set.
        BSONObjSet _uncommittedMinPKs;
        SimpleMutex _mutex;
        SimpleMutex _deleteMutex;
    };

    // Profile collections are non-replicated capped collections that
    // cannot be updated and do not add the _id field on insert.
    class ProfileCollection : public CappedCollection {
    public:
        ProfileCollection(const StringData &ns, const BSONObj &options);
        ProfileCollection(const BSONObj &serialized);

        void insertObjectIntoCappedWithPK(const BSONObj &pk, const BSONObj &obj, uint64_t flags);

        void insertObjectIntoCappedAndLogOps(const BSONObj &obj, uint64_t flags);

        void insertObject(BSONObj &obj, uint64_t flags);

        void deleteObjectFromCappedWithPK(const BSONObj &pk, const BSONObj &obj, uint64_t flags);

        void updateObject(const BSONObj &pk, const BSONObj &oldObj, const BSONObj &newObj,
                          const bool logop, const bool fromMigrate,
                          uint64_t flags = 0);

        void updateObjectMods(const BSONObj &pk, const BSONObj &updateobj,
                          const bool logop, const bool fromMigrate,
                          uint64_t flags = 0);

    private:
        void createIndex(const BSONObj &idx_info);
    };

    // A BulkLoadedCollection is a facade for an IndexedCollection that utilizes
    // a bulk loader for insertions. Other flavors of writes are not allowed.
    //
    // The underlying indexes must exist and be empty.
    class BulkLoadedCollection : public IndexedCollection {
    public:
        BulkLoadedCollection(const BSONObj &serialized);

        void close(const bool abortingLoad);

        void validateConnectionId(const ConnectionId &id);

        void insertObject(BSONObj &obj, uint64_t flags = 0);

        void deleteObject(const BSONObj &pk, const BSONObj &obj, uint64_t flags = 0);

        void updateObject(const BSONObj &pk, const BSONObj &oldObj, const BSONObj &newObj,
                          const bool logop, const bool fromMigrate,
                          uint64_t flags = 0);

        void updateObjectMods(const BSONObj &pk, const BSONObj &updateobj,
                              const bool logop, const bool fromMigrate,
                              uint64_t flags = 0);

        void empty();

        void optimizeAll();

        void optimizePK(const BSONObj &leftPK, const BSONObj &rightPK,
                        const int timeout, uint64_t *loops_run);

        bool dropIndexes(const StringData& name, string &errmsg,
                         BSONObjBuilder &result, bool mayDeleteIdIndex);

    private:
        // When closing a BulkLoadedCollection, we need to make sure the key trackers and
        // loaders are destructed before we call up to the parent destructor, because they
        // reference storage::Dictionaries that get destroyed in the parent destructor.
        void _close();

        void createIndex(const BSONObj &info);

        // The connection that started the bulk load is the only one that can
        // do anything with the namespace until the load is complete and this
        // namespace has been closed / re-opened.
        ConnectionId _bulkLoadConnectionId;
        scoped_array<DB *> _dbs;
        scoped_array< scoped_ptr<MultiKeyTracker> > _multiKeyTrackers;
        scoped_ptr<storage::Loader> _loader;
    };

} // namespace mongo
