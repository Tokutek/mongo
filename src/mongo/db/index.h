// index.h

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

#pragma once

#include "mongo/pch.h"

#include <db.h>
#include <vector>

#include "mongo/db/client.h"
#include "mongo/db/descriptor.h"
#include "mongo/db/keygenerator.h"
#include "mongo/db/storage/builder.h"
#include "mongo/db/storage/cursor.h"
#include "mongo/db/storage/dictionary.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/key.h"
#include "mongo/db/storage/txn.h"

namespace mongo {

    class Cursor; 
    class NamespaceDetails;

    // Represents an index of a collection.
    class IndexDetails : boost::noncopyable {
    public:
        // Only creates internals, useful for tests.
        IndexDetails(const BSONObj &info);

        // Creates an IndexDetails subclass of the appropriate type.
        //
        // Currently, we have:
        // - Regular indexes
        // - Hashed indexes
        // In the future:
        // - Geo indexes?
        // - FTS indexes?
        static shared_ptr<IndexDetails> make(const BSONObj &info, const bool may_create = true);
        virtual ~IndexDetails();

        // @return the "special" name for this index.
        virtual const string &getSpecialIndexName() const {
            static string name = "";
            return name;
        }

        // Is this index special? True for subclasses of IndexDetails.
        virtual bool special() const {
            return false;
        }

        // How suitable is this index for a given query and sort order?
        enum Suitability {
            USELESS = 0,
            HELPFUL = 1,
            OPTIMAL = 2
        };
        virtual Suitability suitability(const BSONObj &query, const BSONObj &order) const;

        virtual shared_ptr<mongo::Cursor> newCursor(const BSONObj &query,
                                                    const BSONObj &order,
                                                    const int numWanted = 0) const {
            msgasserted( 16912, str::stream() << 
                                "Should not have called newCursor on a non-special index: " <<
                                indexNamespace() );
        }

        // Closes the underlying ydb dictionary.
        void close();

        /* pull out the relevant key objects from obj, so we
           can index them.  Note that the set is multiple elements
           only when it's a "multikey" array.
           keys will be left empty if key not found in the object.
        */
        void getKeysFromObject(const BSONObj &obj, BSONObjSet &keys) const;

        BSONObj getKeyFromQuery(const BSONObj& query) const {
            return query.extractFieldsUnDotted(_keyPattern);
        }

        /* get the key pattern for this object.
           e.g., { lastname:1, firstname:1 }
        */
        const BSONObj &keyPattern() const {
            dassert(_info["key"].Obj() == _keyPattern);
            return _keyPattern;
        }

        /**
         * @return offset into keyPattern for key
                   -1 if doesn't exist
         */
        int keyPatternOffset( const StringData& key ) const;
        bool inKeyPattern( const StringData& key ) const { return keyPatternOffset( key ) >= 0; }

        static string indexNamespace(const StringData& ns, const StringData& idxName) {
            dassert(ns != "" && idxName != "");
            stringstream ss;
            ss << ns.toString() << ".$" << idxName.toString();
            return ss.str();
        }

        // returns name of this index's storage area
        // database.table.$index
        string indexNamespace() const {
            return indexNamespace(parentNS(), indexName());
        }

        string indexName() const { // e.g. "ts_1"
            return _info["name"].String();
        }

        static bool isIdIndexPattern( const BSONObj &pattern ) {
            BSONObjIterator i(pattern);
            BSONElement e = i.next();
            // _id index must have form exactly {_id : 1} or {_id : -1}.
            // Allows an index of form {_id : "hashed"} to exist but
            // do not consider it to be the primary _id index
            if (!(strcmp(e.fieldName(), "_id") == 0 && (e.numberInt() == 1 || e.numberInt() == -1))) {
                return false;
            }
            return i.next().eoo();
        }

        /* returns true if this is the _id index. */
        bool isIdIndex() const {
            return isIdIndexPattern( keyPattern() );
        }

        /* gets not our namespace name (indexNamespace for that),
           but the collection we index, its name.
           */
        string parentNS() const {
            return _info["ns"].String();
        }

        /** @return true if index has unique constraint */
        bool unique() const {
            dassert(_info["unique"].trueValue() == _unique);
            return _unique;
        }

        bool sparse() const {
            dassert(_info["sparse"].trueValue() == _sparse);
            return _sparse;
        }

        /** @return true if index is clustering */
        bool clustering() const {
            dassert(_info["clustering"].trueValue() == _clustering);
            return _clustering;
        }

        string toString() const {
            return _info.toString();
        }

        const BSONObj &info() const {
            return _info;
        }

        /** delete this index. */
        void kill_idx();

        enum toku_compression_method getCompressionMethod() const;
        uint32_t getPageSize() const;
        uint32_t getReadPageSize() const;
        void getStat64(DB_BTREE_STAT64* stats) const;
        void optimize(const storage::Key &leftSKey, const storage::Key &rightSKey,
                      const bool sendOptimizeMessage);
        void acquireTableLock();

        struct UniqueCheckExtra : public ExceptionSaver {
            const storage::Key &newKey;
            const Descriptor &descriptor;
            bool &isUnique;
            std::exception *ex;
            UniqueCheckExtra(const storage::Key &sKey, const Descriptor &d, bool &u) :
                newKey(sKey), descriptor(d), isUnique(u), ex(NULL) {
            }
        };
        static int uniqueCheckCallback(const DBT *key, const DBT *val, void *extra);
        void uniqueCheck(const BSONObj &key, const BSONObj *pk) const ;
        void uassertedDupKey(const BSONObj &key) const;

        template<class Callback>
        void getKeyAfterBytes(const storage::Key &startKey, uint64_t skipLen, Callback &cb) const;

        // Index access statistics
        struct AccessStats {
#pragma pack(8)
            // If each word sits on its own cache line, we prevent false-sharing
            // in the cache and reduce cache-invalidation stalls.
            struct CacheLineWord : AtomicWord<uint64_t> {
                char _pad[64 - sizeof(AtomicWord<uint64_t>)];
            };
#pragma pack()
            BOOST_STATIC_ASSERT(sizeof(CacheLineWord) == 64);
            CacheLineWord queries;
            CacheLineWord nscanned;
            CacheLineWord nscannedObjects;
            CacheLineWord inserts;
            CacheLineWord deletes;
            // Not sure how to capture updates just yet
            //CacheLineWord updates;
        };

        const AccessStats &getAccessStats() const {
            return _accessStats;
        }

        // Book-keeping for index access, displayed in db.stats()
        void noteQuery(const long long nscanned, const long long nscannedObjects) const {
            _accessStats.queries.fetchAndAdd(1);
            _accessStats.nscanned.fetchAndAdd(nscanned);
            _accessStats.nscannedObjects.fetchAndAdd(nscannedObjects);
        }
        void noteInsert() const {
            _accessStats.inserts.fetchAndAdd(1);
        }
        void noteDelete() const {
            _accessStats.deletes.fetchAndAdd(1);
        }

        class Cursor : public storage::Cursor {
        public:
            Cursor(const IndexDetails &idx, const int flags = 0) :
                storage::Cursor(idx.db(), flags) {
            }
        };

        class Builder {
        public:
            Builder(IndexDetails &idx);

            void insertPair(const BSONObj &key, const BSONObj *pk, const BSONObj &val);

            void done();

        private:
            IndexDetails &_idx;
            DB *_db;
            storage::Loader _loader;
        };

    protected:
        // Open ydb dictionary representing the index on disk.
        shared_ptr<storage::Dictionary> _db;

        DB *db() const {
            return _db->db();
        }

        static int hot_opt_callback(void *extra, float progress);

        // Info about the index. Stored on disk in the database.ns dictionary
        // for this database as a BSON object.
        const BSONObj _info;

        // Precomputed values from _info, for speed.
        const BSONObj _keyPattern;
        const bool _unique;
        const bool _sparse;
        const bool _clustering;

        // Used to describe the index to the ydb layer.
        //
        // A default descriptor is generated by the IndexDetails constructor.
        // The scoped pointer is possibly reset() to a different descriptor
        // in by subclass constructors.
        scoped_ptr<Descriptor> _descriptor;

    private:
        mutable AccessStats _accessStats;

        // Must be called after constructor. Opens the ydb dictionary
        // using _descriptor, which is set by subclass constructors.
        //
        // Only IndexDetails::make() calls the constructor / open.
        void open(const bool may_create);

        friend class NamespaceDetails;
        friend class BulkLoadedCollection;
    };

    // class to store statistics about an IndexDetails
    class IndexStats {
    public:
        explicit IndexStats(const IndexDetails &idx);
        BSONObj obj(int scale) const;
        uint64_t getCount() const {
            return _stats.bt_nkeys;
        }
        uint64_t getDataSize() const {
            return _stats.bt_dsize;
        }
        uint64_t getStorageSize() const {
            return _stats.bt_fsize;
        }
        uint64_t getQueryCount() const {
            return _accessStats.queries.load();
        }
        uint64_t getNscanned() const {
            return _accessStats.nscanned.load();
        }
        uint64_t getNscannedObjects() const {
            return _accessStats.nscannedObjects.load();
        }
        uint64_t getInsertCount() const {
            return _accessStats.inserts.load();
        }
        uint64_t getDeleleCount() const {
            return _accessStats.deletes.load();
        }
    private:
        string _name;
        DB_BTREE_STAT64 _stats;
        enum toku_compression_method _compressionMethod;
        uint32_t _readPageSize;
        uint32_t _pageSize;
        const IndexDetails::AccessStats &_accessStats;
    };

    template<class Callback>
    void IndexDetails::getKeyAfterBytes(const storage::Key &startKey, uint64_t skipLen, Callback &cb) const {
        class CallbackWrapper {
            Callback &_cb;
          public:
            std::exception *ex;
            CallbackWrapper(Callback &cb) : _cb(cb), ex(NULL) {}
            static void call(const DBT *endKeyDBT, uint64_t skipped, void *thisv) {
                CallbackWrapper *t = static_cast<CallbackWrapper *>(thisv);
                try {
                    if (endKeyDBT == NULL) {
                        t->_cb(NULL, NULL, skipped);
                    }
                    else {                
                        storage::KeyV1 endKey(static_cast<char *>(endKeyDBT->data));
                        if (endKey.dataSize() < (ssize_t) endKeyDBT->size) {
                            BSONObj endPK(static_cast<char *>(endKeyDBT->data) + endKey.dataSize());
                            t->_cb(&endKey, &endPK, skipped);
                        }
                        else {
                            t->_cb(&endKey, NULL, skipped);
                        }
                    }
                }
                catch (std::exception &e) {
                    t->ex = &e;
                }
            }
        };
        DBT startDBT = startKey.dbt();
        CallbackWrapper cbw(cb);
        int r = db()->get_key_after_bytes(db(), cc().txn().db_txn(), &startDBT, skipLen, &CallbackWrapper::call, &cbw, 0);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        if (cbw.ex != NULL) {
            throw *cbw.ex;
        }
    }

    // Sets db->app_private to a bool that gets set
    // if storage::generate_keys() generates multikeys.
    // On destruction, safely unsets db->app_private.
    //
    // Used by the hot indexer and loader to track
    // which indexes are multikey.
    class MultiKeyTracker : boost::noncopyable {
    public:
        MultiKeyTracker(DB *db) :
            _db(db), _multiKey(false) {
            _db->app_private = &_multiKey;
        }
        ~MultiKeyTracker() {
            _db->app_private = NULL;
        }
        bool isMultiKey() const {
            return _multiKey;
        }

    private:
        DB *_db;
        bool _multiKey;
    };

} // namespace mongo

