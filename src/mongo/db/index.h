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
#include "mongo/db/indexkey.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/descriptor.h"
#include "mongo/db/storage/cursor.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/key.h"
#include "mongo/db/storage/txn.h"
#include "mongo/db/storage/loader.h"

namespace mongo {

    class NamespaceDetails;

    /* Details about a particular index. There is one of these effectively for each object in
       system.namespaces (although this also includes the head pointer, which is not in that
       collection).
     */
    class IndexDetails : boost::noncopyable {
    public:
        explicit IndexDetails(const BSONObj &info, bool may_create=true);

        ~IndexDetails();

        // Closes the underlying DB *.  In case that throws, we can't do it in the destructor.
        void close();

        BSONObj getKeyFromQuery(const BSONObj& query) const {
            BSONObj k = keyPattern();
            BSONObj res = query.extractFieldsUnDotted(k);
            return res;
        }

        /* pull out the relevant key objects from obj, so we
           can index them.  Note that the set is multiple elements
           only when it's a "multikey" array.
           keys will be left empty if key not found in the object.
        */
        void getKeysFromObject(const BSONObj &obj, BSONObjSet &keys) const;

        /* get the key pattern for this object.
           e.g., { lastname:1, firstname:1 }
        */
        BSONObj keyPattern() const {
            dassert(_info["key"].Obj() == _keyPattern);
            return _keyPattern;
        }

        /**
         * @return offset into keyPattern for key
                   -1 if doesn't exist
         */
        int keyPatternOffset( const StringData& key ) const;
        bool inKeyPattern( const StringData& key ) const { return keyPatternOffset( key ) >= 0; }

        /* true if the specified key is in the index */
        bool hasKey(const BSONObj& key);

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

        /** @return true if index is clustering */
        bool clustering() const {
            dassert(_info["clustering"].trueValue() == _clustering);
            return _clustering;
        }

        /** delete this index. */
        void kill_idx();

        const IndexSpec& getSpec() const;

        string toString() const {
            return _info.toString();
        }

        const BSONObj &info() const {
            return _info;
        }

        void insertPair(const BSONObj &key, const BSONObj *pk, const BSONObj &val, uint64_t flags);
        void deletePair(const BSONObj &key, const BSONObj *pk, uint64_t flags);
        void acquireTableLock();

        enum toku_compression_method getCompressionMethod() const;
        uint32_t getPageSize() const;
        uint32_t getReadPageSize() const;
        void getStat64(DB_BTREE_STAT64* stats) const;
        void optimize();

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

        class Cursor : public storage::Cursor {
        public:
            Cursor(const IndexDetails &idx, const int flags = 0) :
                storage::Cursor(idx._db, flags) {
            }
        };

        class Builder {
        public:
            Builder(IndexDetails &idx);

            void insertPair(const BSONObj &key, const BSONObj *pk, const BSONObj &val);

            void done();

        private:
            IndexDetails &_idx;
            storage::Loader _loader;
        };

    private:
        // Open dictionary representing the index on disk.
        DB *_db;

        // Used to describe the index to the ydb layer, for key
        // comparisons and, later, for key generation.
        Descriptor _descriptor;

        static int hot_opt_callback(void *extra, float progress);

        // Info about the index. Stored on disk in the database.ns dictionary
        // for this database as a BSON object.
        const BSONObj _info;

        // Precomputed values from _info, for speed.
        const BSONObj _keyPattern;
        const bool _unique;
        const bool _clustering;

        friend class NamespaceDetails;
    };

    // class to store statistics about an IndexDetails
    class IndexStats {
    public:
        explicit IndexStats(const IndexDetails &idx);
        BSONObj bson(int scale) const;
        uint64_t getCount() const {
            return _stats.bt_nkeys;
        }
        uint64_t getDataSize() const {
            return _stats.bt_dsize;
        }
        uint64_t getStorageSize() const {
            return _stats.bt_fsize;
        }
    private:
        string _name;
        DB_BTREE_STAT64 _stats;
        enum toku_compression_method _compressionMethod;
        uint32_t _readPageSize;
        uint32_t _pageSize;
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
        int r = _db->get_key_after_bytes(_db, cc().txn().db_txn(), &startDBT, skipLen, &CallbackWrapper::call, &cbw, 0);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        if (cbw.ex != NULL) {
            throw *cbw.ex;
        }
    }

} // namespace mongo

