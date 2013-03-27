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

#include <algorithm>
#include <list>
#include <map>
#include <vector>
#include <utility>

#include <boost/filesystem/operations.hpp>

#include "mongo/db/cursor.h"
#include "mongo/db/db.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/json.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/oplog.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/txn.h"
#include "mongo/db/storage/key.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/scripting/engine.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/db/db_flags.h"

namespace mongo {

    NamespaceIndex* nsindex(const char *ns) {
        Database *database = cc().database();
        verify( database );
        DEV {
            char buf[256];
            nsToDatabase(ns, buf);
            if ( database->name != buf ) {
                out() << "ERROR: attempt to write to wrong database\n";
                out() << " ns:" << ns << '\n';
                out() << " database->name:" << database->name << endl;
                verify( database->name == buf );
            }
        }
        return &database->namespaceIndex;
    }

    NamespaceDetails* nsdetails(const char *ns) {
        return nsindex(ns)->details(ns);
    }

    NamespaceDetails* nsdetails_maybe_create(const char *ns, BSONObj options) {
        NamespaceIndex *ni = nsindex(ns);
        if (!ni->allocated()) {
            // Must make sure we loaded any existing namespaces before checking, or we might create one that already exists.
            ni->init(true);
        }
        NamespaceDetails *details = ni->details(ns);
        if (details == NULL) {
            tokulog(2) << "Didn't find nsdetails(" << ns << "), creating it." << endl;

            Namespace ns_s(ns);
            shared_ptr<NamespaceDetails> new_details( NamespaceDetails::make(ns, options) );
            ni->add_ns(ns, new_details);

            details = ni->details(ns);
            details->addDefaultIndexesToCatalog();
        }
        return details;
    }

#pragma pack(1)
    struct IDToInsert {
        IDToInsert() : type((char) jstOID) {
            memcpy(_id, "_id", sizeof(_id));
            dassert( sizeof(IDToInsert) == 17 );
            oid.init();
        }
        const char *rawdata() {
            return reinterpret_cast<const char *>(this);
        }
        const char type;
        char _id[4];
        OID oid;
    };
#pragma pack()

    BSONObj addIdField(const BSONObj &orig) {
        if (orig.hasField("_id")) {
            return orig;
        } else {
            IDToInsert id;
            BSONObjBuilder b;
            // _id first, everything else after
            b.append(BSONElement(id.rawdata()));
            b.appendElements(orig);
            return b.obj();
        }
    }

    class IndexedCollection : public NamespaceDetails {
    public:
        IndexedCollection(const string &ns, const BSONObj &options) :
            NamespaceDetails(ns, fromjson("{\"_id\":1}"), options) {
        }
        IndexedCollection(const BSONObj &serialized) :
            NamespaceDetails(serialized) {
        }

        // regular collections are eligble for queryIdHack(), see ops/query.cpp
        bool mayFindById() const {
            dassert(hasIdIndex());
            return true;
        }

        // finds an objectl by _id field, which in the case of indexed collections
        // is the primary key.
        bool findById(const BSONObj &query, BSONObj &result) {
            dassert(query["_id"].ok());
            return findByPK(query["_id"].wrap(""), result);
        }

        // inserts an object into this namespace, taking care of secondary indexes if they exist
        void insertObject(BSONObj &obj, uint64_t flags) {
            obj = addIdField(obj);
            BSONObj pk = obj["_id"].wrap("");
            insertIntoIndexes(pk, obj, flags);
        }

        // deletes an object from this namespace, taking care of secondary indexes if they exist
        void deleteObject(const BSONObj &pk, const BSONObj &obj) {
            deleteFromIndexes(pk, obj);
        }
    };

    struct getfExtra {
        getfExtra(BSONObj &o) : obj(o) {
        }
        BSONObj &obj;
    };

    static int getfCallback(const DBT *key, const DBT *value, void *extra) {
        struct getfExtra *info = reinterpret_cast<struct getfExtra *>(extra);
        if (key) {
            info->obj = BSONObj(reinterpret_cast<char *>(key->data));
        }
        return 0;
    }

    class NaturalOrderCollection : public NamespaceDetails {
    public:
        NaturalOrderCollection(const string &ns, const BSONObj &options) :
            NamespaceDetails(ns, fromjson("{\"$_\":1}"), options),
            _nextPK(0) {
        }
        NaturalOrderCollection(const BSONObj &serialized) :
            NamespaceDetails(serialized),
            _nextPK(0) {

            // the next PK, if it exists, is the last key + 1
            int r;
            IndexDetails &pkIdx = getPKIndex();
            IndexDetails::Cursor c(&pkIdx);
            DBC *cursor = c.dbc();

            BSONObj obj = BSONObj();
            struct getfExtra extra(obj);
            r = cursor->c_getf_last(cursor, 0, getfCallback, &extra);
            verify(r == 0 || r == DB_NOTFOUND);
            if (!obj.isEmpty()) {
                dassert(obj.nFields() == 1);
                _nextPK = AtomicWord<long long>(obj.firstElement().Long() + 1);
            }
        }

        // insert an object, using a fresh auto-increment primary key
        void insertObject(BSONObj &obj, uint64_t flags) {
            BSONObjBuilder pk;
            pk.append("", _nextPK.fetchAndAdd(1));
            insertIntoIndexes(pk.obj(), obj, flags);
        }

        void deleteObject(const BSONObj &pk, const BSONObj &obj) {
            deleteFromIndexes(pk, obj);
        }

    protected:
        AtomicWord<long long> _nextPK;
    };

    class SystemCatalog : public NaturalOrderCollection {
    public:
        SystemCatalog(const string &ns, const BSONObj &options) :
            NaturalOrderCollection(ns, options) {
        }
        SystemCatalog(const BSONObj &serialized) :
            NaturalOrderCollection(serialized) {
        }

        // strip out the _id field before inserting into a system collection
        void insertObject(BSONObj &obj, uint64_t flags) {
            obj = beautify(obj);
            NaturalOrderCollection::insertObject(obj, flags);
        }

        void createIndex(const BSONObj &info) {
            massert(16464, "bug: system collections should not be indexed.", false);
        }

    private:
        // For consistency with Vanilla MongoDB, the system catalogs have the following
        // fields, in order, if they exist.
        //
        //  { key, unique, ns, name, [everything else] }
        //
        // This code is largely borrowed from prepareToBuildIndex() in Vanilla.
        BSONObj beautify(const BSONObj &obj) {
            BSONObjBuilder b;
            if (obj["key"].ok()) {
                b.append(obj["key"]);
            }
            if (obj["unique"].trueValue()) {
                b.appendBool("unique", true);
            }
            if (obj["ns"].ok()) {
                b.append(obj["ns"]);
            }
            if (obj["name"].ok()) { 
                b.append(obj["name"]);
            }
            for (BSONObjIterator i = obj.begin(); i.more(); ) {
                BSONElement e = i.next();
                string s = e.fieldName();
                if (s != "key" && s != "unique" && s != "ns" && s != "name" && s != "_id") {
                    b.append(e);
                }
            }
            return b.obj();
        }
    };

    class CappedCollection : public NaturalOrderCollection {
    public:
        CappedCollection(const string &ns, const BSONObj &options) :
            NaturalOrderCollection(ns, options),
            _maxSize(options["size"].numberLong()),
            _maxObjects(options["max"].numberLong()),
            _currentObjects(0),
            _currentSize(0),
            _deleteMutex("cappedDeleteMutex") {

            // Create an _id index if "autoIndexId" is missing or it exists as true.
            const BSONElement e = options["autoIndexId"];
            if (!e.ok() || e.trueValue()) {
                BSONObj info = indexInfo(fromjson("{\"_id\":1}"), true, false);
                createIndex(info);
            }
        }
        CappedCollection(const BSONObj &serialized) :
            NaturalOrderCollection(serialized),
            _maxSize(serialized["size"].numberLong()),
            _maxObjects(serialized["max"].numberLong()),
            _currentObjects(0),
            _currentSize(0),
            _deleteMutex("cappedDeleteMutex") {
            
            // Determine the number of objects and the total size.
            // We'll have to look at the data, but this might not be so bad because:
            // - you pay for it once, on initialization.
            // - capped collections are meant to be "small" (fit in memory)
            // - capped collectiosn are meant to be "read heavy",
            //   so bringing it all into memory now helps warmup.
            long long n = 0;
            long long size = 0;
            shared_ptr<Cursor> c( new BasicCursor(this) );
            for ( ; c->ok(); n++, c->advance()) {
                size += c->current().objsize();
            }

            _currentObjects = AtomicWord<long long>(n);
            _currentSize = AtomicWord<long long>(size);
            verify((_currentSize.load() > 0) == (_currentObjects.load() > 0));
        }

        bool isCapped() const {
            dassert(_options["capped"].trueValue());
            return true;
        }

        void insertObject(BSONObj &obj, uint64_t flags) {
            CappedInsertIntent intent(this, obj);

            // If the insert is non-trivial, then we need to do some
            // trimming work to make room for the object. Grab the 
            // mutex and do any necessary work.
            if (!intent.trivial()) {
                SimpleMutex::scoped_lock lk(_deleteMutex);
                long long n = _currentObjects.load();
                long long size = _currentSize.load();
                if (isGorged(n, size)) {
                    scoped_ptr<Cursor> c( new BasicCursor(this) );
                    // Delete older objects until we've made enough room for
                    // the new one.
                    // If other threads are trying to insert concurrently, we will do some work on their behalf (until !isGorged).
                    // But we stop if we've deleted 8 objects and done enough to satisfy our own intent, to limit latency.
                    long long bytesTrimmed = 0;
                    for ( long long i = 0;
                          c->ok() && isGorged(n, size) && (bytesTrimmed < obj.objsize() || (i < 8));
                          i++, c->advance()) {
                        BSONObj oldestObj = c->current();
                        NaturalOrderCollection::deleteObject(c->currPK(), oldestObj);

                        size_t objsize = oldestObj.objsize();
                        n = _currentObjects.subtractAndFetch(1);
                        size = _currentSize.subtractAndFetch(objsize);
                        bytesTrimmed += objsize;
                    }
                }
            }

            NaturalOrderCollection::insertObject(obj, flags);
            intent.success();
        }

        void deleteObject(const BSONObj &pk, const BSONObj &obj) {
            massert(16460, "bug: cannot remove from a capped collection, "
                    " should have been enforced higher in the stack", false);
        }

        void updateObject(const BSONObj &pk, const BSONObj &oldObj, const BSONObj &newObj) {
            long long diff = newObj.objsize() - oldObj.objsize();
            uassert( 10003 , "failing update: objects in a capped ns cannot grow", diff <= 0 );
            NaturalOrderCollection::updateObject(pk, oldObj, newObj);
            if (diff < 0) {
                _currentSize.addAndFetch(diff);
            }
        }

    private:
        // Declares a thread's intent to insert into a capped collection.
        // On creation, notes the objects size into the collection's 
        // current size and rolls back the modification on error.
        //
        // By proactively increasing the collection size before inserting,
        // we enable a single thread to perform trimming on behalf of many
        // waiting threads (at that thread's discretion, that is. Do not
        // do so much work that the calling client thread stalls noticably).
        class CappedInsertIntent : boost::noncopyable {
        public:
            CappedInsertIntent(CappedCollection *collection, const BSONObj &obj) :
                _collection(collection),
                _obj(obj),
                _succeeded(false),
                _trivial(false) {

                uassert(16465, "object to insert is larger than the capped collection itself",
                        _obj.objsize() <= _collection->_maxSize);
                long long n = _collection->_currentObjects.addAndFetch(1);
                long long size = _collection->_currentSize.addAndFetch(obj.objsize());
                _trivial = !_collection->isGorged(n, size);
            }
            ~CappedInsertIntent() {
                if (!_succeeded) {
                    // rollback size/count increments on failure
                    _collection->_currentObjects.subtractAndFetch(1);
                    _collection->_currentSize.subtractAndFetch(_obj.objsize());
                }
            }
            bool trivial() const {
                return _trivial;
            }
            void success() {
                _succeeded = true;
            }
        private:
            CappedCollection *_collection;
            const BSONObj &_obj;
            bool _succeeded;
            bool _trivial;
        };

        bool isGorged(long long n, long long size) const {
            return (_maxObjects > 0 && n > _maxObjects)
                || (_maxSize > 0 && size > _maxSize);
        }

        const long long _maxSize;
        const long long _maxObjects;
        AtomicWord<long long> _currentObjects;
        AtomicWord<long long> _currentSize;
        SimpleMutex _deleteMutex;
    };

    BSONObj NamespaceDetails::indexInfo(const BSONObj &keyPattern, bool unique, bool clustering) {
        // Can only create the _id and $_ indexes internally.
        dassert(keyPattern.nFields() == 1);
        dassert(keyPattern["_id"].ok() || keyPattern["$_"].ok());

        BSONObjBuilder b;
        b.append("ns", _ns);
        b.append("key", keyPattern);
        b.append("name", keyPattern["_id"].ok() ? "_id_" : "$_");
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

    static bool isSystemCatalog(const string &ns) {
        return str::contains(ns, ".system.indexes") || str::contains(ns, ".system.namespaces");
    }

    NamespaceDetails::NamespaceDetails(const string &ns, const BSONObj &pkIndexPattern, const BSONObj &options) :
        _ns(ns),
        _options(options.copy()),
        _pk(pkIndexPattern.copy()),
        _indexBuildInProgress(false),
        _nIndexes(0),
        _multiKeyIndexBits(0) {

        massert( 10356 ,  str::stream() << "invalid ns: " << ns , NamespaceString::validCollectionName(ns.c_str()));

        tokulog(1) << "Creating NamespaceDetails " << ns << endl;

        // Create the primary key index, generating the info from the pk pattern and options.
        BSONObj info = indexInfo(pkIndexPattern, true, true);
        createIndex(info);

        addNewNamespaceToCatalog(ns, !options.isEmpty() ? &options : NULL);
    }
    shared_ptr<NamespaceDetails> NamespaceDetails::make(const string &ns, const BSONObj &options) {
        if (isSystemCatalog(ns)) {
            return shared_ptr<NamespaceDetails>(new SystemCatalog(ns, options));
        } else if (options["capped"].trueValue()) {
            return shared_ptr<NamespaceDetails>(new CappedCollection(ns, options));
        } else {
            return shared_ptr<NamespaceDetails>(new IndexedCollection(ns, options));
        }
    }

    NamespaceDetails::NamespaceDetails(const BSONObj &serialized) :
        _ns(serialized["ns"].String()),
        _options(serialized["options"].embeddedObject().copy()),
        _pk(serialized["pk"].embeddedObject().copy()),
        _indexBuildInProgress(false),
        _nIndexes(serialized["indexes"].Array().size()),
        _multiKeyIndexBits(static_cast<uint64_t>(serialized["multiKeyIndexBits"].Long())) {

        std::vector<BSONElement> index_array = serialized["indexes"].Array();
        for (std::vector<BSONElement>::iterator it = index_array.begin(); it != index_array.end(); it++) {
            shared_ptr<IndexDetails> idx(new IndexDetails(it->Obj(), false));
            _indexes.push_back(idx);
        }
    }
    shared_ptr<NamespaceDetails> NamespaceDetails::make(const BSONObj &serialized) {
        if (isSystemCatalog(serialized["ns"])) {
            return shared_ptr<NamespaceDetails>(new SystemCatalog(serialized));
        } else if (serialized["options"]["capped"].trueValue()) {
            return shared_ptr<NamespaceDetails>(new CappedCollection(serialized));
        } else {
            return shared_ptr<NamespaceDetails>(new IndexedCollection(serialized));
        }
    }

    BSONObj NamespaceDetails::serialize(const char *ns, const BSONObj &options, const BSONObj &pk,
            unsigned long long multiKeyIndexBits, const BSONArray &indexes_array) {
        return BSON("ns" << ns <<
                    "options" << options <<
                    "pk" << pk <<
                    "multiKeyIndexBits" << static_cast<long long>(multiKeyIndexBits) <<
                    "indexes" << indexes_array);
    }

    BSONObj NamespaceDetails::serialize() const {
        BSONArrayBuilder indexes_array;
        for (IndexVector::const_iterator it = _indexes.begin(); it != _indexes.end(); it++) {
            IndexDetails *index = it->get();
            indexes_array.append(index->info());
        }
        return serialize(_ns.c_str(), _options, _pk, _multiKeyIndexBits, indexes_array.arr());
    }

    struct findByPKCallbackExtra {
        const BSONObj &key;
        BSONObj &obj;

        findByPKCallbackExtra(const BSONObj &k, BSONObj &o) : key(k), obj(o) { }
    };

    static int findByPKCallback(const DBT *key, const DBT *value, void *extra) {
        if (key != NULL) {
            struct findByPKCallbackExtra *info = reinterpret_cast<findByPKCallbackExtra *>(extra);
            DEV {
                // We should have been called using an exact getf, so the
                // key is non-null iff we found an exact match.
                BSONObj idKey(reinterpret_cast<char *>(key->data));
                verify(!idKey.isEmpty());
                verify(idKey.woCompare(idKey, info->key) == 0);
            }
            BSONObj obj(reinterpret_cast<char *>(value->data));
            info->obj = obj.getOwned();
        }
        return 0;
    }

    bool NamespaceDetails::findByPK(const BSONObj &key, BSONObj &result) {
        int r;

        // get a cursor over the primary key index
        IndexDetails &pkIdx = getPKIndex();
        IndexDetails::Cursor c(&pkIdx);
        DBC *cursor = c.dbc();

        // create an index key
        DBT key_dbt = storage::make_dbt(key.objdata(), key.objsize());

        // Try to find it.
        BSONObj obj = BSONObj();
        tokulog(3) << "NamespaceDetails::findByPK looking for " << key << endl;
        struct findByPKCallbackExtra extra(key, obj);
        r = cursor->c_getf_set(cursor, 0, &key_dbt, findByPKCallback, &extra);
        verify(r == 0 || r == DB_NOTFOUND);

        if (!obj.isEmpty()) {
            result = obj;
            return true;
        } else {
            return false;
        }
    }

    // Temporarily factored out of insertIntoIndexes() for trickle-loaded index builds.
    void NamespaceDetails::insertIntoOneIndex(const int i, const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        IndexDetails &idx = *_indexes[i];
        if (i == 0) {
            dassert(isPKIndex(idx));
            idx.insertPair(pk, NULL, obj, flags);
        } else {
            BSONObjSet keys;
            idx.getKeysFromObject(obj, keys);
            if (keys.size() > 1) {
                setIndexIsMultikey(_ns.c_str(), i);
            }
            for (BSONObjSet::const_iterator ki = keys.begin(); ki != keys.end(); ++ki) {
                idx.insertPair(*ki, &pk, obj, flags);
            }
        }
    }

    void NamespaceDetails::insertIntoIndexes(const BSONObj &pk, const BSONObj &obj, uint64_t flags) {
        dassert(!pk.isEmpty());
        dassert(!obj.isEmpty());
        if ((flags & ND_UNIQUE_CHECKS_OFF) && _indexes.size() > 1) {
            wunimplemented("overwrite inserts on secondary keys right now don't work");
            //uassert(16432, "can't do overwrite inserts when there are secondary keys yet", !overwrite || _indexes.size() == 1);
        }
        for (int i = 0; i < nIndexes(); i++) {
            insertIntoOneIndex(i, pk, obj, flags);
        }
    }

    void NamespaceDetails::deleteFromIndexes(const BSONObj &pk, const BSONObj &obj) {
        dassert(!pk.isEmpty());
        dassert(!obj.isEmpty());
        for (int i = 0; i < nIndexes(); i++) {
            IndexDetails &idx = *_indexes[i];

            if (i == 0) {
                idx.deletePair(pk, NULL, obj);
            } else {
                BSONObjSet keys;
                idx.getKeysFromObject(obj, keys);
                if (keys.size() > 1) {
                    dassert(isMultikey(i)); // the previous insert should have marked it as multikey
                }
                for (BSONObjSet::const_iterator ki = keys.begin(); ki != keys.end(); ++ki) {
                    idx.deletePair(*ki, &pk, obj);
                }
            }
        }
    }

    void NamespaceDetails::updateObject(const BSONObj &pk, const BSONObj &oldObj, const BSONObj &newObj) {
        tokulog(4) << "NamespaceDetails::updateObject pk "
            << pk << ", old " << oldObj << ", new " << newObj << endl;
        deleteFromIndexes(pk, oldObj);
        insertIntoIndexes(pk, newObj, 0);
    }

    void NamespaceDetails::setIndexIsMultikey(const char *thisns, int i) {
        dassert(string(thisns) == _ns);
        dassert(i < NIndexesMax);
        unsigned long long x = ((unsigned long long) 1) << i;
        if (_multiKeyIndexBits & x) {
            return;
        }
        _multiKeyIndexBits |= x;

        dassert(nsdetails(thisns) == this);
        nsindex(thisns)->update_ns(thisns, serialize(), true);

        NamespaceDetailsTransient::get(thisns).clearQueryCache();
    }

    void NamespaceDetails::createIndex(const BSONObj &idx_info) {
        uassert(16449, "dropDups is not supported and is likely to remain unsupported for some time because it deletes arbitrary data",
                !idx_info["dropDups"].trueValue());
        uassert(12588, "cannot add index with a background operation in progress", !_indexBuildInProgress);
        uassert(12523, "no index name specified", idx_info["name"].ok());

        if (nIndexes() >= NIndexesMax ) {
            string s = (mongoutils::str::stream() <<
                        "add index fails, too many indexes for " << idx_info["ns"].String() <<
                        " key:" << idx_info["key"].Obj().toString());
            log() << s << endl;
            uasserted(12505,s);
        }

        shared_ptr<IndexDetails> index(new IndexDetails(idx_info));
        // Ensure we initialize the spec in case the collection is empty.
        // This also causes an error to be thrown if we're trying to create an invalid index on an empty collection.
        index->getSpec();
        _indexBuildInProgress = true;
        _indexes.push_back(index);
        try {
            string thisns(index->parentNS());
            uint64_t iter = 0;
            const int i = idxNo(*index);
            for (shared_ptr<Cursor> cursor(Helpers::findTableScan(thisns.c_str(), BSONObj())); cursor->ok(); cursor->advance(), iter++) {
                if (iter % 1000 == 0) {
                    killCurrentOp.checkForInterrupt(false); // uasserts if we should stop
                }
                insertIntoOneIndex(i, cursor->currPK(), cursor->current(), 0);
            }
        } catch (DBException &e) {
            _indexes.pop_back();
            _indexBuildInProgress = false;
            throw;
        }
        _nIndexes++;
        _indexBuildInProgress = false;

        string idx_ns = idx_info["ns"].String();
        const char *ns = idx_ns.c_str();

        // The first index we create should be the pk index, when we first create the collection.
        // Therefore the collection's NamespaceDetails should not already exist in the NamespaceIndex.
        const bool may_overwrite = _nIndexes > 1;
        if (!may_overwrite) {
            massert(16435, "first index should be pk index", index->keyPattern() == _pk);
        }
        nsindex(ns)->update_ns(ns, serialize(), may_overwrite);

        NamespaceDetailsTransient::get(ns).addedIndex();
    }

    // Normally, we cannot drop the _id_ index.
    // The parameters mayDeleteIdIndex is here for the case where we call dropIndexes
    // through dropCollection, in which case we are dropping an entire collection,
    // hence the _id_ index will have to go.
    bool NamespaceDetails::dropIndexes(const char *ns, const char *name, string &errmsg, BSONObjBuilder &result, bool mayDeleteIdIndex) {
        tokulog(1) << "dropIndexes " << name << endl;

        //BackgroundOperation::assertNoBgOpInProgForNs(ns);

        NamespaceDetails *d = nsdetails(ns);
        ClientCursor::invalidate(ns);

        if (mongoutils::str::equals(name, "*")) {
            result.append("nIndexesWas", (double) _nIndexes);
            // This is O(n^2), not great, but you can have at most 64 indexes anyway.
            for (IndexVector::iterator it = _indexes.begin(); it != _indexes.end(); ) {
                IndexDetails *idx = it->get();
                if (mayDeleteIdIndex || (!idx->isIdIndex() && !d->isPKIndex(*idx))) {
                    idx->kill_idx();
                    it = _indexes.erase(it);
                    _nIndexes--;
                } else {
                    it++;
                }
            }
            // Assuming id index isn't multikey
            _multiKeyIndexBits = 0;
            result.append("msg", (mayDeleteIdIndex
                                  ? "indexes dropped for collection"
                                  : "non-_id indexes dropped for collection"));
        } else {
            int x = findIndexByName(name);
            if (x >= 0) {
                result.append("nIndexesWas", (double) _nIndexes);
                IndexVector::iterator it = _indexes.begin() + x;
                IndexDetails *idx = it->get();
                if ( !mayDeleteIdIndex && (idx->isIdIndex() || d->isPKIndex(*idx)) ) {
                    errmsg = "may not delete _id or $_ index";
                    return false;
                }
                idx->kill_idx();
                _indexes.erase(it);
                _nIndexes--;
                // Removes the nth bit, and shifts any bits higher than it down a slot.
                _multiKeyIndexBits = ((_multiKeyIndexBits & ((1ULL << x) - 1)) |
                                     ((_multiKeyIndexBits >> (x + 1)) << x));
            } else {
                // theoretically, this should not be needed, as we do all of our fileops
                // transactionally, but keeping this here just in case at the moment
                // just in case an orphaned listing there - i.e. should have been repaired but wasn't
                int n = removeFromSysIndexes(ns, name);
                if (n) {
                    log() << "info: removeFromSysIndexes cleaned up " << n << " entries" << endl;
                }
                log() << "dropIndexes: " << name << " not found" << endl;
                errmsg = "index not found";
                return false;
            }
        }
        // Updated whatever in memory structures are necessary, now update the nsindex.
        nsindex(ns)->update_ns(ns, serialize(), true);
        return true;
    }

    void NamespaceDetails::fillIndexStats(std::vector<IndexStats> &indexStats) {
        for (IndexVector::iterator it = _indexes.begin(); it != _indexes.end(); ++it) {
            IndexDetails *index = it->get();
            IndexStats stats(*index);
            indexStats.push_back(stats);
        }
    }

    void NamespaceDetails::optimize() {
        for (IndexVector::iterator it = _indexes.begin(); it != _indexes.end(); ++it) {
            IndexDetails *index = it->get();
            index->optimize();
        }
    }

    void NamespaceDetails::fillCollectionStats(
        struct NamespaceDetailsAccStats* accStats, 
        BSONObjBuilder* result, 
        int scale) 
    {
        uint32_t numIndexes = nIndexes();
        accStats->nIndexes = numIndexes;
        std::vector<IndexStats> indexStats;
        // fill each of the indexStats with statistics
        fillIndexStats(indexStats);
        // also sum up some stats of secondary indexes,
        // calculate their total data size and storage size
        uint64_t totalIndexDataSize = 0;
        uint64_t totalIndexStorageSize = 0;
        BSONArrayBuilder index_info;
        for (std::vector<IndexStats>::const_iterator it = indexStats.begin(); it != indexStats.end(); ++it) {
            index_info.append(it->bson(scale));
            // the primary key is at indexStats[0], secondary indexes come after
            if (it - indexStats.begin() > 0) {
                totalIndexDataSize += it->getDataSize();
                totalIndexStorageSize += it->getStorageSize();
            }
        }

        accStats->count = indexStats[0].getCount();
        result->appendNumber("count", (long long) accStats->count);

        result->append("nindexes" , numIndexes );

        accStats->size = indexStats[0].getDataSize();
        result->appendNumber("size", (long long) accStats->size/scale);

        accStats->storageSize = indexStats[0].getStorageSize();
        result->appendNumber("storageSize", (long long) accStats->storageSize/scale);

        accStats->indexSize = totalIndexDataSize;
        result->appendNumber("totalIndexSize", (long long) totalIndexDataSize/scale);

        accStats->indexStorageSize = totalIndexStorageSize;
        result->appendNumber("totalIndexStorageSize", (long long) totalIndexStorageSize/scale);

        result->append("indexDetails", index_info.arr());        
    }

    static void addIndexToCatalog(const BSONObj &info) {
        string indexns = info["ns"].String();
        if (mongoutils::str::contains(indexns, ".system.indexes")) {
            // system.indexes holds all the others, so it is not explicitly listed in the catalog.
            return;
        }

        char database[256];
        nsToDatabase(indexns.c_str(), database);
        string s = string(database) + ".system.indexes";
        const char *ns = s.c_str();
        NamespaceDetails *d = nsdetails_maybe_create(ns);
        NamespaceDetailsTransient *nsdt = &NamespaceDetailsTransient::get(ns);
        BSONObj objMod = info;
        insertOneObject(d, nsdt, objMod, false);
    }

    void NamespaceDetails::addDefaultIndexesToCatalog() {
        // Either a single primary key or a hidden primary key + _id index.
        dassert(_nIndexes == 1 || (_nIndexes == 2 && findIdIndex() == 1));
        for (int i = 0; i < nIndexes(); i++) {
            addIndexToCatalog(_indexes[i]->info());
        }
    }

    /* ------------------------------------------------------------------------- */

    SimpleMutex NamespaceDetailsTransient::_qcMutex("qc");
    SimpleMutex NamespaceDetailsTransient::_isMutex("is");
    map< string, shared_ptr< NamespaceDetailsTransient > > NamespaceDetailsTransient::_nsdMap;
    typedef map< string, shared_ptr< NamespaceDetailsTransient > >::iterator ouriter;

    void NamespaceDetailsTransient::reset() {
        Lock::assertWriteLocked(_ns); 
        clearQueryCache();
        _keysComputed = false;
        _indexSpecs.clear();
    }

    /*static*/ NOINLINE_DECL NamespaceDetailsTransient& NamespaceDetailsTransient::make_inlock(const char *ns) {
        shared_ptr< NamespaceDetailsTransient > &t = _nsdMap[ ns ];
        verify( t.get() == 0 );
        if( _nsdMap.size() % 20000 == 10000 ) { 
            // so we notice if insanely large #s
            log() << "opening namespace " << ns << endl;
            log() << _nsdMap.size() << " namespaces in nsdMap" << endl;
        }
        t.reset( new NamespaceDetailsTransient(ns) );
        return *t;
    }

    NamespaceDetailsTransient::NamespaceDetailsTransient(const char *ns) : 
        _ns(ns), _keysComputed(false), _qcWriteCount() {
    }

    NamespaceDetailsTransient::~NamespaceDetailsTransient() { 
    }

    void NamespaceDetailsTransient::clearForPrefix(const char *prefix) {
        SimpleMutex::scoped_lock lk(_qcMutex);
        vector< string > found;
        for( ouriter i = _nsdMap.begin(); i != _nsdMap.end(); ++i ) {
            if ( strncmp( i->first.c_str(), prefix, strlen( prefix ) ) == 0 ) {
                found.push_back( i->first );
                Lock::assertWriteLocked(i->first);
            }
        }
        for( vector< string >::iterator i = found.begin(); i != found.end(); ++i ) {
            _nsdMap[ *i ].reset();
        }
    }

    void NamespaceDetailsTransient::eraseForPrefix(const char *prefix) {
        SimpleMutex::scoped_lock lk(_qcMutex);
        vector< string > found;
        for( ouriter i = _nsdMap.begin(); i != _nsdMap.end(); ++i ) {
            if ( strncmp( i->first.c_str(), prefix, strlen( prefix ) ) == 0 ) {
                found.push_back( i->first );
                Lock::assertWriteLocked(i->first);
            }
        }
        for( vector< string >::iterator i = found.begin(); i != found.end(); ++i ) {
            _nsdMap.erase(*i);
        }
    }

    void NamespaceDetailsTransient::computeIndexKeys() {
        _indexKeys.clear();
        NamespaceDetails *d = nsdetails(_ns.c_str());
        if ( ! d )
            return;
        NamespaceDetails::IndexIterator i = d->ii();
        while( i.more() )
            i.next().keyPattern().getFieldNames(_indexKeys);
        _keysComputed = true;
    }

    // TODO: All global functions manipulating namespaces should be static in NamespaceDetails

    bool userCreateNS(const char *ns, BSONObj options, string& err, bool logForReplication) {
        const char *coll = strchr( ns, '.' ) + 1;
        massert( 16451 ,  str::stream() << "invalid ns: " << ns , NamespaceString::validCollectionName(ns));
        char cl[ 256 ];
        nsToDatabase( ns, cl );
        if (nsdetails(ns) != NULL) {
            // Namespace already exists
            return false;
        } else {
            // This creates the namespace as well as its _id index
            nsdetails_maybe_create(ns, options);
            if ( logForReplication ) {
                if ( options.getField( "create" ).eoo() ) {
                    BSONObjBuilder b;
                    b << "create" << coll;
                    b.appendElements( options );
                    options = b.obj();
                }
                string logNs = string( cl ) + ".$cmd";
                OpLogHelpers::logCommand(logNs.c_str(), options, &cc().txn());
            }
            // TODO: Identify error paths for this function
            return true;
        }
    }

    void dropDatabase(const string &name) {
        tokulog(1) << "dropDatabase " << name << endl;
        Lock::assertWriteLocked(name);
        Database *d = cc().database();
        verify(d != NULL);
        verify(d->name == name);

        //BackgroundOperation::assertNoBgOpInProgForNs(name.c_str());

        // Not sure we need this here, so removed.  If we do, we need to move it down 
        // within other calls both (1) as they could be called from elsewhere and 
        // (2) to keep the lock order right - groupcommitmutex must be locked before 
        // mmmutex (if both are locked).
        //
        //  RWLockRecursive::Exclusive lk(MongoFile::mmmutex);

        d->namespaceIndex.drop();
        Database::closeDatabase(d->name.c_str(), d->path);
    }

    void dropCollection(const string &name, string &errmsg, BSONObjBuilder &result, bool can_drop_system) {
        tokulog(1) << "dropCollection " << name << endl;
        const char *ns = name.c_str();
        NamespaceDetails *d = nsdetails(ns);
        if (d == NULL) {
            return;
        }

        // Check that we are allowed to drop the namespace.
        NamespaceString s(name);
        verify(s.db == cc().database()->name);
        if (s.isSystem()) {
            if (s.coll == "system.profile") {
                uassert(10087, "turn off profiling before dropping system.profile collection", cc().database()->profile == 0);
            } else if (!can_drop_system) {
                uasserted(12502, "can't drop system ns");
            }
        }

        // Invalidate cursors and query cache, then drop all of the indexes.
        ClientCursor::invalidate(ns);
        NamespaceDetailsTransient::eraseForPrefix(ns);

        log(1) << "\t dropIndexes done" << endl;
        d->dropIndexes(ns, "*", errmsg, result, true);
        verify(d->nIndexes() == 0);
        removeNamespaceFromCatalog(name);

        // If everything succeeds, kill the namespace from the nsindex.
        Top::global.collectionDropped(name);
        nsindex(ns)->kill_ns(ns);
        result.append("ns", name);
    }

    /* add a new namespace to the system catalog (<dbname>.system.namespaces).
       options: { capped : ..., size : ... }
    */
    void addNewNamespaceToCatalog(const string &ns, const BSONObj *options) {
        LOG(1) << "New namespace: " << ns << endl;
        if (mongoutils::str::contains(ns, ".system.namespaces") ) {
            // system.namespaces holds all the others, so it is not explicitly listed in the catalog.
            return;
        }

        BSONObjBuilder b;
        b.append("name", ns);
        if ( options ) {
            b.append("options", *options);
        }
        BSONObj info = b.done();

        char database[256];
        nsToDatabase(ns.c_str(), database);
        string s = string(database) + ".system.namespaces";
        const char *system_ns = s.c_str();
        NamespaceDetails *d = nsdetails_maybe_create(system_ns);
        NamespaceDetailsTransient *nsdt = &NamespaceDetailsTransient::get(system_ns);
        insertOneObject(d, nsdt, info, false);
    }

    void removeNamespaceFromCatalog(const string &ns) {
        if (!mongoutils::str::contains(ns, ".system.namespaces")) {
            string system_namespaces = cc().database()->name + ".system.namespaces";
            _deleteObjects(system_namespaces.c_str(), BSON("name" << ns), false, false);
        }
    }

    int removeFromSysIndexes(const char *ns, const char *name) {
        string system_indexes = cc().database()->name + ".system.indexes";
        BSONObj obj = BSON("ns" << ns << "name" << name);
        tokulog(2) << "removeFromSysIndexes removing " << obj << endl;
        return (int) _deleteObjects(system_indexes.c_str(), obj, false, false);
    }

    static BSONObj replaceNSField(const BSONObj &obj, const char *to) {
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

    void renameNamespace(const char *from, const char *to, bool stayTemp) {
        class Rollback {
        public:
            Rollback(const char *to) :
                _to(to), _success(false) {
            }
            ~Rollback() {
                if (!_success && nsdetails(_to) != NULL) {
                    nsindex(_to)->kill_ns(_to);
                }
            }
            void success() {
                _success = true;
            }
        private:
            const char *_to;
            bool _success;
        };

        Lock::assertWriteLocked(from);
        verify( nsdetails(from) != NULL );
        verify( nsdetails(to) == NULL );

        // Invalidate any existing cursors on the old namespace details,
        // and reset the query cache.
        ClientCursor::invalidate( from );
        NamespaceDetailsTransient::eraseForPrefix( from );

        char database[MaxDatabaseNameLen];
        nsToDatabase(from, database);
        string sysIndexes = string(database) + ".system.indexes";
        string sysNamespaces = string(database) + ".system.namespaces";

        Rollback rollback(to);

        // Grab the serialized form of the namespace, and then close it.
        // This will close the underlying dictionaries and allow us to
        // rename them in the environment.
        BSONObj serialized = nsdetails(from)->serialize();
        nsindex(from)->close_ns(from);

        // Rename each index in system.indexes and system.namespaces
        {
            BSONObj oldIndexSpec;
            while ( Helpers::findOne( sysIndexes.c_str(), BSON( "ns" << from ), oldIndexSpec ) ) {
                string idxName = oldIndexSpec["name"].String();
                string oldIdxNS = IndexDetails::indexNamespace(from, idxName);
                string newIdxNS = IndexDetails::indexNamespace(to, idxName);

                tokulog(1) << "renaming " << oldIdxNS << " to " << newIdxNS << endl;
                storage::db_rename(oldIdxNS, newIdxNS);

                BSONObj newIndexSpec = replaceNSField( oldIndexSpec, to );
                addIndexToCatalog( newIndexSpec );
                addNewNamespaceToCatalog( newIdxNS, newIndexSpec.isEmpty() ? 0 : &newIndexSpec );
                _deleteObjects( sysNamespaces.c_str(), BSON( "name" << oldIdxNS ), false, false );
            }
            // Clean out the old entries from system.indexes. We already removed them
            // from system.namespaces in the loop above.
            _deleteObjects( sysIndexes.c_str(), BSON( "ns" << from ), false, false);
            verify ( !Helpers::findOne( sysIndexes.c_str(), BSON( "ns" << from ), oldIndexSpec ) );
        }

        // Rename the namespace in system.namespaces
        BSONObj newSpec;
        {
            BSONObj oldSpec;
            verify( Helpers::findOne( sysNamespaces.c_str(), BSON( "name" << from ), oldSpec ) );
            BSONObjBuilder b;
            BSONObjIterator i( oldSpec.getObjectField( "options" ) );
            while ( i.more() ) {
                BSONElement e = i.next();
                if ( strcmp( e.fieldName(), "create" ) != 0 ) {
                    if (stayTemp || (strcmp(e.fieldName(), "temp") != 0)) {
                        b.append( e );
                    }
                }
                else {
                    b << "create" << to;
                }
            }
            newSpec = b.obj();
            addNewNamespaceToCatalog( to, newSpec.isEmpty() ? 0 : &newSpec );
            _deleteObjects( sysNamespaces.c_str(), BSON( "name" << from ), false, false );
        }

        // Update the namespace index
        {
            NamespaceIndex *ni = nsindex( from );
            // Kill the old entry. Create a new serialized form of this namespace
            // using the new name, modified spec, and modified indexes array. This
            // will let us make a NamespaceDetails object and put it in the nsindex.
            ni->kill_ns( from );
            BSONArrayBuilder newIndexesArray;
            vector<BSONElement> indexes = serialized["indexes"].Array();
            for (vector<BSONElement>::const_iterator it = indexes.begin(); it != indexes.end(); it++) {
                newIndexesArray.append( replaceNSField( it->Obj(), to ) );
            }
            BSONObj newSerialized = NamespaceDetails::serialize( to, newSpec, serialized["pk"].Obj(),
                                                                 serialized["multiKeyIndexBits"].Long(),
                                                                 newIndexesArray.arr());
            ni->update_ns( to, newSerialized, false );
            ni->add_ns( to, shared_ptr<NamespaceDetails>() );
            verify( nsdetails(from) == NULL );
        }

        rollback.success();
    }

    bool legalClientSystemNS( const string& ns , bool write ) {
        if( ns == "local.system.replset" ) return true;

        if ( ns.find( ".system.users" ) != string::npos )
            return true;

        if ( ns.find( ".system.js" ) != string::npos ) {
            if ( write )
                Scope::storedFuncMod();
            return true;
        }

        return false;
    }

} // namespace mongo
