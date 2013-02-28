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
#include "mongo/db/idgen.h"
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
#include "mongo/scripting/engine.h"

namespace mongo {

    BSONObj idKeyPattern = fromjson("{\"_id\":1}");

    // Clone of DBClientWithCommands::genIndexName(), should be factored out somewhere.
    static string genIndexName(const BSONObj &keys) {
        stringstream ss;

        bool first = 1;
        for ( BSONObjIterator i(keys); i.more(); ) {
            BSONElement f = i.next();

            if ( first )
                first = 0;
            else
                ss << "_";

            ss << f.fieldName() << "_";
            if( f.isNumber() )
                ss << f.numberInt();
            else
                ss << f.str(); //this should match up with shell command
        }
        return ss.str();
    }

    static BSONObj id_index_info(const string &ns, const BSONObj &options) {
        BSONObjBuilder id_info;
        id_info.append("ns", ns);
        id_info.append("key", idKeyPattern);
        id_info.append("name", genIndexName(idKeyPattern));
        id_info.appendBool("unique", true);

        // Choose which options are used for the _id index, manually. 
        BSONElement e;
        e = options["basementsize"];
        if (e.ok() && !e.isNull()) {
            id_info.append(e);
        }
        e = options["compression"];
        if (e.ok() && !e.isNull()) {
            id_info.append(e);
        }
        return id_info.obj();
    }

    NamespaceDetails::NamespaceDetails(const string &ns, const BSONObj &options, bool capped) : indexBuildInProgress(false), _nIndexes(0), multiKeyIndexBits(0) {
        if ( capped ) {
            unimplemented("capped collections"); //cappedLastDelRecLastExtent().setInvalid(); TODO: Capped collections will need to be re-done in TokuDB
        }

        tokulog(1) << "Creating NamespaceDetails " << ns << endl;
        BSONObj id_info = id_index_info(ns, options);
        createIndex(id_info, true);

        int i = findIdIndex();
        verify(i == 0);

        addNewNamespaceToCatalog(ns);
    }

    NamespaceDetails::NamespaceDetails(const BSONObj &serialized) :
        indexBuildInProgress(false),
        _nIndexes(serialized["indexes"].Array().size()),
        multiKeyIndexBits(static_cast<uint64_t>(serialized["multiKeyIndexBits"].Long())) {
        std::vector<BSONElement> index_array = serialized["indexes"].Array();
        for (std::vector<BSONElement>::iterator it = index_array.begin(); it != index_array.end(); it++) {
            shared_ptr<IndexDetails> idx(new IndexDetails(it->Obj(), false));
            _indexes.push_back(idx);
        }
    }

    NamespaceDetails::~NamespaceDetails() {
    }

    BSONObj NamespaceDetails::serialize() const {
        BSONArrayBuilder indexes_array;
        for (IndexVector::const_iterator it = _indexes.begin(); it != _indexes.end(); it++) {
            IndexDetails *index = it->get();
            indexes_array.append(index->info());
        }
        return BSON("multiKeyIndexBits" << static_cast<long long>(multiKeyIndexBits) <<
                    "indexes" << indexes_array.arr());
    }

    NamespaceIndex::~NamespaceIndex() {
        if (nsdb != NULL) {
            tokulog(1) << "Closing NamespaceIndex " << database_ << endl;
            storage::db_close(nsdb);
            dassert(namespaces.get() != NULL);
        } else {
            dassert(namespaces.get() == NULL);
        }
    }

    static int populate_nsindex_map(const DBT *key, const DBT *val, void *map_v) {
        BSONObj nobj(static_cast<const char *>(key->data));
        string ns = nobj["ns"].String();
        Namespace n(ns.c_str());
        BSONObj dobj(static_cast<const char *>(val->data));
        tokulog(1) << "Loading NamespaceDetails " << (string) n << endl;
        shared_ptr<NamespaceDetails> d(new NamespaceDetails(dobj));

        NamespaceIndex::NamespaceDetailsMap *m = static_cast<NamespaceIndex::NamespaceDetailsMap *>(map_v);
        std::pair<NamespaceIndex::NamespaceDetailsMap::iterator, bool> ret;
        ret = m->insert(make_pair(n, d));
        dassert(ret.second == true);
        return 0;
    }

    NOINLINE_DECL void NamespaceIndex::_init(bool may_create) {
        int r;

        Lock::assertWriteLocked(database_);
        verify(namespaces.get() == NULL);
        dassert(nsdb == NULL);

        string nsdbname(database_ + ".ns");
        r = storage::db_open(&nsdb, nsdbname, BSON("key" << fromjson("{\"ns\":1}" )), may_create);
        if (r == ENOENT) {
            // didn't find on disk
            dassert(!may_create);
            return;
        }
        verify(r == 0);

        namespaces.reset(new NamespaceDetailsMap());

        tokulog(1) << "Initializing NamespaceIndex " << database_ << endl;
        {
            const Client::Context::Transaction &txn = cc().getContext()->transaction();
            DBC *cursor;
            r = nsdb->cursor(nsdb, txn.txn(), &cursor, 0);
            verify(r == 0);

            while (r != DB_NOTFOUND) {
                r = cursor->c_getf_next(cursor, 0, populate_nsindex_map, namespaces.get());
                verify(r == 0 || r == DB_NOTFOUND);
            }

            r = cursor->c_close(cursor);
            verify(r == 0);
        }

        /* if someone manually deleted the datafiles for a database,
           we need to be sure to clear any cached info for the database in
           local.*.
        */
        /*
        if ( "local" != database_ ) {
            DBInfo i(database_.c_str());
            i.dbDropped();
        }
        */
    }

    void NamespaceIndex::getNamespaces( list<string>& tofill , bool onlyCollections ) const {
        verify( onlyCollections ); // TODO: need to implement this
        //                                  need boost::bind or something to make this less ugly

        if (namespaces.get() != NULL) {
            for (NamespaceDetailsMap::const_iterator it = namespaces->begin(); it != namespaces->end(); it++) {
                const Namespace &n = it->first;
                tofill.push_back((string) n);
            }
        }
    }

    void NamespaceIndex::kill_ns(const char *ns) {
        Lock::assertWriteLocked(ns);
        if (namespaces.get() == NULL) {
            return;
        }
        Namespace n(ns);
        NamespaceDetailsMap::iterator it = namespaces->find(n);
        verify(it != namespaces->end());
        BSONObj nsobj = BSON("ns" << ns);
        DBT ndbt;
        ndbt.data = const_cast<void *>(static_cast<const void *>(nsobj.objdata()));
        ndbt.size = nsobj.objsize();
        int r = nsdb->del(nsdb, cc().getContext()->transaction().txn(), &ndbt, DB_DELETE_ANY);
        verify(r == 0);

        // Should really only do this after the commit of the del.
        namespaces->erase(it);
    }

    void NamespaceIndex::add_ns(const char *ns, shared_ptr<NamespaceDetails> details) {
        Lock::assertWriteLocked(ns);

        init();
        Namespace n(ns);

        std::pair<NamespaceDetailsMap::iterator, bool> ret;
        ret = namespaces->insert(make_pair(n, details));
        dassert(ret.second == true);
    }

    void NamespaceIndex::update_ns(const char *ns, NamespaceDetails *details, bool overwrite) {
        Lock::assertWriteLocked(ns);
        dassert(namespaces.get() != NULL);

        BSONObj nsobj = BSON("ns" << ns);
        DBT ndbt, ddbt;
        ndbt.data = const_cast<void *>(static_cast<const void *>(nsobj.objdata()));
        ndbt.size = nsobj.objsize();
        BSONObj serialized = details->serialize();
        ddbt.data = const_cast<void *>(static_cast<const void *>(serialized.objdata()));
        ddbt.size = serialized.objsize();
        const int flags = overwrite ? 0 : DB_NOOVERWRITE;
        int r = nsdb->put(nsdb, cc().getContext()->transaction().txn(), &ndbt, &ddbt, flags);
        verify(r == 0);
    }

    void NamespaceIndex::drop() {
        if (!allocated()) {
            return;
        }
        string errmsg;
        BSONObjBuilder result;
        while (!namespaces->empty()) {
            NamespaceDetailsMap::iterator it = namespaces->begin();
            const Namespace &ns = it->first;
            dropCollection((string) ns, errmsg, result, true);
        }
        dassert(nsdb != NULL);
        storage::db_close(nsdb);
        nsdb = NULL;
        storage::db_remove(database_ + ".ns");
    }

    void NamespaceDetails::setIndexIsMultikey(const char *thisns, int i) {
        dassert(i < NIndexesMax);
        unsigned long long x = ((unsigned long long) 1) << i;
        if (multiKeyIndexBits & x) {
            return;
        }
        multiKeyIndexBits |= x;

        dassert(nsdetails(thisns) == this);
        nsindex(thisns)->update_ns(thisns, this, true);

        NamespaceDetailsTransient::get(thisns).clearQueryCache();
    }

    void NamespaceDetails::fillNewIndex(IndexDetails &newIndex) {
        const char *thisns = newIndex.parentNS().c_str();
        for (shared_ptr<Cursor> cursor(Helpers::findTableScan(thisns, BSONObj())); cursor->ok(); cursor->advance()) {
            newIndex.insert(cursor->current(), cursor->currPK(), false);
        }
    }

    void NamespaceDetails::createIndex(const BSONObj &idx_info, bool resetTransient) {
        shared_ptr<IndexDetails> index(new IndexDetails(idx_info));
        indexBuildInProgress = true;
        _indexes.push_back(index);
        fillNewIndex(*index);
        _nIndexes++;
        indexBuildInProgress = false;

        string idx_ns = idx_info["ns"].String();
        const char *ns = idx_ns.c_str();

        // The first index we create should be the _id index, when we first create the collection.
        // Therefore the collection's NamespaceDetails should not already exist in the NamespaceIndex.
        const bool may_overwrite = _nIndexes > 1;
        if (!may_overwrite) {
            massert(16435, "first index should be _id index", index->isIdIndex());
        } else {
            dassert(nsdetails(ns) == this);
        }
        nsindex(ns)->update_ns(ns, this, may_overwrite);

        if (resetTransient) {
            NamespaceDetailsTransient::get(ns).addedIndex();
        }
    }

    bool NamespaceDetails::dropIndexes(const char *ns, const char *name, string &errmsg, BSONObjBuilder &result, bool mayDeleteIdIndex, bool can_drop_system) {
        tokulog(1) << "dropIndexes " << name << endl;

        //BackgroundOperation::assertNoBgOpInProgForNs(ns);

        ClientCursor::invalidate(ns);

        if (mongoutils::str::equals(name, "*")) {
            result.append("nIndexesWas", (double) _nIndexes);
            // This is O(n^2), not great, but you can have at most 64 indexes anyway.
            for (IndexVector::iterator it = _indexes.begin(); it != _indexes.end(); ) {
                IndexDetails *idx = it->get();
                if (mayDeleteIdIndex || !idx->isIdIndex()) {
                    idx->kill_idx(can_drop_system);
                    it = _indexes.erase(it);
                    _nIndexes--;
                } else {
                    it++;
                }
            }
            // Assuming id index isn't multikey
            multiKeyIndexBits = 0;
            result.append("msg", (mayDeleteIdIndex
                                  ? "indexes dropped for collection"
                                  : "non-_id indexes dropped for collection"));
        } else {
            verify(!can_drop_system);
            int x = findIndexByName(name);
            if (x >= 0) {
                result.append("nIndexesWas", (double) _nIndexes);
                IndexVector::iterator it = _indexes.begin() + x;
                IndexDetails *idx = it->get();
                idx->kill_idx(can_drop_system);
                _indexes.erase(it);
                _nIndexes--;
                // Removes the nth bit, and shifts any bits higher than it down a slot.
                multiKeyIndexBits = ((multiKeyIndexBits & ((1ULL << x) - 1)) |
                                     ((multiKeyIndexBits >> (x + 1)) << x));
            } else {
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
        nsindex(ns)->update_ns(ns, this, true);
        return true;
    }

#if 0
    /* you MUST call when adding an index.  see pdfile.cpp */
    IndexDetails& NamespaceDetails::addIndex(const char *thisns, bool resetTransient) {
        IndexDetails *id;
        try {
            id = &idx(nIndexes(), true);
        }
        catch(DBException&) {
            allocExtra(thisns, nIndexes);
            id = &idx(nIndexes,false);
        }

        unimplemented("NamespaceDetails durability for nindexes, we currently don't store this because we don't yet have hot indexing, much less resume from shutdown during hot indexing");
        _nIndexes++; //(*getDur().writing(&nIndexes))++;
        return *id;
    }
#endif

    struct findByIdCallbackExtra {
        const BSONObj &key;
        BSONObj &obj;

        findByIdCallbackExtra(const BSONObj &k, BSONObj &o) : key(k), obj(o) { }
    };

    static int findByIdCallback(const DBT *key, const DBT *value, void *extra) {
        if (key != NULL) {
            struct findByIdCallbackExtra *info = reinterpret_cast<findByIdCallbackExtra *>(extra);
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

    bool NamespaceDetails::findById(const BSONObj &query, BSONObj &result, bool getKey) {
        int r;

        // Get a cursor over the _id index.
        IndexDetails &idIndex = idx(findIdIndex());
        DBC *cursor = idIndex.cursor();

        // create an index key
        BSONObj key = getKey ? idIndex.getKeyFromQuery(query) : query;
        DBT key_dbt;
        storage::dbt_init(&key_dbt, key.objdata(), key.objsize());

        // Try to find it.
        BSONObj obj = BSONObj();
        tokulog(3) << "NamespaceDetails::findById looking for " << key << endl;
        struct findByIdCallbackExtra extra(key, obj);
        r = cursor->c_getf_set(cursor, 0, &key_dbt, findByIdCallback, &extra);
        verify(r == 0 || r == DB_NOTFOUND);
        r = cursor->c_close(cursor);
        verify(r == 0);

        if (!obj.isEmpty()) {
            result = obj;
            return true;
        } else {
            return false;
        }
    }

    void NamespaceDetails::insertObject(const BSONObj &obj, bool overwrite) {
        if (overwrite && _indexes.size() > 1) {
            wunimplemented("overwrite inserts on secondary keys right now don't work");
            //uassert(16432, "can't do overwrite inserts when there are secondary keys yet", !overwrite || _indexes.size() == 1);
        }

        BSONObj primary_key;
        if (_indexes.size() > 1) {
            // Have secondary indexes, it's worth it to precompute the key
            IndexDetails &id_index = idx(findIdIndex());
            BSONObjSet keys;
            id_index.getKeysFromObject(obj, keys);
            dassert(keys.size() == 1);
            primary_key = *(keys.begin());
        }

        // TODO: use put_multiple API
        for (IndexVector::iterator it = _indexes.begin(); it != _indexes.end(); ++it) {
            IndexDetails *index = it->get();
            index->insert(obj, primary_key, overwrite);
        }
    }

    void NamespaceDetails::deleteObject(const BSONObj &pk, const BSONObj &obj) {
        for (IndexVector::iterator it = _indexes.begin(); it != _indexes.end(); ++it) {
            IndexDetails *index = it->get();
            index->deleteObject(pk, obj);
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
        Database *database = cc().database();
        verify( database );
        if( _nsdMap.size() % 20000 == 10000 ) { 
            // so we notice if insanely large #s
            log() << "opening namespace " << ns << endl;
            log() << _nsdMap.size() << " namespaces in nsdMap" << endl;
        }
        t.reset( new NamespaceDetailsTransient(database, ns) );
        return *t;
    }

    // note with repair there could be two databases with the same ns name.
    // that is NOT handled here yet!  TODO
    // repair may not use nsdt though not sure.  anyway, requires work.
    NamespaceDetailsTransient::NamespaceDetailsTransient(Database *db, const char *ns) : 
        _ns(ns), _keysComputed(false), _qcWriteCount() 
    {
        dassert(db);
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

    bool userCreateNS(const char *ns, BSONObj options, string& err, bool logForReplication) {
        const char *coll = strchr( ns, '.' ) + 1;
        massert( 10356 ,  str::stream() << "invalid ns: " << ns , NamespaceString::validCollectionName(ns));
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
                logOp("c", logNs.c_str(), options);
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

        //BackgroundOperation::assertNoBgOpInProgForNs(ns);

        d->dropIndexes(ns, "*", errmsg, result, true, can_drop_system);
        uassert(16445, "bug in dropCollection (nIndexes != 0), should crash but won't for now.", d->nIndexes() == 0);
        log(1) << "\t dropIndexes done" << endl;
        result.append("ns", name);
        ClientCursor::invalidate(ns);
        Top::global.collectionDropped(name);
        NamespaceDetailsTransient::eraseForPrefix(ns);
        dropNS(name, true, can_drop_system);
    }

    void NamespaceDetails::addIdIndexToCatalog() {
        int i = findIdIndex();
        verify(i >= 0);
        const BSONObj &info = idx(i).info();
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
        insertOneObject(d, nsdt, addIdField(info), false);
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
        if ( options )
            b.append("options", *options);
        BSONObj j = b.done();
        char database[256];
        nsToDatabase(ns.c_str(), database);
        string s = string(database) + ".system.namespaces";
        const char *system_ns = s.c_str();
        NamespaceDetails *d = nsdetails_maybe_create(system_ns);
        NamespaceDetailsTransient *nsdt = &NamespaceDetailsTransient::get(system_ns);
        insertOneObject(d, nsdt, addIdField(j), false);
    }

    void dropNS(const string &nsname, bool is_collection, bool can_drop_system) {
        const char *ns = nsname.c_str();
        if (is_collection) {
            NamespaceDetails *d = nsdetails(ns);
            uassert(10086, mongoutils::str::stream() << "ns not found: " + nsname, d);

            NamespaceString s(nsname);
            verify(s.db == cc().database()->name);
            if (s.isSystem()) {
                if (s.coll == "system.profile") {
                    uassert(10087, "turn off profiling before dropping system.profile collection", cc().database()->profile == 0);
                } else if (!can_drop_system) {
                    uasserted(12502, "can't drop system ns");
                }
            }
        }

        //BackgroundOperation::assertNoBgOpInProgForNs(ns);

        if (!mongoutils::str::contains(ns, ".system.namespaces")) {
            string system_namespaces = cc().database()->name + ".system.namespaces";
            _deleteObjects(system_namespaces.c_str(),
                           BSON("name" << nsname),
                           false, false);
        }

        if (is_collection) {
            nsindex(ns)->kill_ns(ns);
        }
    }

    void renameNamespace( const char *from, const char *to, bool stayTemp) {
        // TODO: TokuDB: Pay attention to the usage of the NamespaceIndex object.
        // That's still important. Anything to do with disklocs (ie: storage code)
        // is probably not.
        ::abort();
#if 0
        NamespaceIndex *ni = nsindex( from );
        verify( ni );
        verify( ni->details( from ) );
        verify( ! ni->details( to ) );

        // Our namespace and index details will move to a different
        // memory location.  The only references to namespace and
        // index details across commands are in cursors and nsd
        // transient (including query cache) so clear these.
        ClientCursor::invalidate( from );
        NamespaceDetailsTransient::eraseForPrefix( from );

        NamespaceDetails *details = ni->details( from );
        ni->add_ns( to, *details );
        NamespaceDetails *todetails = ni->details( to );
        try {
            todetails->copyingFrom(to, details); // fixes extraOffset
        }
        catch( DBException& ) {
            // could end up here if .ns is full - if so try to clean up / roll back a little
            ni->kill_ns(to);
            throw;
        }
        ni->kill_ns( from );
        details = todetails;

        BSONObj oldSpec;
        char database[MaxDatabaseNameLen];
        nsToDatabase(from, database);
        string s = database;
        s += ".system.namespaces";
        verify( Helpers::findOne( s.c_str(), BSON( "name" << from ), oldSpec ) );

        BSONObjBuilder newSpecB;
        BSONObjIterator i( oldSpec.getObjectField( "options" ) );
        while( i.more() ) {
            BSONElement e = i.next();
            if ( strcmp( e.fieldName(), "create" ) != 0 ) {
                if (stayTemp || (strcmp(e.fieldName(), "temp") != 0))
                    newSpecB.append( e );
            }
            else {
                newSpecB << "create" << to;
            }
        }
        BSONObj newSpec = newSpecB.done();
        addNewNamespaceToCatalog( to, newSpec.isEmpty() ? 0 : &newSpec );

        deleteObjects( s.c_str(), BSON( "name" << from ), false, false, true );
        // oldSpec variable no longer valid memory

        BSONObj oldIndexSpec;
        s = database;
        s += ".system.indexes";
        while( Helpers::findOne( s.c_str(), BSON( "ns" << from ), oldIndexSpec ) ) {
            BSONObjBuilder newIndexSpecB;
            BSONObjIterator i( oldIndexSpec );
            while( i.more() ) {
                BSONElement e = i.next();
                if ( strcmp( e.fieldName(), "ns" ) != 0 )
                    newIndexSpecB.append( e );
                else
                    newIndexSpecB << "ns" << to;
            }
            BSONObj newIndexSpec = newIndexSpecB.done();
            DiskLoc newIndexSpecLoc = theDataFileMgr.insert( s.c_str(), newIndexSpec.objdata(), newIndexSpec.objsize(), true, false );
            int indexI = details->findIndexByName( oldIndexSpec.getStringField( "name" ) );
            IndexDetails &indexDetails = details->idx(indexI);
            string oldIndexNs = indexDetails.indexNamespace();
            indexDetails.info = newIndexSpecLoc;
            string newIndexNs = indexDetails.indexNamespace();

            renameNamespace( oldIndexNs.c_str(), newIndexNs.c_str(), false );
            deleteObjects( s.c_str(), oldIndexSpec.getOwned(), true, false, true );
        }
#endif
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
