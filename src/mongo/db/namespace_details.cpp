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

#include "mongo/db/namespace_details.h"

#include <algorithm>
#include <list>
#include <map>
#include <vector>
#include <utility>

#include <boost/filesystem/operations.hpp>

#include "mongo/db/indexcursor.h"
#include "mongo/db/db.h"
#include "mongo/db/json.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/txn.h"
#include "mongo/scripting/engine.h"

namespace mongo {

    BSONObj idKeyPattern = fromjson("{\"_id\":1}");

    // Clone of DBClientWithCommands::genIndexName(), should be factored out somewhere.
    static string genIndexName(const string &ns, const BSONObj &keys) {
        stringstream ss;
        ss << ns;

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

    static BSONObj id_index_info(const string &ns) {
        BSONObjBuilder id_info;
        id_info.append("ns", ns);
        id_info.append("key", idKeyPattern);
        id_info.append("name", genIndexName(ns, idKeyPattern));
        id_info.appendBool("unique", true);
        // TODO: Is this right?
        id_info.appendBool("background", false);
        return id_info.obj();
    }

    NamespaceDetails::NamespaceDetails(const string &ns, bool capped) : indexBuildInProgress(false), _nIndexes(0), multiKeyIndexBits(0) {
        tokulog() << "create NamespaceDetails for " << ns << endl;
        if ( capped ) {
            unimplemented("capped collections"); //cappedLastDelRecLastExtent().setInvalid(); TODO: Capped collections will need to be re-done in TokuDB
        }

        shared_ptr<IndexDetails> id_index(new IndexDetails(id_index_info(ns)));
        _indexes.push_back(id_index);
        _nIndexes++;
    }

    NamespaceDetails::NamespaceDetails(const BSONObj &serialized) :
        indexBuildInProgress(false),
        _nIndexes(serialized["indexes"].Array().size()),
        multiKeyIndexBits(serialized["multiKeyIndexBits"].Number()) {
        std::vector<BSONElement> index_array = serialized["indexes"].Array();
        for (std::vector<BSONElement>::iterator it = index_array.begin(); it != index_array.end(); it++) {
            shared_ptr<IndexDetails> idx(new IndexDetails(it->Obj()));
            _indexes.push_back(idx);
        }
        tokulog() << "load NamespaceDetails for " << _indexes[0]->toString() << endl;
    }

    NamespaceDetails::~NamespaceDetails() {
        tokulog() << "closing NamespaceDetails for " << _indexes[0]->toString() << endl;
    }

    BSONObj NamespaceDetails::serialize() const {
        BSONArrayBuilder indexes_array;
        typedef std::vector<shared_ptr<IndexDetails> >::const_iterator index_iterator_t;
        for (index_iterator_t it = _indexes.begin(); it != _indexes.end(); it++) {
            shared_ptr<IndexDetails> index(*it);
            indexes_array.append(index->info());
        }
        BSONObjBuilder s;
        s.appendIntOrLL("multiKeyIndexBits", multiKeyIndexBits);
        s.append("indexes", indexes_array.arr());
        return s.obj();
    }

    NamespaceIndex::~NamespaceIndex() {
        tokulog() << "closing NamespaceIndex (" << this << ") for " << database_ << endl;
        tokulog() << "nsdb is " << nsdb << endl;
        if (nsdb != NULL) {
            dassert(namespaces.get() != NULL);
            storage::db_close(nsdb);
            tokulog() << "destroying namespace map" << endl;
            namespaces.reset(NULL);
            tokulog() << "destroyed namespace map" << endl;
            // namespaces gets destructed automatically
        } else {
            tokulog() << "nsdb was null, nothing to do!" << endl;
            dassert(namespaces.get() == NULL);
        }
        tokulog() << "done closing NamespaceIndex" << endl;
    }

    bool NamespaceIndex::exists() const {
        return !boost::filesystem::exists(path());
    }

    boost::filesystem::path NamespaceIndex::path() const {
        boost::filesystem::path ret( dir_ );
        ret /= ( database_ + ".ns" );
        return ret;
    }

    static int populate_nsindex_map(const DBT *key, const DBT *val, void *map_v) {
        Namespace n(static_cast<const char *>(key->data));
        BSONObj obj(static_cast<const char *>(val->data));
        shared_ptr<NamespaceDetails> d(new NamespaceDetails(obj));
        tokulog() << "found namespace " << n << endl;

        NamespaceIndex::NamespaceDetailsMap *m = static_cast<NamespaceIndex::NamespaceDetailsMap *>(map_v);
        std::pair<NamespaceIndex::NamespaceDetailsMap::iterator, bool> ret;
        ret = m->insert(make_pair(n, d));
        dassert(ret.second == true);
        return 0;
    }

    NOINLINE_DECL void NamespaceIndex::_init() {
        int r;

        Lock::assertWriteLocked(database_);

        string nsdbname(database_ + ".ns");
        dassert(nsdb == NULL);
        nsdb = storage::db_open(cc().transaction(), nsdbname.c_str());
        dassert(nsdb != NULL);

        verify(namespaces.get() == NULL);
        namespaces.reset(new NamespaceDetailsMap());
        dassert(namespaces.get() != NULL);

        tokulog() << "initializing NamespaceIndex (" << this << ") for " << database_ << endl;
        tokulog() << "nsdb is " << nsdb << endl;
        {
            Client::Transaction scan_txn;
            DBC *cursor;
            r = nsdb->cursor(nsdb, scan_txn.txn(), &cursor, 0);
            verify(r == 0);

            while (r != DB_NOTFOUND) {
                r = cursor->c_getf_next(cursor, 0, populate_nsindex_map, namespaces.get());
                verify(r == 0 || r == DB_NOTFOUND);
            }
            verify(r == DB_NOTFOUND);

            r = cursor->c_close(cursor);
            verify(r == 0);
            scan_txn.commit();
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
        dassert(it != namespaces->end());
        namespaces->erase(it);

        ::abort(); // TODO: TokuDB: Understand how ht->kill(extra) works
#if 0
        for( int i = 0; i<=1; i++ ) {
            try {
                Namespace extra(n.extraName(i).c_str());
                ht->kill(extra);
            }
            catch(DBException&) { 
                dlog(3) << "caught exception in kill_ns" << endl;
            }
        }
#endif
    }

    void NamespaceIndex::add_ns(const char *ns, shared_ptr<NamespaceDetails> details) {
        Lock::assertWriteLocked(ns);

        init();
        Namespace n(ns);

        DBT ndbt, ddbt;
        ndbt.data = const_cast<void *>(static_cast<const void *>(ns));
        ndbt.size = strlen(ns) + 1;
        BSONObj serialized = details->serialize();
        ddbt.data = const_cast<void *>(static_cast<const void *>(serialized.objdata()));
        ddbt.size = serialized.objsize();
        int r = nsdb->put(nsdb, cc().transaction().txn(), &ndbt, &ddbt, 0);
        verify(r == 0);

        std::pair<NamespaceDetailsMap::iterator, bool> ret;
        ret = namespaces->insert(make_pair(n, details));
        dassert(ret.second == true);
    }

    void NamespaceDetails::setIndexIsMultikey(const char *thisns, int i) {
        dassert( i < NIndexesMax );
        unsigned long long x = ((unsigned long long) 1) << i;
        if( multiKeyIndexBits & x ) return;
        unimplemented("NamespaceDetails durability");
        multiKeyIndexBits |= x; //*getDur().writing(&multiKeyIndexBits) |= x;
        NamespaceDetailsTransient::get(thisns).clearQueryCache();
    }

    /* you MUST call when adding an index.  see pdfile.cpp */
    IndexDetails& NamespaceDetails::addIndex(const char *thisns, bool resetTransient) {
        IndexDetails *id;
        try {
            id = &idx(nIndexes() ,true);
        }
        catch(DBException&) {
            //allocExtra(thisns, nIndexes);
            //id = &idx(nIndexes,false);
            ::abort(); // TODO: TokuDB: what to do?
        }

        unimplemented("NamespaceDetails durability");
        _nIndexes++; //(*getDur().writing(&nIndexes))++;
        if ( resetTransient )
            NamespaceDetailsTransient::get(thisns).addedIndex();
        return *id;
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

    /* add a new namespace to the system catalog (<dbname>.system.namespaces).
       options: { capped : ..., size : ... }
    */
    void addNewNamespaceToCatalog(const char *ns, const BSONObj *options = 0) {
        LOG(1) << "New namespace: " << ns << endl;
        if ( strstr(ns, "system.namespaces") ) {
            // system.namespaces holds all the others, so it is not explicitly listed in the catalog.
            // TODO: fix above should not be strstr!
            return;
        }
        
        BSONObjBuilder b;
        b.append("name", ns);
        if ( options )
            b.append("options", *options);
        BSONObj j = b.done();
        char database[256];
        nsToDatabase(ns, database);
        string s = string(database) + ".system.namespaces";
        //theDataFileMgr.insert(s.c_str(), j.objdata(), j.objsize(), true);
        ::abort();
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
            DiskLoc newIndexSpecLoc = minDiskLoc; ::abort(); //theDataFileMgr.insert( s.c_str(), newIndexSpec.objdata(), newIndexSpec.objsize(), true, false );
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
