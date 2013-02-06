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

#include <boost/filesystem/operations.hpp>

#include "mongo/db/btree.h"
#include "mongo/db/db.h"
#include "mongo/db/json.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/update.h"
#include "mongo/scripting/engine.h"
#include "mongo/util/hashtab.h"

namespace mongo {

    BSONObj idKeyPattern = fromjson("{\"_id\":1}");

    //NamespaceDetails::NamespaceDetails( const DiskLoc &loc, bool capped )
    NamespaceDetails::NamespaceDetails( bool capped ) {

        /* be sure to initialize new fields here -- doesn't default to zeroes the way we use it */
        //firstExtent = lastExtent = capExtent = loc;
        //stats.datasize = stats.nrecords = 0;
        //lastExtentSize = 0;
        nIndexes = 0;
        _isCapped = capped;
        _maxDocsInCapped = 0x7fffffff;
        _systemFlags = 0;
        _userFlags = 0;
#if 0
        capFirstNewRecord = DiskLoc();
        // Signal that we are on first allocation iteration through extents.
        capFirstNewRecord.setInvalid();
        // For capped case, signal that we are doing initial extent allocation.
#endif
            
        if ( capped )
            ::abort(); //cappedLastDelRecLastExtent().setInvalid(); TODO: Capped collections will need to be re-done in TokuDB
        verify( sizeof(dataFileVersion) == 2 );
        dataFileVersion = 0;
        indexFileVersion = 0;
        multiKeyIndexBits = 0;
        reservedA = 0;
        indexBuildInProgress = 0;
        memset(reserved, 0, sizeof(reserved));
    }

    bool NamespaceIndex::exists() const {
        return !boost::filesystem::exists(path());
    }

    boost::filesystem::path NamespaceIndex::path() const {
        boost::filesystem::path ret( dir_ );
#if 0
        if ( directoryperdb )
            ret /= database_;
#endif
        ret /= ( database_ + ".ns" );
        return ret;
    }

    void NamespaceIndex::maybeMkdir() const {
        // Directory per db is always false, so this should do nothing
#if 0
        if ( !directoryperdb )
            return;
        boost::filesystem::path dir( dir_ );
        dir /= database_;
        if ( !boost::filesystem::exists( dir ) )
            MONGO_ASSERT_ON_EXCEPTION_WITH_MSG( boost::filesystem::create_directory( dir ), "create dir for db " );
#endif
    }

    unsigned lenForNewNsFiles = 16 * 1024 * 1024;

    void NamespaceDetails::onLoad(const Namespace& k) {
        if( indexBuildInProgress ) {
            verify( Lock::isW() ); // TODO(erh) should this be per db?
            if( indexBuildInProgress ) {
                log() << "indexBuildInProgress was " << indexBuildInProgress << " for " << k << ", indicating an abnormal db shutdown" << endl;
                indexBuildInProgress = 0; // TODO: Transactional //getDur().writingInt( indexBuildInProgress ) = 0;
            }
        }
    }

#if 0
    static void namespaceOnLoadCallback(const Namespace& k, NamespaceDetails& v) {
        v.onLoad(k);
    }
#endif

    bool checkNsFilesOnLoad = true;

    NOINLINE_DECL void NamespaceIndex::_init() {
        verify( !ht );

        Lock::assertWriteLocked(database_);

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

#if 0
        unsigned long long len = 0;
        boost::filesystem::path nsPath = path();
        string pathString = nsPath.string();
        void *p = 0;
        if( boost::filesystem::exists(nsPath) ) {
            if( f.open(pathString, true) ) {
                len = f.length();
                if ( len % (1024*1024) != 0 ) {
                    log() << "bad .ns file: " << pathString << endl;
                    uassert( 10079 ,  "bad .ns file length, cannot open database", len % (1024*1024) == 0 );
                }
                p = f.getView();
            }
        }
        else {
            // use lenForNewNsFiles, we are making a new database
            massert( 10343, "bad lenForNewNsFiles", lenForNewNsFiles >= 1024*1024 );
            maybeMkdir();
            unsigned long long l = lenForNewNsFiles;
            if( f.create(pathString, l, true) ) {
                // TODO: Log a new file create?
                //getDur().createdFile(pathString, l); // always a new file
                len = l;
                verify( len == lenForNewNsFiles );
                p = f.getView();
            }
        }

        if ( p == 0 ) {
            /** TODO: this shouldn't terminate? */
            log() << "error couldn't open file " << pathString << " terminating" << endl;
            dbexit( EXIT_FS );
        }


        verify( len <= 0x7fffffff );
        ht = new HashTable<Namespace,NamespaceDetails>(p, (int) len, "namespace index");
        if( checkNsFilesOnLoad )
            ht->iterAll(namespaceOnLoadCallback);
#endif
        ::abort();
    }

    static void namespaceGetNamespacesCallback( const Namespace& k , NamespaceDetails& v , void * extra ) {
        list<string> * l = (list<string>*)extra;
        if ( ! k.hasDollarSign() )
            l->push_back( (string)k );
    }
    void NamespaceIndex::getNamespaces( list<string>& tofill , bool onlyCollections ) const {
        verify( onlyCollections ); // TODO: need to implement this
        //                                  need boost::bind or something to make this less ugly

        if ( ht )
            ht->iterAll( namespaceGetNamespacesCallback , (void*)&tofill );
    }

    void NamespaceIndex::kill_ns(const char *ns) {
        Lock::assertWriteLocked(ns);
        if ( !ht )
            return;
        Namespace n(ns);
        ht->kill(n);

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

    void NamespaceIndex::add_ns( const char *ns, const NamespaceDetails &details ) {
        Lock::assertWriteLocked(ns);
        init();
        Namespace n(ns);
        uassert( 10081 , "too many namespaces/collections", ht->put(n, details));
    }

    void NamespaceDetails::setIndexIsMultikey(const char *thisns, int i) {
        dassert( i < NIndexesMax );
        unsigned long long x = ((unsigned long long) 1) << i;
        if( multiKeyIndexBits & x ) return;
        multiKeyIndexBits |= x; //*getDur().writing(&multiKeyIndexBits) |= x;
        NamespaceDetailsTransient::get(thisns).clearQueryCache();
    }

    /* you MUST call when adding an index.  see pdfile.cpp */
    IndexDetails& NamespaceDetails::addIndex(const char *thisns, bool resetTransient) {
        IndexDetails *id;
        try {
            id = &idx(nIndexes,true);
        }
        catch(DBException&) {
            //allocExtra(thisns, nIndexes);
            //id = &idx(nIndexes,false);
            ::abort(); // TODO: TokuDB: what to do?
        }

        nIndexes++; //(*getDur().writing(&nIndexes))++;
        if ( resetTransient )
            NamespaceDetailsTransient::get(thisns).addedIndex();
        return *id;
    }

    /* returns index of the first index in which the field is present. -1 if not present.
       (aug08 - this method not currently used)
    */
    int NamespaceDetails::fieldIsIndexed(const char *fieldName) {
        massert( 10346 , "not implemented", false);
        /*
        for ( int i = 0; i < nIndexes; i++ ) {
            IndexDetails& idx = indexes[i];
            BSONObj idxKey = idx.info.obj().getObjectField("key"); // e.g., { ts : -1 }
            if ( !idxKey.getField(fieldName).eoo() )
                return i;
        }*/
        return -1;
    }

    void NamespaceDetails::setMaxCappedDocs( long long max ) {
        verify( max <= 0x7fffffffLL ); // TODO: this is temp
        _maxDocsInCapped = static_cast<int>(max);
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

    void NamespaceDetails::setSystemFlag( int flag ) {
        _systemFlags |= flag; // TODO: Transactional //getDur().writingInt(_systemFlags) |= flag;
    }

    void NamespaceDetails::clearSystemFlag( int flag ) {
        _systemFlags &= ~flag; // TODO: Transactional //getDur().writingInt(_systemFlags) &= ~flag;
    }
    
    /**
     * keeping things in sync this way is a bit of a hack
     * and the fact that we have to pass in ns again
     * should be changed, just not sure to what
     */
    void NamespaceDetails::syncUserFlags( const string& ns ) {
        Lock::assertWriteLocked( ns );
        
        string system_namespaces = NamespaceString( ns ).db + ".system.namespaces";

        BSONObj oldEntry;
        // TODO: Do we need to verify this?
        //verify( Helpers::findOne( system_namespaces , BSON( "name" << ns ) , oldEntry ) );
        BSONObj newEntry = applyUpdateOperators( oldEntry , BSON( "$set" << BSON( "options.flags" << userFlags() ) ) );
        
        verify( 1 == deleteObjects( system_namespaces.c_str() , oldEntry , true , false , true ) );
        //theDataFileMgr.insert( system_namespaces.c_str() , newEntry.objdata() , newEntry.objsize() , true );
        ::abort();
    }

    bool NamespaceDetails::setUserFlag( int flags ) {
        if ( ( _userFlags & flags ) == flags )
            return false;
        
        _userFlags |= flags; // TODO: Transactional // getDur().writingInt(_userFlags) |= flags;
        return true;
    }

    bool NamespaceDetails::clearUserFlag( int flags ) {
        if ( ( _userFlags & flags ) == 0 )
            return false;

        _userFlags &= ~flags; // TODO: Transactional //getDur().writingInt(_userFlags) &= ~flags;
        return true;
    }

    bool NamespaceDetails::replaceUserFlags( int flags ) {
        if ( flags == _userFlags )
            return false;

        _userFlags = flags; // TODO: Transactional //getDur().writingInt(_userFlags) = flags;
        return true;
    }

    /* ------------------------------------------------------------------------- */

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
