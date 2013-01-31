// pdfile.cpp

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

/*
todo:
_ table scans must be sequential, not next/prev pointers
_ coalesce deleted
_ disallow system* manipulations from the database.
*/

#include "pch.h"
#include "pdfile.h"
#include "db.h"
#include "../util/mmap.h"
#include "../util/hashtab.h"
#include "../util/file_allocator.h"
#include "../util/processinfo.h"
#include "../util/file.h"
#include "btree.h"
#include "btreebuilder.h"
#include <algorithm>
#include <list>
#include "repl.h"
#include "dbhelpers.h"
#include "namespace-inl.h"
#include "extsort.h"
#include "curop-inl.h"
#include "background.h"
#include "compact.h"
#include "ops/delete.h"
#include "instance.h"
#include "replutil.h"
#include "mongo/db/lasterror.h"

#include <boost/filesystem/operations.hpp>

namespace mongo {

    //BOOST_STATIC_ASSERT( sizeof(Extent)-4 == 48+128 );
    //BOOST_STATIC_ASSERT( sizeof(DataFileHeader)-4 == 8192 );

    void printMemInfo( const char * where ) {
        cout << "mem info: ";
        if ( where )
            cout << where << " ";
        ProcessInfo pi;
        if ( ! pi.supported() ) {
            cout << " not supported" << endl;
            return;
        }

        cout << "vsize: " << pi.getVirtualMemorySize() << " resident: " << pi.getResidentSize() << " mapped: " << ( MemoryMappedFile::totalMappedLength() / ( 1024 * 1024 ) ) << endl;
    }

    bool isValidNS( const StringData& ns ) {
        // TODO: should check for invalid characters

        const char * x = strchr( ns.data() , '.' );
        if ( ! x )
            return false;

        x++;
        return *x > 0;
    }

    // TODO SERVER-4328
    bool inDBRepair = false;
    struct doingRepair {
        doingRepair() {
            verify( ! inDBRepair );
            inDBRepair = true;
        }
        ~doingRepair() {
            inDBRepair = false;
        }
    };

    SimpleMutex BackgroundOperation::m("bg");
    map<string, unsigned> BackgroundOperation::dbsInProg;
    set<string> BackgroundOperation::nsInProg;

    bool BackgroundOperation::inProgForDb(const char *db) {
        SimpleMutex::scoped_lock lk(m);
        return dbsInProg[db] != 0;
    }

    bool BackgroundOperation::inProgForNs(const char *ns) {
        SimpleMutex::scoped_lock lk(m);
        return nsInProg.count(ns) != 0;
    }

    void BackgroundOperation::assertNoBgOpInProgForDb(const char *db) {
        uassert(12586, "cannot perform operation: a background operation is currently running for this database",
                !inProgForDb(db));
    }

    void BackgroundOperation::assertNoBgOpInProgForNs(const char *ns) {
        uassert(12587, "cannot perform operation: a background operation is currently running for this collection",
                !inProgForNs(ns));
    }

    BackgroundOperation::BackgroundOperation(const char *ns) : _ns(ns) {
        SimpleMutex::scoped_lock lk(m);
        dbsInProg[_ns.db]++;
        verify( nsInProg.count(_ns.ns()) == 0 );
        nsInProg.insert(_ns.ns());
    }

    BackgroundOperation::~BackgroundOperation() {
        SimpleMutex::scoped_lock lk(m);
        dbsInProg[_ns.db]--;
        nsInProg.erase(_ns.ns());
    }

    void BackgroundOperation::dump(stringstream& ss) {
        SimpleMutex::scoped_lock lk(m);
        if( nsInProg.size() ) {
            ss << "\n<b>Background Jobs in Progress</b>\n";
            for( set<string>::iterator i = nsInProg.begin(); i != nsInProg.end(); i++ )
                ss << "  " << *i << '\n';
        }
        for( map<string,unsigned>::iterator i = dbsInProg.begin(); i != dbsInProg.end(); i++ ) {
            if( i->second )
                ss << "database " << i->first << ": " << i->second << '\n';
        }
    }

    /* ----------------------------------------- */
#ifdef _WIN32
    string dbpath = "\\data\\db\\";
#else
    string dbpath = "/data/db/";
#endif
    const char FREELIST_NS[] = ".$freelist";
    bool directoryperdb = false;
    string repairpath;
    string pidfilepath;

    // TODO: Get rid of the data theDataFileMgr.
    //DataFileMgr theDataFileMgr;

    DatabaseHolder _dbHolder;
    int MAGIC = 0x1000;

    DatabaseHolder& dbHolderUnchecked() {
        return _dbHolder;
    }

    void addNewNamespaceToCatalog(const char *ns, const BSONObj *options = 0);
    void ensureIdIndexForNewNs(const char *ns) {
        if ( ( strstr( ns, ".system." ) == 0 || legalClientSystemNS( ns , false ) ) &&
                strstr( ns, FREELIST_NS ) == 0 ) {
            log( 1 ) << "adding _id index for collection " << ns << endl;
            //ensureHaveIdIndex( ns );
            ::abort();
        }
    }

    string getDbContext() {
        stringstream ss;
        Client * c = currentClient.get();
        if ( c ) {
            Client::Context * cx = c->getContext();
            if ( cx ) {
                Database *database = cx->db();
                if ( database ) {
                    ss << database->name << ' ';
                    ss << cx->ns() << ' ';
                }
            }
        }
        return ss.str();
    }

    /*---------------------------------------------------------------------*/

    // inheritable class to implement an operation that may be applied to all
    // files in a database using _applyOpToDataFiles()
    class FileOp {
    public:
        virtual ~FileOp() {}
        // Return true if file exists and operation successful
        virtual bool apply( const boost::filesystem::path &p ) = 0;
        virtual const char * op() const = 0;
    };

    void _applyOpToDataFiles( const char *database, FileOp &fo, bool afterAllocator = false, const string& path = dbpath );

    void _deleteDataFiles(const char *database) {
        if ( directoryperdb ) {
            FileAllocator::get()->waitUntilFinished();
            MONGO_ASSERT_ON_EXCEPTION_WITH_MSG( boost::filesystem::remove_all( boost::filesystem::path( dbpath ) / database ), "delete data files with a directoryperdb" );
            return;
        }
        class : public FileOp {
            virtual bool apply( const boost::filesystem::path &p ) {
                return boost::filesystem::remove( p );
            }
            virtual const char * op() const {
                return "remove";
            }
        } deleter;
        _applyOpToDataFiles( database, deleter, true );
    }

    void checkConfigNS(const char *ns) {
        if ( cmdLine.configsvr && 
             !( str::startsWith( ns, "config." ) || str::startsWith( ns, "admin." ) ) ) { 
            uasserted(14037, "can't create user databases on a --configsvr instance");
        }
    }

    bool _userCreateNS(const char *ns, const BSONObj& options, string& err, bool *deferIdIndex) {
        ::abort();
        return false;
#if 0
        log(1) << "create collection " << ns << ' ' << options << endl;

        if ( nsdetails(ns) ) {
            err = "collection already exists";
            return false;
        }

        checkConfigNS(ns);

        long long size = Extent::initialSize(128);
        {
            BSONElement e = options.getField("size");
            if ( e.isNumber() ) {
                size = e.numberLong();
                size += 256;
                size &= 0xffffffffffffff00LL;
            }
        }

        uassert( 10083 , "create collection invalid size spec", size > 0 );

        bool newCapped = false;
        long long mx = 0;
        if( options["capped"].trueValue() ) {
            newCapped = true;
            BSONElement e = options.getField("max");
            if ( e.isNumber() ) {
                mx = e.numberLong();
            }
        }

        // $nExtents just for debug/testing.
        BSONElement e = options.getField( "$nExtents" );
        Database *database = cc().database();
        if ( e.type() == Array ) {
            // We create one extent per array entry, with size specified
            // by the array value.
            BSONObjIterator i( e.embeddedObject() );
            while( i.more() ) {
                BSONElement e = i.next();
                int size = int( e.number() );
                verify( size <= 0x7fffffff );
                // $nExtents is just for testing - always allocate new extents
                // rather than reuse existing extents so we have some predictibility
                // in the extent size used by our tests
                ::abort();
                //database->suitableFile( ns, (int) size, false, false )->createExtent( ns, (int) size, newCapped );
            }
        }
        else if ( int( e.number() ) > 0 ) {
            // We create '$nExtents' extents, each of size 'size'.
            int nExtents = int( e.number() );
            verify( size <= 0x7fffffff );
            for ( int i = 0; i < nExtents; ++i ) {
                verify( size <= 0x7fffffff );
                // $nExtents is just for testing - always allocate new extents
                // rather than reuse existing extents so we have some predictibility
                // in the extent size used by our tests
                ::abort();
                //database->suitableFile( ns, (int) size, false, false )->createExtent( ns, (int) size, newCapped );
            }
        }
        else {
            // This is the non test case, where we don't have a $nExtents spec.
            while ( size > 0 ) {
                const int max = Extent::maxSize();
                const int min = Extent::minSize();
                int desiredExtentSize = static_cast<int> (size > max ? max : size);
                desiredExtentSize = static_cast<int> (desiredExtentSize < min ? min : desiredExtentSize);

                desiredExtentSize &= 0xffffff00;
                Extent *e = database->allocExtent( ns, desiredExtentSize, newCapped, true );
                size -= e->length;
            }
        }

        NamespaceDetails *d = nsdetails(ns);
        verify(d);
        
        bool ensure = true;

        // respect autoIndexId if set. otherwise, create an _id index for all colls, except for
        // capped ones in local w/o autoIndexID (reason for the exception is for the oplog and
        //  non-replicated capped colls)
        if( options.hasField( "autoIndexId" ) ||
            (newCapped && str::equals( nsToDatabase( ns ).c_str() ,  "local" )) ) {
            ensure = options.getField( "autoIndexId" ).trueValue();
        }

        if( ensure ) {
            if( deferIdIndex )
                *deferIdIndex = true;
            else
                ensureIdIndexForNewNs( ns );
        }
        
        if ( mx > 0 )
            d->setMaxCappedDocs( mx );

        bool isFreeList = strstr(ns, FREELIST_NS) != 0;
        if( !isFreeList )
            addNewNamespaceToCatalog(ns, options.isEmpty() ? 0 : &options);
        
        if ( options["flags"].numberInt() ) {
            d->replaceUserFlags( options["flags"].numberInt() );
        }

        return true;
#endif
    }

    /** { ..., capped: true, size: ..., max: ... }
        @param deferIdIndex - if not not, defers id index creation.  sets the bool value to true if we wanted to create the id index.
        @return true if successful
    */
    bool userCreateNS(const char *ns, BSONObj options, string& err, bool logForReplication, bool *deferIdIndex) {
        const char *coll = strchr( ns, '.' ) + 1;
        massert( 10356 ,  str::stream() << "invalid ns: " << ns , NamespaceString::validCollectionName(ns));
        char cl[ 256 ];
        nsToDatabase( ns, cl );
        bool ok = _userCreateNS(ns, options, err, deferIdIndex);
        if ( logForReplication && ok ) {
            if ( options.getField( "create" ).eoo() ) {
                BSONObjBuilder b;
                b << "create" << coll;
                b.appendElements( options );
                options = b.obj();
            }
            string logNs = string( cl ) + ".$cmd";
            logOp("c", logNs.c_str(), options);
        }
        return ok;
    }

    /*---------------------------------------------------------------------*/

    /* get a table scan cursor, but can be forward or reverse direction.
       order.$natural - if set, > 0 means forward (asc), < 0 backward (desc).
    */
    shared_ptr<Cursor> findTableScan(const char *ns, const BSONObj& order, const DiskLoc &startLoc) {
        BSONElement el = order.getField("$natural"); // e.g., { $natural : -1 }

        if ( el.number() >= 0 )
            ::abort(); //return DataFileMgr::findAll(ns, startLoc);
        ::abort();

        // "reverse natural order"
        //NamespaceDetails *d = nsdetails(ns);

        //if ( !d )
            return shared_ptr<Cursor>(new BasicCursor(DiskLoc()));

#if 0
        if ( !d->isCapped() ) {
            if ( !startLoc.isNull() )
                return shared_ptr<Cursor>(new ReverseCursor( startLoc ));
            Extent *e = d->lastExtent.ext();
            while ( e->lastRecord.isNull() && !e->xprev.isNull() ) {
                OCCASIONALLY out() << "  findTableScan: extent empty, skipping ahead" << endl;
                e = e->getPrevExtent();
            }
            return shared_ptr<Cursor>(new ReverseCursor( e->lastRecord ));
        }
        else {
            return shared_ptr<Cursor>( new ReverseCappedCursor( d, startLoc ) );
        }
#endif
    }

    /* drop a collection/namespace */
    void dropNS(const string& nsToDrop) {
        NamespaceDetails* d = nsdetails(nsToDrop.c_str());
        uassert( 10086 ,  (string)"ns not found: " + nsToDrop , d );

        BackgroundOperation::assertNoBgOpInProgForNs(nsToDrop.c_str());

        NamespaceString s(nsToDrop);
        verify( s.db == cc().database()->name );
        if( s.isSystem() ) {
            if( s.coll == "system.profile" )
                uassert( 10087 ,  "turn off profiling before dropping system.profile collection", cc().database()->profile == 0 );
            else
                uasserted( 12502, "can't drop system ns" );
        }

        {
            // remove from the system catalog
            BSONObj cond = BSON( "name" << nsToDrop );   // { name: "colltodropname" }
            string system_namespaces = cc().database()->name + ".system.namespaces";
            /*int n = */ deleteObjects(system_namespaces.c_str(), cond, false, false, true);
            // no check of return code as this ns won't exist for some of the new storage engines
        }

        // free extents
        if( !d->firstExtent.isNull() ) {
            ::abort();
            //freeExtents(d->firstExtent, d->lastExtent);
            //getDur().writingDiskLoc( d->firstExtent ).setInvalid();
            //getDur().writingDiskLoc( d->lastExtent ).setInvalid();
        }

        // remove from the catalog hashtable
        cc().database()->namespaceIndex.kill_ns(nsToDrop.c_str());
    }

    void dropCollection( const string &name, string &errmsg, BSONObjBuilder &result ) {
        log(1) << "dropCollection: " << name << endl;
        NamespaceDetails *d = nsdetails(name.c_str());
        if( d == 0 )
            return;

        BackgroundOperation::assertNoBgOpInProgForNs(name.c_str());

        if ( d->nIndexes != 0 ) {
            try {
                ::abort();
                //verify( dropIndexes(d, name.c_str(), "*", errmsg, result, true) );
            }
            catch( DBException& e ) {
                stringstream ss;
                ss << "drop: dropIndexes for collection failed - consider trying repair ";
                ss << " cause: " << e.what();
                uasserted(12503,ss.str());
            }
            verify( d->nIndexes == 0 );
        }
        log(1) << "\t dropIndexes done" << endl;
        result.append("ns", name.c_str());
        ClientCursor::invalidate(name.c_str());
        Top::global.collectionDropped( name );
        NamespaceDetailsTransient::eraseForPrefix( name.c_str() );
        dropNS(name);
    }

} // namespace mongo

#include "clientcursor.h"

namespace mongo {

    void dropAllDatabasesExceptLocal() {
        Lock::GlobalWrite lk;

        vector<string> n;
        getDatabaseNames(n);
        if( n.size() == 0 ) return;
        log() << "dropAllDatabasesExceptLocal " << n.size() << endl;
        for( vector<string>::iterator i = n.begin(); i != n.end(); i++ ) {
            if( *i != "local" ) {
                Client::Context ctx(*i);
                dropDatabase(*i);
            }
        }
    }

    void dropDatabase(string db) {
        log(1) << "dropDatabase " << db << endl;
        Lock::assertWriteLocked(db);
        Database *d = cc().database();
        verify( d );
        verify( d->name == db );

        BackgroundOperation::assertNoBgOpInProgForDb(d->name.c_str());

        // Not sure we need this here, so removed.  If we do, we need to move it down 
        // within other calls both (1) as they could be called from elsewhere and 
        // (2) to keep the lock order right - groupcommitmutex must be locked before 
        // mmmutex (if both are locked).
        //
        //  RWLockRecursive::Exclusive lk(MongoFile::mmmutex);

        // TODO: What does TokuDB do here?
        //getDur().syncDataAndTruncateJournal();

        Database::closeDatabase( d->name.c_str(), d->path );
        d = 0; // d is now deleted

        _deleteDataFiles( db.c_str() );
    }

    typedef boost::filesystem::path Path;

    void boostRenameWrapper( const Path &from, const Path &to ) {
        try {
            boost::filesystem::rename( from, to );
        }
        catch ( const boost::filesystem::filesystem_error & ) {
            // boost rename doesn't work across partitions
            boost::filesystem::copy_file( from, to);
            boost::filesystem::remove( from );
        }
    }

    // back up original database files to 'temp' dir
    void _renameForBackup( const char *database, const Path &reservedPath ) {
        Path newPath( reservedPath );
        if ( directoryperdb )
            newPath /= database;
        class Renamer : public FileOp {
        public:
            Renamer( const Path &newPath ) : newPath_( newPath ) {}
        private:
            const boost::filesystem::path &newPath_;
            virtual bool apply( const Path &p ) {
                if ( !boost::filesystem::exists( p ) )
                    return false;
                boostRenameWrapper( p, newPath_ / ( p.leaf() + ".bak" ) );
                return true;
            }
            virtual const char * op() const {
                return "renaming";
            }
        } renamer( newPath );
        _applyOpToDataFiles( database, renamer, true );
    }

    // move temp files to standard data dir
    void _replaceWithRecovered( const char *database, const char *reservedPathString ) {
        Path newPath( dbpath );
        if ( directoryperdb )
            newPath /= database;
        class Replacer : public FileOp {
        public:
            Replacer( const Path &newPath ) : newPath_( newPath ) {}
        private:
            const boost::filesystem::path &newPath_;
            virtual bool apply( const Path &p ) {
                if ( !boost::filesystem::exists( p ) )
                    return false;
                boostRenameWrapper( p, newPath_ / p.leaf() );
                return true;
            }
            virtual const char * op() const {
                return "renaming";
            }
        } replacer( newPath );
        _applyOpToDataFiles( database, replacer, true, reservedPathString );
    }

    // generate a directory name for storing temp data files
    Path uniqueReservedPath( const char *prefix ) {
        Path repairPath = Path( repairpath );
        Path reservedPath;
        int i = 0;
        bool exists = false;
        do {
            stringstream ss;
            ss << prefix << "_repairDatabase_" << i++;
            reservedPath = repairPath / ss.str();
            MONGO_ASSERT_ON_EXCEPTION( exists = boost::filesystem::exists( reservedPath ) );
        }
        while ( exists );
        return reservedPath;
    }

    boost::intmax_t dbSize( const char *database ) {
        class SizeAccumulator : public FileOp {
        public:
            SizeAccumulator() : totalSize_( 0 ) {}
            boost::intmax_t size() const {
                return totalSize_;
            }
        private:
            virtual bool apply( const boost::filesystem::path &p ) {
                if ( !boost::filesystem::exists( p ) )
                    return false;
                totalSize_ += boost::filesystem::file_size( p );
                return true;
            }
            virtual const char *op() const {
                return "checking size";
            }
            boost::intmax_t totalSize_;
        };
        SizeAccumulator sa;
        _applyOpToDataFiles( database, sa );
        return sa.size();
    }

    bool repairDatabase( string dbNameS , string &errmsg,
                         bool preserveClonedFilesOnFailure, bool backupOriginalFiles ) {
        doingRepair dr;
        dbNameS = nsToDatabase( dbNameS );
        const char * dbName = dbNameS.c_str();

        stringstream ss;
        ss << "localhost:" << cmdLine.port;
        string localhost = ss.str();

        problem() << "repairDatabase " << dbName << endl;
        verify( cc().database()->name == dbName );
        verify( cc().database()->path == dbpath );

        BackgroundOperation::assertNoBgOpInProgForDb(dbName);

        // TODO: What does tokudb do here?
        //getDur().syncDataAndTruncateJournal(); // Must be done before and after repair

        boost::intmax_t totalSize = dbSize( dbName );
        boost::intmax_t freeSize = File::freeSpace(repairpath);
        if ( freeSize > -1 && freeSize < totalSize ) {
            stringstream ss;
            ss << "Cannot repair database " << dbName << " having size: " << totalSize
               << " (bytes) because free disk space is: " << freeSize << " (bytes)";
            errmsg = ss.str();
            problem() << errmsg << endl;
            return false;
        }

        killCurrentOp.checkForInterrupt();

        Path reservedPath =
            uniqueReservedPath( ( preserveClonedFilesOnFailure || backupOriginalFiles ) ?
                                "backup" : "_tmp" );
        MONGO_ASSERT_ON_EXCEPTION( boost::filesystem::create_directory( reservedPath ) );
        string reservedPathString = reservedPath.native_directory_string();

        bool res;
        {
            // clone to temp location, which effectively does repair
            Client::Context ctx( dbName, reservedPathString );
            verify( ctx.justCreated() );

            res = cloneFrom(localhost.c_str(), errmsg, dbName,
                            /*logForReplication=*/false, /*slaveOk*/false, /*replauth*/false,
                            /*snapshot*/false, /*mayYield*/false, /*mayBeInterrupted*/true);
            Database::closeDatabase( dbName, reservedPathString.c_str() );
        }

        //getDur().syncDataAndTruncateJournal(); // Must be done before and after repair
        MongoFile::flushAll(true); // need both in case journaling is disabled

        if ( !res ) {
            errmsg = str::stream() << "clone failed for " << dbName << " with error: " << errmsg;
            problem() << errmsg << endl;

            if ( !preserveClonedFilesOnFailure )
                MONGO_ASSERT_ON_EXCEPTION( boost::filesystem::remove_all( reservedPath ) );

            return false;
        }

        Client::Context ctx( dbName );
        Database::closeDatabase( dbName, dbpath );

        if ( backupOriginalFiles ) {
            _renameForBackup( dbName, reservedPath );
        }
        else {
            _deleteDataFiles( dbName );
            MONGO_ASSERT_ON_EXCEPTION( boost::filesystem::create_directory( Path( dbpath ) / dbName ) );
        }

        _replaceWithRecovered( dbName, reservedPathString.c_str() );

        if ( !backupOriginalFiles )
            MONGO_ASSERT_ON_EXCEPTION( boost::filesystem::remove_all( reservedPath ) );

        return true;
    }

    void _applyOpToDataFiles( const char *database, FileOp &fo, bool afterAllocator, const string& path ) {
        if ( afterAllocator )
            FileAllocator::get()->waitUntilFinished();
        string c = database;
        c += '.';
        boost::filesystem::path p(path);
        if ( directoryperdb )
            p /= database;
        boost::filesystem::path q;
        q = p / (c+"ns");
        bool ok = false;
        MONGO_ASSERT_ON_EXCEPTION( ok = fo.apply( q ) );
        if ( ok )
            log(2) << fo.op() << " file " << q.string() << endl;
        int i = 0;
        int extra = 10; // should not be necessary, this is defensive in case there are missing files
        while ( 1 ) {
            verify( i <= DiskLoc::MaxFiles );
            stringstream ss;
            ss << c << i;
            q = p / ss.str();
            MONGO_ASSERT_ON_EXCEPTION( ok = fo.apply(q) );
            if ( ok ) {
                if ( extra != 10 ) {
                    log(1) << fo.op() << " file " << q.string() << endl;
                    log() << "  _applyOpToDataFiles() warning: extra == " << extra << endl;
                }
            }
            else if ( --extra <= 0 )
                break;
            i++;
        }
    }

    NamespaceDetails* nsdetails_notinline(const char *ns) { return nsdetails(ns); }

    bool DatabaseHolder::closeAll( const string& path , BSONObjBuilder& result , bool force ) {
        log() << "DatabaseHolder::closeAll path:" << path << endl;
        verify( Lock::isW() );
        // TODO: Tokudb close
        //getDur().commitNow(); // bad things happen if we close a DB with outstanding writes

        map<string,Database*>& m = _paths[path];
        _size -= m.size();

        set< string > dbs;
        for ( map<string,Database*>::iterator i = m.begin(); i != m.end(); i++ ) {
            wassert( i->second->path == path );
            dbs.insert( i->first );
        }

        currentClient.get()->getContext()->_clear();

        BSONObjBuilder bb( result.subarrayStart( "dbs" ) );
        int n = 0;
        int nNotClosed = 0;
        for( set< string >::iterator i = dbs.begin(); i != dbs.end(); ++i ) {
            string name = *i;
            log(2) << "DatabaseHolder::closeAll path:" << path << " name:" << name << endl;
            Client::Context ctx( name , path );
            if( !force && BackgroundOperation::inProgForDb(name.c_str()) ) {
                log() << "WARNING: can't close database " << name << " because a bg job is in progress - try killOp command" << endl;
                nNotClosed++;
            }
            else {
                Database::closeDatabase( name.c_str() , path );
                bb.append( bb.numStr( n++ ) , name );
            }
        }
        bb.done();
        if( nNotClosed )
            result.append("nNotClosed", nNotClosed);
        else {
            ClientCursor::assertNoCursors();
        }

        return true;
    }

} // namespace mongo
