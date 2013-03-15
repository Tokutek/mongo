// database.cpp

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
#include "database.h"
#include "instance.h"
#include "introspect.h"
#include "clientcursor.h"
#include "databaseholder.h"

#include <boost/filesystem/operations.hpp>

namespace mongo {

    DatabaseHolder _dbHolder;
    DatabaseHolder& dbHolderUnchecked() {
        return _dbHolder;
    }

    void assertDbAtLeastReadLocked(const Database *db) { 
        if( db ) { 
            Lock::assertAtLeastReadLocked(db->name);
        }
        else {
            verify( Lock::isLocked() );
        }
    }

    void assertDbWriteLocked(const Database *db) { 
        if( db ) { 
            Lock::assertWriteLocked(db->name);
        }
        else {
            verify( Lock::isW() );
        }
    }

    Database::~Database() {
    }

    Database::Database(const char *nm, bool& newDb, const string& _path )
        : name(nm), path(_path), namespaceIndex( path, name ),
          profileName(name + ".system.profile")
    {
        try {
            {
                // check db name is valid
                size_t L = strlen(nm);
                uassert( 10028 ,  "db name is empty", L > 0 );
                uassert( 10032 ,  "db name too long", L < 64 );
                uassert( 10029 ,  "bad db name [1]", *nm != '.' );
                uassert( 10030 ,  "bad db name [2]", nm[L-1] != '.' );
                uassert( 10031 ,  "bad char(s) in db name", strchr(nm, ' ') == 0 );
#ifdef _WIN32
                static const char* windowsReservedNames[] = {
                    "con", "prn", "aux", "nul",
                    "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9",
                    "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9"
                };
                for ( size_t i = 0; i < (sizeof(windowsReservedNames) / sizeof(char*)); ++i ) {
                    if ( strcasecmp( nm, windowsReservedNames[i] ) == 0 ) {
                        stringstream errorString;
                        errorString << "db name \"" << nm << "\" is a reserved name";
                        uassert( 16185 , errorString.str(), false );
                    }
                }
#endif
            }
            profile = cmdLine.defaultProfile;
            checkDuplicateUncasedNames(true);
            // If already exists, open.  Otherwise behave as if empty until
            // there's a write, then open.
            namespaceIndex.init();
            if (namespaceIndex.allocated()) {
                newDb = false;
            } else {
                newDb = true;
            }
            magic = 781231;
        } catch(std::exception& e) {
            log() << "warning database " << path << " " << nm << " could not be opened" << endl;
            DBException* dbe = dynamic_cast<DBException*>(&e);
            if ( dbe != 0 ) {
                log() << "DBException " << dbe->getCode() << ": " << e.what() << endl;
            }
            else {
                log() << e.what() << endl;
            }
            // since destructor won't be called:
#if 0
            for ( size_t i = 0; i < _files.size(); i++ ) {
                delete _files[i];
            }
            _files.clear();
#endif
            throw;
        }
    }
    
    void Database::checkDuplicateUncasedNames(bool inholderlock) const {
        string duplicate = duplicateUncasedName(inholderlock, name, path );
        if ( !duplicate.empty() ) {
            stringstream ss;
            ss << "db already exists with different case other: [" << duplicate << "] me [" << name << "]";
            uasserted( DatabaseDifferCaseCode , ss.str() );
        }
    }

    /*static*/
    string Database::duplicateUncasedName( bool inholderlock, const string &name, const string &path, set< string > *duplicates ) {
        Lock::assertAtLeastReadLocked(name);

        if ( duplicates ) {
            duplicates->clear();   
        }
        
        vector<string> others;
        getDatabaseNames( others , path );
        
        set<string> allShortNames;
        dbHolder().getAllShortNames(allShortNames, inholderlock);
        
        others.insert( others.end(), allShortNames.begin(), allShortNames.end() );
        
        for ( unsigned i=0; i<others.size(); i++ ) {

            if ( strcasecmp( others[i].c_str() , name.c_str() ) )
                continue;
            
            if ( strcmp( others[i].c_str() , name.c_str() ) == 0 )
                continue;

            if ( duplicates ) {
                duplicates->insert( others[i] );
            } else {
                return others[i];
            }
        }
        if ( duplicates ) {
            return duplicates->empty() ? "" : *duplicates->begin();
        }
        return "";
    }
    
#if 0
    boost::filesystem::path Database::fileName( int n ) const {
        stringstream ss;
        ss << name << '.' << n;
        boost::filesystem::path fullName;
        fullName = boost::filesystem::path(path);
        //if ( directoryperdb )
        //    fullName /= name;
        fullName /= ss.str();
        return fullName;
    }

    bool Database::openExistingFile( int n ) { 
        ::abort();
        verify(this);
        Lock::assertWriteLocked(name);
        {
            // must not yet be visible to others as we aren't in the db's write lock and 
            // we will write to _files vector - thus this assert.
            bool loaded = dbHolder().__isLoaded(name, path);
            verify( !loaded );
        }
        // additionally must be in the dbholder mutex (no assert for that yet)

        // todo: why here? that could be bad as we may be read locked only here
        namespaceIndex.init();

        if ( n < 0 || n >= DiskLoc::MaxFiles ) {
            massert( 15924 , str::stream() << "getFile(): bad file number value " << n << " (corrupt db?): run repair", false);
        }

        {
            if( n < (int) _files.size() && _files[n] ) {
                dlog(2) << "openExistingFile " << n << " is already open" << endl;
                return true;
            }
        }

        {
            boost::filesystem::path fullName = fileName( n );
            string fullNameString = fullName.string();
            MongoDataFile *df = new MongoDataFile(n);
            try {
                if( !df->openExisting( fullNameString.c_str() ) ) { 
                    delete df;
                    return false;
                }
            }
            catch ( AssertionException& ) {
                delete df;
                throw;
            }
            while ( n >= (int) _files.size() ) {
                _files.push_back(0);
            }
            _files[n] = df;
        }

        return true;
    }

    // todo : we stop once a datafile dne.
    //        if one datafile were missing we should keep going for 
    //        repair purposes yet we do not.
    void Database::openAllFiles() {
        verify(this);
        int n = 0;
        while( openExistingFile(n) ) {
            n++;
        }
    }

    // todo: this is called a lot. streamline the common case
    MongoDataFile* Database::getFile( int n, int sizeNeeded , bool preallocateOnly) {
        verify(this);
        DEV assertDbAtLeastReadLocked(this);

        namespaceIndex.init();
        if ( n < 0 || n >= DiskLoc::MaxFiles ) {
            out() << "getFile(): n=" << n << endl;
            massert( 10295 , "getFile(): bad file number value (corrupt db?): run repair", false);
        }
        DEV {
            if ( n > 100 ) {
                out() << "getFile(): n=" << n << endl;
            }
        }
        MongoDataFile* p = 0;
        if ( !preallocateOnly ) {
            while ( n >= (int) _files.size() ) {
                verify(this);
                if( !Lock::isWriteLocked(this->name) ) {
                    log() << "error: getFile() called in a read lock, yet file to return is not yet open" << endl;
                    log() << "       getFile(" << n << ") _files.size:" <<_files.size() << ' ' << fileName(n).string() << endl;
                    log() << "       context ns: " << cc().ns() << " openallfiles:" << _openAllFiles << endl;
                    verify(false);
                }
                _files.push_back(0);
            }
            p = _files[n];
        }
        if ( p == 0 ) {
            assertDbWriteLocked(this);
            boost::filesystem::path fullName = fileName( n );
            string fullNameString = fullName.string();
            p = new MongoDataFile(n);
            int minSize = 0;
            if ( n != 0 && 0) //_files[ n - 1 ] )
                minSize = 0;//_files[ n - 1 ]->getHeader()->fileLength;
            if ( sizeNeeded ) //+ DataFileHeader::HeaderSize > minSize )
                minSize = 0;//sizeNeeded + DataFileHeader::HeaderSize;
            ::abort();
            try {
                p->open( fullNameString.c_str(), minSize, preallocateOnly );
            }
            catch ( AssertionException& ) {
                delete p;
                throw;
            }
            if ( preallocateOnly )
                delete p;
            else
                ; //_files[n] = p;
        }
        return preallocateOnly ? 0 : p;
    }
#endif

#if 0
    MongoDataFile* Database::addAFile( int sizeNeeded, bool preallocateNextFile ) {
        assertDbWriteLocked(this);
        int n = (int) _files.size();
        MongoDataFile *ret = getFile( n, sizeNeeded );
        if ( preallocateNextFile )
            preallocateAFile();
        return ret;
    }
#endif

    bool fileIndexExceedsQuota( const char *ns, int fileIndex, bool enforceQuota ) {
        return
            cmdLine.quota &&
            enforceQuota &&
            fileIndex >= cmdLine.quotaFiles &&
            // we don't enforce the quota on "special" namespaces as that could lead to problems -- e.g.
            // rejecting an index insert after inserting the main record.
            !NamespaceString::special( ns ) &&
            NamespaceString( ns ).db != "local";
    }
    
#if 0
    MongoDataFile* Database::suitableFile( const char *ns, int sizeNeeded, bool preallocate, bool enforceQuota ) {

        // check existing files
        for ( int i=numFiles()-1; i>=0; i-- ) {
            MongoDataFile* f = getFile( i );
            ::abort();
            if ( 0 ) { //f->getHeader()->unusedLength >= sizeNeeded ) {
                if ( fileIndexExceedsQuota( ns, i-1, enforceQuota ) ) // NOTE i-1 is the value used historically for this check.
                    ;
                else
                    return f;
            }
        }

        if ( fileIndexExceedsQuota( ns, numFiles(), enforceQuota ) ) {
            if ( cc().hasWrittenThisPass() ) {
                warning() << "quota exceeded, but can't assert, probably going over quota for: " << ns << endl;
            }
            else {
                uasserted(12501, "quota exceeded");
            }
        }

        // allocate files until we either get one big enough or hit maxSize
        for ( int i = 0; i < 8; i++ ) {
            MongoDataFile* f = addAFile( sizeNeeded, preallocate );

            ::abort();
            if ( 0 ) //f->getHeader()->unusedLength >= sizeNeeded )
                return f;

            ::abort();
            if ( 0 ) //f->getHeader()->fileLength >= MongoDataFile::maxSize() ) // this is as big as they get so might as well stop
                return f;
        }

        uasserted(14810, "couldn't allocate space (suitableFile)"); // callers don't check for null return code
        return 0;
    }

    MongoDataFile* Database::newestFile() {
        int n = numFiles();
        if ( n == 0 )
            return 0;
        return getFile(n-1);
    }
#endif


#if 0
    Extent* Database::allocExtent( const char *ns, int size, bool capped, bool enforceQuota ) {
        // todo: when profiling, these may be worth logging into profile collection
        bool fromFreeList = true;
        Extent *e = NULL; ::abort(); //DataFileMgr::allocFromFreeList( ns, size, capped );
        if( e == 0 ) {
            fromFreeList = false;
            e = suitableFile( ns, size, !capped, enforceQuota )->createExtent( ns, size, capped );
        }
        LOG(1) << "allocExtent " << ns << " size " << size << ' ' << fromFreeList << endl; 
        return e;
        ::abort();
        return NULL;
    }
#endif


    bool Database::setProfilingLevel( int newLevel , string& errmsg ) {
        if ( profile == newLevel )
            return true;

        if ( newLevel < 0 || newLevel > 2 ) {
            errmsg = "profiling level has to be >=0 and <= 2";
            return false;
        }

        if ( newLevel == 0 ) {
            profile = 0;
            return true;
        }

        verify( cc().database() == this );

        if (!getOrCreateProfileCollection(this, true))
            return false;

        profile = newLevel;
        return true;
    }

#if 0
    bool Database::exists(int n) const { 
        return boost::filesystem::exists( fileName( n ) ); 
    }

    int Database::numFiles() const { 
        DEV assertDbAtLeastReadLocked(this);
        return (int) _files.size(); 
        ::abort();
        return 0;
    }
#endif

    void Database::flushFiles( bool sync ) {
        ::abort();
#if 0
        assertDbAtLeastReadLocked(this);
        for( vector<MongoDataFile*>::iterator i = _files.begin(); i != _files.end(); i++ ) { 
            MongoDataFile *f = *i;
            f->flush(sync);
        }
#endif
    }

    long long Database::fileSize() const {
        long long size=0;
        wunimplemented("Database::fileSize");
#if 0
        for (int n=0; exists(n); n++)
            size += boost::filesystem::file_size( fileName(n) );
#endif
        return size;
    }

    void DatabaseHolder::closeDatabases(const string &path) {
        Paths::iterator pi = _paths.find(path);
        if (pi != _paths.end()) {
            DBs &dbs = pi->second;
            while (!dbs.empty()) {
                DBs::iterator it = dbs.begin();
                Database *db = it->second;
                dassert(db->name == it->first);
                // This erases dbs[db->name] for us, can't lift it out yet until we understand the callers of closeDatabase().
                // That's why we have a weird loop here.
                Client::WriteContext ctx(db->name);
                db->closeDatabase(db->name.c_str(), path);
            }
            _paths.erase(pi);
        }
    }

    bool DatabaseHolder::closeAll(const string& path, BSONObjBuilder& result, bool force) {
        log() << "DatabaseHolder::closeAll path:" << path << endl;
        verify(Lock::isW());

        Paths::iterator x = _paths.find(path);
        wassert(x != _paths.end());
        const DBs &m = x->second;

        set<string> dbs;
        for (DBs::const_iterator it = m.begin(); it != m.end(); ++it) {
            wassert(it->second->path == path);
            dbs.insert(it->first);
        }

        cc().getContext()->_clear();

        BSONObjBuilder bb(result.subarrayStart("dbs"));
        int n = 0;
        int nNotClosed = 0;
        for (set<string>::const_iterator it = dbs.begin(); it != dbs.end(); ++it) {
            string name = *it;
            log(2) << "DatabaseHolder::closeAll path:" << path << " name:" << name << endl;
            Client::Context ctx(name, path);
            // Don't know how to implement this check anymore.
            /*
            if (!force && BackgroundOperation::inProgForDb(name.c_str())) {
                log() << "WARNING: can't close database " << name << " because a bg job is in progress - try killOp command" << endl;
                nNotClosed++;
            }
            else {
            */
                // This removes the db from m for us
                Database::closeDatabase(name.c_str(), path);
                bb.append(bb.numStr(n++), name);
            /*
            }
            */
        }
        bb.done();
        if (nNotClosed) {
            result.append("nNotClosed", nNotClosed);
        }
        else {
            ClientCursor::assertNoCursors();
        }

        _size -= m.size();
        _paths.erase(x);

        return true;
    }

    Database* DatabaseHolder::getOrCreate( const string& ns , const string& path , bool& justCreated ) {
        string dbname = _todb( ns );
        Database *db;
        {
            SimpleMutex::scoped_lock lk(_m);
            Lock::assertAtLeastReadLocked(ns);
            DBs& m = _paths[path];
            {
                DBs::iterator i = m.find(dbname); 
                if( i != m.end() ) {
                    justCreated = false;
                    return i->second;
                }
            }

            // todo: protect against getting sprayed with requests for different db names that DNE - 
            //       that would make the DBs map very large.  not clear what to do to handle though, 
            //       perhaps just log it, which is what we do here with the "> 40" : 
            bool cant = !Lock::isWriteLocked(ns);
            if( logLevel >= 1 || m.size() > 40 || cant || DEBUG_BUILD ) {
                log() << "opening db: " << (path==dbpath?"":path) << ' ' << dbname << endl;
            }
            massert(15927, "can't open database in a read lock. if db was just closed, consider retrying the query. might otherwise indicate an internal error", !cant);

            db = new Database( dbname.c_str() , justCreated , path );

            verify( m[dbname] == 0 );
            m[dbname] = db;
            _size++;
        }

        return db;
    }

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
            details->addPKIndexToCatalog();
        }
        return details;
    }

} // namespace mongo
