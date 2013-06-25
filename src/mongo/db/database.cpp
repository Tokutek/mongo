// database.cpp

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

#include "mongo/db/database.h"

#include <boost/filesystem/operations.hpp>

#include "mongo/db/clientcursor.h"
#include "mongo/db/databaseholder.h"
#include "mongo/db/instance.h"
#include "mongo/db/introspect.h"
#include "mongo/db/namespacestring.h"

namespace mongo {

    DatabaseHolder _dbHolder;
    DatabaseHolder& dbHolderUnchecked() {
        return _dbHolder;
    }

    Database::Database(const StringData &name, const StringData &path)
        : _name(name.toString()), _path(path.toString()), _nsIndex( _path, _name ),
          _profileName(getSisterNS(_name, "system.profile"))
    {
        try {
            // check db name is valid
            size_t L = name.size();
            uassert( 10028 ,  "db name is empty", L > 0 );
            uassert( 10032 ,  "db name too long", L < 64 );
            uassert( 10029 ,  "bad db name [1]", name[0] != '.' );
            uassert( 10030 ,  "bad db name [2]", name[L-1] != '.' );
            uassert( 10031 ,  "bad char(s) in db name", name.find(' ') == string::npos );
#ifdef _WIN32
            static const char* windowsReservedNames[] = {
                "con", "prn", "aux", "nul",
                "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9",
                "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9"
            };
            for ( size_t i = 0; i < (sizeof(windowsReservedNames) / sizeof(char*)); ++i ) {
                if ( strcasecmp( name, windowsReservedNames[i] ) == 0 ) {
                    stringstream errorString;
                    errorString << "db name \"" << name << "\" is a reserved name";
                    uassert( 16185 , errorString.str(), false );
                }
            }
#endif
            _profile = cmdLine.defaultProfile;
            // The underlying dbname.ns dictionary is opend if it exists,
            // and created lazily on the next write.
            _nsIndex.init();
        } catch (std::exception &e) {
            log() << "warning database " << _path << " " << _name << " could not be opened" << endl;
            DBException* dbe = dynamic_cast<DBException*>(&e);
            if ( dbe != 0 ) {
                log() << "DBException " << dbe->getCode() << ": " << e.what() << endl;
            }
            else {
                log() << e.what() << endl;
            }
            throw;
        }
<<<<<<< HEAD
    }    
=======
    }

    void Database::checkDuplicateUncasedNames(bool inholderlock) const {
        string duplicate = duplicateUncasedName(inholderlock, _name, _path );
        if ( !duplicate.empty() ) {
            stringstream ss;
            ss << "db already exists with different case other: [" << duplicate << "] me [" << _name << "]";
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
        dbHolder().getAllShortNames( allShortNames );

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

    boost::filesystem::path Database::fileName( int n ) const {
        stringstream ss;
        ss << _name << '.' << n;
        boost::filesystem::path fullName;
        fullName = boost::filesystem::path(_path);
        if ( directoryperdb )
            fullName /= _name;
        fullName /= ss.str();
        return fullName;
    }

    bool Database::openExistingFile( int n ) {
        verify(this);
        Lock::assertWriteLocked(_name);
        {
            // must not yet be visible to others as we aren't in the db's write lock and
            // we will write to _files vector - thus this assert.
            bool loaded = dbHolder().__isLoaded(_name, _path);
            verify( !loaded );
        }
        // additionally must be in the dbholder mutex (no assert for that yet)

        // todo: why here? that could be bad as we may be read locked only here
        _namespaceIndex.init();

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

    void Database::clearTmpCollections() {

        Lock::assertWriteLocked( _name );
        Client::Context ctx( _name );

        string systemNamespaces =  _name + ".system.namespaces";

        // Note: we build up a toDelete vector rather than dropping the collection inside the loop
        // to avoid modifying the system.namespaces collection while iterating over it since that
        // would corrupt the cursor.
        vector<string> toDelete;
        shared_ptr<Cursor> cursor = theDataFileMgr.findAll(systemNamespaces);
        while ( cursor && cursor->ok() ) {
            BSONObj nsObj = cursor->current();
            cursor->advance();

            BSONElement e = nsObj.getFieldDotted( "options.temp" );
            if ( !e.trueValue() )
                continue;

            string ns = nsObj["name"].String();

            // Do not attempt to drop indexes
            if ( !NamespaceString::normal(ns.c_str()) )
                continue;

            toDelete.push_back(ns);
        }

        for (size_t i=0; i < toDelete.size(); i++) {
            const string& ns = toDelete[i];

            string errmsg;
            BSONObjBuilder result;
            dropCollection(ns, errmsg, result);

            if ( errmsg.size() > 0 ) {
                warning() << "could not delete temp collection: " << ns
                          << " because of: " << errmsg << endl;
            }
        }
    }

    // todo: this is called a lot. streamline the common case
    MongoDataFile* Database::getFile( int n, int sizeNeeded , bool preallocateOnly) {
        verify(this);
        DEV assertDbAtLeastReadLocked(this);

        _namespaceIndex.init();
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
                if( !Lock::isWriteLocked(this->_name) ) {
                    log() << "error: getFile() called in a read lock, yet file to return is not yet open" << endl;
                    log() << "       getFile(" << n << ") _files.size:" <<_files.size() << ' ' << fileName(n).string() << endl;
                    log() << "       context ns: " << cc().ns() << endl;
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
            if ( n != 0 && _files[ n - 1 ] )
                minSize = _files[ n - 1 ]->getHeader()->fileLength;
            if ( sizeNeeded + DataFileHeader::HeaderSize > minSize )
                minSize = sizeNeeded + DataFileHeader::HeaderSize;
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
                _files[n] = p;
        }
        return preallocateOnly ? 0 : p;
    }

    MongoDataFile* Database::addAFile( int sizeNeeded, bool preallocateNextFile ) {
        assertDbWriteLocked(this);
        int n = (int) _files.size();
        MongoDataFile *ret = getFile( n, sizeNeeded );
        if ( preallocateNextFile )
            preallocateAFile();
        return ret;
    }

    bool fileIndexExceedsQuota( const char *ns, int fileIndex, bool enforceQuota ) {
        return
            cmdLine.quota &&
            enforceQuota &&
            fileIndex >= cmdLine.quotaFiles &&
            // we don't enforce the quota on "special" namespaces as that could lead to problems -- e.g.
            // rejecting an index insert after inserting the main record.
            !NamespaceString::special( ns ) &&
            nsToDatabaseSubstring( ns ) != "local";
    }
    
    MongoDataFile* Database::suitableFile( const char *ns, int sizeNeeded, bool preallocate, bool enforceQuota ) {

        // check existing files
        for ( int i=numFiles()-1; i>=0; i-- ) {
            MongoDataFile* f = getFile( i );
            if ( f->getHeader()->unusedLength >= sizeNeeded ) {
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
                log() << "quota exceeded for namespace: " << ns << endl;
                uasserted(12501, "quota exceeded");
            }
        }

        // allocate files until we either get one big enough or hit maxSize
        for ( int i = 0; i < 8; i++ ) {
            MongoDataFile* f = addAFile( sizeNeeded, preallocate );

            if ( f->getHeader()->unusedLength >= sizeNeeded )
                return f;

            if ( f->getHeader()->fileLength >= MongoDataFile::maxSize() ) // this is as big as they get so might as well stop
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


    Extent* Database::allocExtent( const char *ns, int size, bool capped, bool enforceQuota ) {
        // todo: when profiling, these may be worth logging into profile collection
        bool fromFreeList = true;
        Extent *e = DataFileMgr::allocFromFreeList( ns, size, capped );
        if( e == 0 ) {
            fromFreeList = false;
            e = suitableFile( ns, size, !capped, enforceQuota )->createExtent( ns, size, capped );
        }
        LOG(1) << "allocExtent " << ns << " size " << size << ' ' << fromFreeList << endl; 
        return e;
    }

>>>>>>> 692f185... clean NamespaceString so that it can be the thing passed around

    bool Database::setProfilingLevel( int newLevel , string& errmsg ) {
        if ( _profile == newLevel )
            return true;

        if ( newLevel < 0 || newLevel > 2 ) {
            errmsg = "profiling level has to be >=0 and <= 2";
            return false;
        }

        if ( newLevel == 0 ) {
            _profile = 0;
            return true;
        }

        verify( cc().database() == this );

        if (!getOrCreateProfileCollection(this, true))
            return false;

        _profile = newLevel;
        return true;
    }

    /* db - database name
       path - db directory
    */
    void Database::closeDatabase( const StringData &name, const StringData &path ) {
        verify( Lock::isW() );

        Client::Context * ctx = cc().getContext();
        verify( ctx );
        verify( ctx->inDB( name , path ) );
        Database *database = ctx->db();
        verify( database->name() == name );

        /* important: kill all open cursors on the database */
        string prefix(name.toString() + ".");
        ClientCursor::invalidate(prefix);

        dbHolderW().erase( name, path );
        ctx->_clear();
        delete database; // closes files
    }

    void DatabaseHolder::closeDatabases(const StringData &path) {
        Paths::const_iterator pi = _paths.find(path);
        if (pi != _paths.end()) {
            const DBs &dbs = pi->second;
            while (!dbs.empty()) {
                DBs::const_iterator it = dbs.begin();
                Database *db = it->second;
                dassert(db->name() == it->first);
                // This erases dbs[db->name] for us, can't lift it out yet until we understand the callers of closeDatabase().
                // That's why we have a weird loop here.
                Client::WriteContext ctx(db->name());
                db->closeDatabase(db->name(), path);
            }
            _paths.erase(path);
        }
    }

    Database* DatabaseHolder::getOrCreate( const StringData &ns , const StringData& path ) {
        Lock::assertAtLeastReadLocked(ns);
        Database *db;

        // Try first holding a shared lock
        {
            SimpleRWLock::Shared lk(_rwlock);
            db = _get(ns, path);
            if (db != NULL) {
                return db;
            }
        }

        // If we didn't find it, take an exclusive lock and check
        // again. If it's still not there, do the open.
        {
            SimpleRWLock::Exclusive lk(_rwlock);
            db = _get(ns, path);
            if (db == NULL) {
                StringData dbname = _todb( ns );
                DBs &m = _paths[path];
                if( logLevel >= 1 || m.size() > 40 || DEBUG_BUILD ) {
                    log() << "opening db: " << (path==dbpath?"":path) << ' ' << dbname << endl;
                }

                db = new Database( dbname , path );

                verify( m[dbname] == 0 );
                m[dbname] = db;
                _size++;
            }
        }

        return db;
    }

} // namespace mongo
