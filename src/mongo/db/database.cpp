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
            Lock::assertAtLeastReadLocked(db->name());
        }
        else {
            verify( Lock::isLocked() );
        }
    }

    void assertDbWriteLocked(const Database *db) { 
        if( db ) { 
            Lock::assertWriteLocked(db->name());
        }
        else {
            verify( Lock::isW() );
        }
    }

    Database::~Database() {
    }

    Database::Database(const char *name, const string &path)
        : _name(name), _path(path), _nsIndex( _path, _name ),
          _profileName(_name + ".system.profile")
    {
        try {
            // check db name is valid
            size_t L = strlen(name);
            uassert( 10028 ,  "db name is empty", L > 0 );
            uassert( 10032 ,  "db name too long", L < 64 );
            uassert( 10029 ,  "bad db name [1]", *name != '.' );
            uassert( 10030 ,  "bad db name [2]", name[L-1] != '.' );
            uassert( 10031 ,  "bad char(s) in db name", strchr(name, ' ') == 0 );
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
            _magic = 781231;
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
    }    


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

    void DatabaseHolder::closeDatabases(const string &path) {
        Paths::iterator pi = _paths.find(path);
        if (pi != _paths.end()) {
            DBs &dbs = pi->second;
            while (!dbs.empty()) {
                DBs::iterator it = dbs.begin();
                Database *db = it->second;
                dassert(db->name() == it->first);
                // This erases dbs[db->name] for us, can't lift it out yet until we understand the callers of closeDatabase().
                // That's why we have a weird loop here.
                Client::WriteContext ctx(db->name());
                db->closeDatabase(db->name().c_str(), path);
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
            wassert(it->second->path() == path);
            dbs.insert(it->first);
        }

        cc().getContext()->_clear();

        BSONObjBuilder bb(result.subarrayStart("dbs"));
        int n = 0;
        int nNotClosed = 0;
        for (set<string>::const_iterator it = dbs.begin(); it != dbs.end(); ++it) {
            string name = *it;
            LOG(2) << "DatabaseHolder::closeAll path:" << path << " name:" << name << endl;
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

    Database* DatabaseHolder::getOrCreate( const string& ns , const string& path ) {
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
                string dbname = _todb( ns );
                DBs &m = _paths[path];
                if( logLevel >= 1 || m.size() > 40 || DEBUG_BUILD ) {
                    log() << "opening db: " << (path==dbpath?"":path) << ' ' << dbname << endl;
                }

                db = new Database( dbname.c_str() , path );

                verify( m[dbname] == 0 );
                m[dbname] = db;
                _size++;
            }
        }

        return db;
    }

} // namespace mongo
