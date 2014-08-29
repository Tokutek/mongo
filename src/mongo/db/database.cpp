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

#include "mongo/db/clientcursor.h"
#include "mongo/db/database.h"
#include "mongo/db/databaseholder.h"
#include "mongo/db/instance.h"
#include "mongo/db/introspect.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/storage_options.h"

namespace mongo {

    DatabaseHolder _dbHolder;
    DatabaseHolder& dbHolderUnchecked() {
        return _dbHolder;
    }

    Database::Database(const StringData &name, const StringData &path)
        : _name(name.toString()), _path(path.toString()), _collectionMap( _path, _name ),
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
            _profile = serverGlobalParams.defaultProfile;
            // The underlying dbname.ns dictionary is opend if it exists,
            // and created lazily on the next write.
            _collectionMap.init();
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

    void Database::diskSize(intmax_t &uncompressedSize, intmax_t &compressedSize) {
        list<string> colls;
        _collectionMap.getNamespaces(colls);
        CollectionData::Stats dbstats;
        for (list<string>::const_iterator it = colls.begin(); it != colls.end(); ++it) {
            Collection *c = getCollection(*it);
            if (c == NULL) {
                DEV warning() << "collection " << *it << " wasn't found in Database::diskSize" << endl;
                continue;
            }
            c->fillCollectionStats(dbstats, NULL, 1);
        }
        uncompressedSize += dbstats.size + dbstats.indexSize;
        compressedSize += dbstats.storageSize + dbstats.indexStorageSize;
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
                LOCK_REASON(lockReason, "closing databases");
                Client::WriteContext ctx(db->name(), lockReason);
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
                if( logger::globalLogDomain()->shouldLog(logger::LogSeverity::Debug(1)) || m.size() > 40 || DEBUG_BUILD ) {
                    log() << "opening db: " << (path==storageGlobalParams.dbpath?"":path) << ' ' << dbname << endl;
                }

                db = new Database( dbname , path );

                verify( m[dbname] == 0 );
                m[dbname] = db;
                _size++;
            }
        }

        return db;
    }

    void dropDatabase(const StringData& name) {
        TOKULOG(1) << "dropDatabase " << name << endl;
        Lock::assertWriteLocked(name);
        Database *d = cc().database();
        verify(d != NULL);
        verify(d->name() == name);

        // Disable dropDatabase in a multi-statement transaction until
        // we have the time/patience to test/debug it.
        if (cc().txnStackSize() > 1) {
            uasserted(16777, "Cannot dropDatabase in a multi-statement transaction.");
        }

        collectionMap(name)->drop();
        Database::closeDatabase(d->name().c_str(), d->path());
    }

} // namespace mongo
