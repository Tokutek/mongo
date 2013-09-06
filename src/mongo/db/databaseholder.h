// @file databaseholder.h

/**
*    Copyright (C) 2012 10gen Inc.
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
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#pragma once

#include "mongo/db/database.h"
#include "mongo/db/namespacestring.h"

namespace mongo { 

    /**
     * path + dbname -> Database
     */
    class DatabaseHolder {
        typedef StringMap<Database*> DBs;
        typedef StringMap<DBs> Paths;
        mutable SimpleRWLock _rwlock;
        Paths _paths;
        int _size;
    public:
        DatabaseHolder() : _rwlock("dbholderRWLock"),_size(0) { }

        bool __isLoaded( const StringData& ns , const StringData& path ) const {
            SimpleRWLock::Shared lk(_rwlock);
            Paths::const_iterator x = _paths.find( path );
            if ( x == _paths.end() )
                return false;
            const DBs& m = x->second;

            StringData db = _todb( ns );

            DBs::const_iterator it = m.find(db);
            return it != m.end();
        }
        // must be write locked as otherwise isLoaded could go false->true on you 
        // in the background and you might not expect that.
        bool _isLoaded( const StringData& ns , const StringData& path ) const {
            Lock::assertWriteLocked(ns);
            return __isLoaded(ns,path);
        }

        Database *get( const StringData& ns , const StringData& path ) const {
            SimpleRWLock::Shared lk(_rwlock);
            Lock::assertAtLeastReadLocked(ns);
            return _get(ns, path);
        }

        Database *getOrCreate( const StringData& ns , const StringData& path );

        void erase( const StringData& ns , const StringData& path ) {
            SimpleRWLock::Exclusive lk(_rwlock);
            verify( Lock::isW() );
            DBs& m = _paths[path];
            _size -= (int)m.erase( _todb( ns ) );
        }

        /** @param force - force close even if something underway - use at shutdown */
        bool closeAll( const StringData& path , BSONObjBuilder& result, bool force );

        void closeDatabases(const StringData &path);

        // "info" as this is informational only could change on you if you are not write locked
        int sizeInfo() const { return _size; }

        /**
         * gets all unique db names, ignoring paths
         * need some lock
         */
        void getAllShortNames( set<string>& all, bool inholderlock=false ) const {
            if (inholderlock) {
                _getAllShortNames(all);
            } else {
                SimpleRWLock::Shared lk(_rwlock);
                _getAllShortNames(all);
            }
        }

    private:
        void _getAllShortNames(set<string> &all) const {
            for ( Paths::const_iterator i=_paths.begin(); i!=_paths.end(); ++i ) {
                DBs m = i->second;
                for( DBs::const_iterator j=m.begin(); j!=m.end(); ++j ) {
                    all.insert( j->first );
                }
            }
        }

        Database *_get( const StringData& ns , const StringData& path ) const {
            Paths::const_iterator x = _paths.find( path );
            if ( x == _paths.end() )
                return 0;
            const DBs& m = x->second;
            StringData db = _todb( ns );
            DBs::const_iterator it = m.find(db);
            if ( it != m.end() )
                return it->second;
            return 0;
        }
                
        static StringData _todb( const StringData& ns ) {
            StringData d = __todb( ns );
            uassert( 13280 , (string)"invalid db name: " + ns.toString() , NamespaceString::validDBName( d ) );
            return d;
        }
        static StringData __todb( const StringData& ns ) {
            StringData d = nsToDatabaseSubstring(ns);
            uassert( 13074 , "db name can't be empty" , !d.empty() );
            return d;
        }
    };

    DatabaseHolder& dbHolderUnchecked();
    inline const DatabaseHolder& dbHolder() { 
        dassert( Lock::isLocked() );
        return dbHolderUnchecked();
    }
    inline DatabaseHolder& dbHolderW() { 
        dassert( Lock::isW() );
        return dbHolderUnchecked();
    }

}
