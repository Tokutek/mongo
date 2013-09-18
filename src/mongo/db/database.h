// database.h

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

#pragma once

#include "mongo/db/collection.h"
#include "mongo/db/collection_map.h"
#include "mongo/db/storage_options.h"

namespace mongo {

    void dropDatabase(const StringData &db);

    /**
     * Database represents an set of namespaces. It has an index mapping
     * namespace name to Collection object, if it exists and is open.
     * The database is represented on disk as a TokuMX dictionary named dbname.ns
    */
    class Database {
    public:
        Database(const StringData &name, const StringData &path = storageGlobalParams.dbpath);

        /* you must use this to close - there is essential code in this method that is not in the ~Database destructor.
           thus the destructor is private.  this could be cleaned up one day...
        */
        static void closeDatabase( const StringData &db, const StringData& path );

        bool isEmpty() {
            _collectionMap.init();
            return !_collectionMap.allocated();
        }

        int profile() const { return _profile; }

        const string &profileName() const { return _profileName; }

        const string &name() const { return _name; }

        const string &path() const { return _path; }

        void diskSize(size_t &uncompressedSize, size_t &compressedSize);

        /**
         * @return true if success.  false if bad level or error creating profile ns
         */
        bool setProfilingLevel( int newLevel , string& errmsg );

    private:
        const string _name;
        const string _path;

        CollectionMap _collectionMap;
        int _profile; // 0=off.
        const string _profileName; // "alleyinsider.system.profile"
        int _magic; // used for making sure the object is still loaded in memory

        ~Database() { }

        friend class CollectionMap;
        friend CollectionMap *collectionMap(const StringData&);
    };

} // namespace mongo
