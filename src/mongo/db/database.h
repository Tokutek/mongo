// database.h

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

#pragma once

#include "mongo/db/cmdline.h"
#include "mongo/db/namespace_details.h"

namespace mongo {

    /**
     * Database represents an set of namespaces. It has an index mapping
     * namespace name to NamespaceDetails object, if it exists and is open.
     * The database is represented on disk as a TokuDB dictionary named dbname.ns
    */
    class Database {
    public:
        // you probably need to be in dbHolderMutex when constructing this
        Database(const char *nm, /*out*/ bool& newDb, const string& _path = dbpath);

        /* you must use this to close - there is essential code in this method that is not in the ~Database destructor.
           thus the destructor is private.  this could be cleaned up one day...
        */
        static void closeDatabase( const char *db, const string& path );

        /**
         * tries to make sure that this hasn't been deleted
         */
        bool isOk() const { return magic == 781231; }

        bool isEmpty() { return ! namespaceIndex.allocated(); }

        /**
         * @return true if success.  false if bad level or error creating profile ns
         */
        bool setProfilingLevel( int newLevel , string& errmsg );

        /**
         * @return true if ns is part of the database
         *         ns=foo.bar, db=foo returns true
         */
        bool ownsNS( const string& ns ) const {
            if ( ! startsWith( ns , name ) )
                return false;
            return ns[name.size()] == '.';
        }

        // TODO: Make all of these private
        const string name; // "alleyinsider"
        const string path;

        NamespaceIndex namespaceIndex;
        int profile; // 0=off.
        const string profileName; // "alleyinsider.system.profile"
        int magic; // used for making sure the object is still loaded in memory

    private:
        ~Database(); // closes files and other cleanup see below.
    };

} // namespace mongo
