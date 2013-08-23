// @file namespacestring.h

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

#include <string>

#include "mongo/bson/util/builder.h"
#include "mongo/util/assert_util.h"

namespace mongo {

    using std::string;

    /* in the mongo source code, "client" means "database". */

    const size_t MaxDatabaseNameLen = 128; // max str len for the db name, including null char

    // "database.a.b.c" -> "a.b.c"
    // cheaper than constructing a whole NamespaceString
    inline StringData nsToCollectionSubstring( const StringData &ns ) {
        size_t i = ns.find( '.' );
        if ( i == string::npos ) {
            massert(17011, "nsToCollection: ns too long", ns.size() < MaxDatabaseNameLen );
            return StringData();
        }
        massert(17012, "nsToCollection: ns too long", i < static_cast<size_t>(MaxDatabaseNameLen));
        return ns.substr(i + 1);
    }

    // "database.a.b.c" -> "database"
    inline StringData nsToDatabaseSubstring( const StringData &ns ) {
        size_t i = ns.find( '.' );
        if ( i == string::npos ) {
            massert(10078, "nsToDatabase: ns too long", ns.size() < MaxDatabaseNameLen );
            return ns;
        }
        massert(10088, "nsToDatabase: ns too long", i < static_cast<size_t>(MaxDatabaseNameLen));
        return ns.substr( 0, i );
    }

    /* e.g.
       NamespaceString ns("acme.orders");
       cout << ns.coll; // "orders"
    */
    class NamespaceString {
    public:
        string db;
        string coll; // note collection names can have periods in them for organizing purposes (e.g. "system.indexes")

        NamespaceString( const char * ns ) { init(ns); }
        NamespaceString( const string& ns ) { init(ns.c_str()); }

        string ns() const { return db + '.' + coll; }

        bool isSystem() const { return strncmp(coll.c_str(), "system.", 7) == 0; }
        static bool isSystem(const StringData &ns) {
            return nsToCollectionSubstring(ns).startsWith("system.");
        }
        bool isCommand() const { return coll == "$cmd"; }
        static bool isCommand(const StringData &ns) {
            return nsToCollectionSubstring(ns) == "$cmd";
        }

        /**
         * @return true if the namespace is valid. Special namespaces for internal use are considered as valid.
         */
        bool isValid() const {
            return validDBName( db ) && !coll.empty();
        }
        static bool isValid(const StringData &ns) {
            return validDBName(nsToDatabaseSubstring(ns)) && !nsToCollectionSubstring(ns).empty();
        }

        operator string() const { return ns(); }

        bool operator==( const string& nsIn ) const { return nsIn == ns(); }
        bool operator==( const char* nsIn ) const { return (string)nsIn == ns(); }
        bool operator==( const NamespaceString& nsIn ) const { return nsIn.db == db && nsIn.coll == coll; }

        bool operator!=( const string& nsIn ) const { return nsIn != ns(); }
        bool operator!=( const char* nsIn ) const { return (string)nsIn != ns(); }
        bool operator!=( const NamespaceString& nsIn ) const { return nsIn.db != db || nsIn.coll != coll; }

        bool operator<( const NamespaceString& rhs ) const { return ns() < rhs.ns(); }

        size_t size() const { return ns().size(); }

        string toString() const { return ns(); }

        /**
         * @return true if ns is 'normal'.  $ used for collections holding index data, which do not contain BSON objects in their records.
         * special case for the local.oplog.$main ns -- naming it as such was a mistake.
         * TokuMX doesn't need this special case.
         */
        static bool normal(const StringData &ns) {
            return ns.find('$') == string::npos;
        }

        /**
         * @return true if the ns is an oplog one, otherwise false.
         */
        static bool oplog(const StringData &ns) {
            return ns == "local.oplog.rs" || ns == "local.oplog.refs";
        }

        static bool special(const StringData &ns) {
            return !normal(ns) || isSystem(ns);
        }

        /**
         * samples:
         *   good:  
         *      foo  
         *      bar
         *      foo-bar
         *   bad:
         *      foo bar
         *      foo.bar
         *      foo"bar
         *        
         * @param db - a possible database name
         * @return if db is an allowed database name
         */
        static bool validDBName(const StringData &db) {
            if (db.size() == 0 || db.size() > 64) {
                return false;
            }
#ifdef _WIN32
# error "TokuMX doesn't support windows."
#else
            size_t good = db.cspn("/\\. \"");
#endif
            return good == db.size();
        }

        /**
         * samples:
         *   good:
         *      foo.bar
         *   bad:
         *      foo.
         *
         * @param dbcoll - a possible collection name of the form db.coll
         * @return if db.coll is an allowed collection name
         */
        static bool validCollectionName(const StringData &dbcoll) {
            StringData coll = nsToCollectionSubstring(dbcoll);
            return !coll.empty() && normal(dbcoll);
        }

    private:
        void init(const char *ns) {
            const char *p = strchr(ns, '.');
            if( p == 0 ) return;
            db = string(ns, p - ns);
            coll = p + 1;
        }
    };

    // "database.a.b.c" -> "database"
    inline void nsToDatabase(const StringData& ns, char *database) {
        StringData db = nsToDatabaseSubstring( ns );
        db.copyTo( database, true );
    }

    // TODO: make this return a StringData
    inline string nsToDatabase(const StringData &ns) {
        return nsToDatabaseSubstring( ns ).toString();
    }

    inline string nsToCollection(const StringData &ns) {
        return nsToCollectionSubstring( ns ).toString();
    }

    inline bool isValidNS( const StringData &ns ) {
        // TODO: should check for invalid characters

        size_t idx = ns.find( '.' );
        if ( idx == string::npos )
            return false;

        if ( idx == ns.size() - 1 )
            return false;

        return true;
    }

    // TODO: Possibly make this less inefficient.
    inline string getSisterNS(const StringData &ns, const StringData &local) {
        verify( local.size() > 0 && local[0] != '.' );
        mongo::StackStringBuilder ss;
        ss << nsToDatabaseSubstring(ns) << "." << local;
        return ss.str();
    }

}
