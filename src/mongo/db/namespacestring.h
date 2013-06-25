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

<<<<<<< HEAD
#include "mongo/bson/util/builder.h"
=======
#include "mongo/base/string_data.h"
>>>>>>> 692f185... clean NamespaceString so that it can be the thing passed around
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
        NamespaceString( const StringData& ns );

        StringData db() const;
        StringData coll() const;

        const string& ns() const { return _ns; }

<<<<<<< HEAD
        bool isSystem() const { return strncmp(coll.c_str(), "system.", 7) == 0; }
        static bool isSystem(const StringData &ns) {
            return nsToCollectionSubstring(ns).startsWith("system.");
        }
        bool isCommand() const { return coll == "$cmd"; }
        static bool isCommand(const StringData &ns) {
            return nsToCollectionSubstring(ns) == "$cmd";
        }
=======
        operator string() const { return _ns; }
        string toString() const { return _ns; }

        size_t size() const { return _ns.size(); }

        bool isSystem() const { return coll().startsWith( "system." ); }
        bool isCommand() const { return coll() == "$cmd"; }
>>>>>>> 692f185... clean NamespaceString so that it can be the thing passed around

        /**
         * @return true if the namespace is valid. Special namespaces for internal use are considered as valid.
         */
<<<<<<< HEAD
        bool isValid() const {
            return validDBName( db ) && !coll.empty();
        }
        static bool isValid(const StringData &ns) {
            return validDBName(nsToDatabaseSubstring(ns)) && !nsToCollectionSubstring(ns).empty();
        }

        operator string() const { return ns(); }
=======
        bool isValid() const { return validDBName( db() ) && !coll().empty(); }
>>>>>>> 692f185... clean NamespaceString so that it can be the thing passed around

        bool operator==( const string& nsIn ) const { return nsIn == _ns; }
        bool operator==( const NamespaceString& nsIn ) const { return nsIn._ns == _ns; }

        bool operator!=( const string& nsIn ) const { return nsIn != _ns; }
        bool operator!=( const NamespaceString& nsIn ) const { return nsIn._ns != _ns; }

        /**
         * @return true if ns is 'normal'.  $ used for collections holding index data, which do not contain BSON objects in their records.
         * special case for the local.oplog.$main ns -- naming it as such was a mistake.
         * TokuMX doesn't need this special case.
         */
<<<<<<< HEAD
        static bool normal(const StringData &ns) {
            return ns.find('$') == string::npos;
        }
=======
        static bool normal(const StringData& ns);
>>>>>>> 692f185... clean NamespaceString so that it can be the thing passed around

        /**
         * @return true if the ns is an oplog one, otherwise false.
         */
<<<<<<< HEAD
        static bool oplog(const StringData &ns) {
            return ns == "local.oplog.rs" || ns == "local.oplog.refs";
        }

        static bool special(const StringData &ns) {
            return !normal(ns) || isSystem(ns);
        }
=======
        static bool oplog(const StringData& ns);

        static bool special(const StringData& ns);
>>>>>>> 692f185... clean NamespaceString so that it can be the thing passed around

        /**
         * samples:
         *   good
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
<<<<<<< HEAD
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
=======
        static bool validDBName( const StringData& dbin );
>>>>>>> 692f185... clean NamespaceString so that it can be the thing passed around

        /**
         * samples:
         *   good:
         *      foo.bar
         *   bad:
         *      foo.
         *
         * @param ns - a full namesapce (a.b)
         * @return if db.coll is an allowed collection name
         */
<<<<<<< HEAD
        static bool validCollectionName(const StringData &dbcoll) {
            StringData coll = nsToCollectionSubstring(dbcoll);
            return !coll.empty() && normal(dbcoll);
        }
=======
        static bool validCollectionName(const StringData& ns);
>>>>>>> 692f185... clean NamespaceString so that it can be the thing passed around

    private:

        string _ns;
        size_t _dotIndex;
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

<<<<<<< HEAD
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

=======
    // "database.a.b.c" -> "database"
    inline StringData nsToCollectionSubstring( const StringData& ns ) {
        size_t i = ns.find( '.' );
        massert(16886, "nsToCollectionSubstring: no .", i != string::npos );
        return ns.substr( i + 1 );
    }


    /**
     * NamespaceDBHash and NamespaceDBEquals allow you to do something like
     * unordered_map<string,int,NamespaceDBHash,NamespaceDBEquals>
     * and use the full namespace for the string
     * but comparisons are done only on the db piece
     */

    /**
     * this can change, do not store on disk
     */
    int nsDBHash( const string& ns );

    bool nsDBEquals( const string& a, const string& b );

    struct NamespaceDBHash {
        int operator()( const string& ns ) const {
            return nsDBHash( ns );
        }
    };

    struct NamespaceDBEquals {
        bool operator()( const string& a, const string& b ) const {
            return nsDBEquals( a, b );
        }
    };

>>>>>>> 692f185... clean NamespaceString so that it can be the thing passed around
}


#include "mongo/db/namespacestring-inl.h"
