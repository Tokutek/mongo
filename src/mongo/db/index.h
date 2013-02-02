// index.h

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

#include "mongo/pch.h"

#include <vector>

#include "mongo/db/indexkey.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/key.h"
#include "mongo/db/namespace.h"

namespace mongo {

    /* Details about a particular index. There is one of these effectively for each object in
       system.namespaces (although this also includes the head pointer, which is not in that
       collection).

       ** MemoryMapped Record ** (i.e., this is on disk data)
     */
    class IndexDetails {
    public:
        /**
         * btree head disk location
         * TODO We should make this variable private, since btree operations
         * may change its value and we don't want clients to rely on an old
         * value.  If we create a btree class, we can provide a btree object
         * to clients instead of 'head'.
         */
        //DiskLoc head;
        // TODO: TokuDB this will probably be an open DB* object.

        /* Location of index info object. Format:

             { name:"nameofindex", ns:"parentnsname", key: {keypattobject}
               [, unique: <bool>, background: <bool>, v:<version>]
             }

           This object is in the system.indexes collection.  Note that since we
           have a pointer to the object here, the object in system.indexes MUST NEVER MOVE.
        */
        //DiskLoc info;
        // TODO: TokuDB this stuff will probably go in the descriptor

        /* extract key value from the query object
           e.g., if key() == { x : 1 },
                 { x : 70, y : 3 } -> { x : 70 }
        */
        BSONObj getKeyFromQuery(const BSONObj& query) const {
            BSONObj k = keyPattern();
            BSONObj res = query.extractFieldsUnDotted(k);
            return res;
        }

        /* pull out the relevant key objects from obj, so we
           can index them.  Note that the set is multiple elements
           only when it's a "multikey" array.
           keys will be left empty if key not found in the object.
        */
        void getKeysFromObject( const BSONObj& obj, BSONObjSet& keys) const;

        /* get the key pattern for this object.
           e.g., { lastname:1, firstname:1 }
        */
        BSONObj keyPattern() const {
            ::abort();
            return BSONObj();
            //return info.obj().getObjectField("key");
        }

        /**
         * @return offset into keyPattern for key
                   -1 if doesn't exist
         */
        int keyPatternOffset( const string& key ) const;
        bool inKeyPattern( const string& key ) const { return keyPatternOffset( key ) >= 0; }

        /* true if the specified key is in the index */
        bool hasKey(const BSONObj& key);

        // returns name of this index's storage area
        // database.table.$index
        string indexNamespace() const {
#if 0
            BSONObj io = info.obj();
            string s;
            s.reserve(Namespace::MaxNsLen);
            s = io.getStringField("ns");
            verify( !s.empty() );
            s += ".$";
            s += io.getStringField("name");
            return s;
#endif
            ::abort();
            return string();
        }

        string indexName() const { // e.g. "ts_1"
#if 0
            BSONObj io = info.obj();
            return io.getStringField("name");
#endif
            ::abort();
            return string();
        }

        static bool isIdIndexPattern( const BSONObj &pattern ) {
            BSONObjIterator i(pattern);
            BSONElement e = i.next();
            //_id index must have form exactly {_id : 1} or {_id : -1}.
            //Allows an index of form {_id : "hashed"} to exist but
            //do not consider it to be the primary _id index
            if(! ( strcmp(e.fieldName(), "_id") == 0
                    && (e.numberInt() == 1 || e.numberInt() == -1)))
                return false;
            return i.next().eoo();
        }

        /* returns true if this is the _id index. */
        bool isIdIndex() const {
            return isIdIndexPattern( keyPattern() );
        }

        /* gets not our namespace name (indexNamespace for that),
           but the collection we index, its name.
           */
        string parentNS() const {
#if 0
            BSONObj io = info.obj();
            return io.getStringField("ns");
#endif
            ::abort();
            return string();
        }

        /** @return true if index has unique constraint */
        bool unique() const {
#if 0
            BSONObj io = info.obj();
            return io["unique"].trueValue() ||
                   /* temp: can we juse make unique:true always be there for _id and get rid of this? */
                   isIdIndex();
#endif
            ::abort();
            return false;
        }

        /** delete this index.  does NOT clean up the system catalog
            (system.indexes or system.namespaces) -- only NamespaceIndex.
        */
        void kill_idx();

        const IndexSpec& getSpec() const;

        string toString() const {
            //return info.obj().toString();
            ::abort();
            return string();
        }

#if 0
        /** @return true if supported.  supported means we can use the index, including adding new keys.
                    it may not mean we can build the index version in question: we may not maintain building 
                    of indexes in old formats in the future.
        */
        // tokudb: tokudb indexes are version 2
        static bool isASupportedIndexVersionNumber(int v) { return v == 2; } // only tokudb indexes are supported

        /** @return the interface for this interface, which varies with the index version.
            used for backward compatibility of index versions/formats.
        */
        IndexInterface& idxInterface() const { 
            int v = version();
            dassert( isASupportedIndexVersionNumber(v) );
            return *iis[v];
        }

        static IndexInterface *iis[];
#endif
    };

} // namespace mongo
