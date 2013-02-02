/** @file toku/index.cpp */

/**
*    Copyright (C) 2012 Tokutek Inc.
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

#include <tokudb.h>

#include "db/toku/env.h"
#include "db/toku/index.h"
#include "db/toku/cursor.h"
#include "db/toku/dbt-bson-inl.h"
#include "db/toku/invariant.h"

#include "db/namespace_details.h"
#include "db/btree.h"

namespace mongo {
#if 0
    void dump_bson(BSONObj obj) {
        std::cout << obj.toString() << std::endl;
    }

    // drop the index in the environment
    void IndexInterfaceTokuDB::dropIndex(const IndexDetails &idx) { 
        toku::env_drop_index(idx);
    }

    struct cursor_getf_cb_extra {
        BSONObj *obj;
        DiskLoc *loc;
    };

    // return the diskloc associated with the given key in the index
    DiskLoc IndexInterfaceTokuDB::findSingle(const IndexDetails &idx,
            const DiskLoc &thisLoc, const BSONObj &key) const {
        const char *ns = idx.parentNS().c_str();
        NamespaceDetails *nsd = nsindex(ns)->details(ns);
        invariant(nsd);
        TokuDBCursor cursor(nsd, nsd->idxNo(idx), idx);
        // initialize a cursor over the interval [key, key]
        // where endkeyinclusive = true and direction = 1
        cursor.init(key, key, true, 1);
        DiskLoc loc = DiskLoc();
        if (cursor.ok() && cursor.currKey() == key) {
            loc = cursor.currLoc();
        }
        return loc;
    }

    // stolen from btree.cpp
    // generate an error string for a duplicate key in the given idx
    static std::string dup_key_error_string(const IndexDetails &idx,
            const BSONObj &key) {
        std::stringstream ss;
        ss << "E11000 duplicate key error ";
        ss << "index: " << idx.indexNamespace() << "  ";
        ss << "dup key: " << key.toString();
        return ss.str();
    }

    
    // index the given key, associating it with the recordLoc
    // - not sure what toplevel is.
    int IndexInterfaceTokuDB::insert(const DiskLoc thisLoc, const DiskLoc recordLoc,
            const BSONObj &key, const Ordering &order, bool dupsAllowed,
            IndexDetails& idx, const BSONObj *obj, bool toplevel) const {
        DB *db = toku::env_get_db_handle_by_idx(idx);
        // get an index key that is (bson obj key, recordLoc)
        char key_buf[toku::index_key_size(key)];
        DBT index_key = toku::generate_index_key(key_buf, key, recordLoc);
        DBT val_dbt;
        if (obj) {
            // if we were given the object, then that's the value.
            val_dbt = toku::init_dbt_from_bson_obj(*obj);
        } else {
            // if we weren't given the object, then store an empty one.
            val_dbt = toku::init_dbt_from_bson_obj(BSONObj());
        }
        if (!dupsAllowed) {
            DiskLoc loc = findSingle(idx, thisLoc, key);
            if (!loc.isNull()) {
                // this is how mongo deals with dup key errors.
                // they throw a user exception that says so.
                const std::string error_string = dup_key_error_string(idx, key);
                uasserted(ASSERT_ID_DUPKEY, error_string);
            }
        } 
        int r = db->put(db, NULL, &index_key, &val_dbt, 0);
        invariant(r == 0);
        return r;
    }

    int IndexInterfaceTokuDB::bt_insert(const DiskLoc thisLoc, const DiskLoc recordLoc,
                  const BSONObj &key, const Ordering &order, bool dupsAllowed,
                  IndexDetails &idx, bool toplevel) const {
        // pass null for the obj so the insert does not cluster
        return insert(thisLoc, recordLoc, key, order, dupsAllowed, idx, NULL, toplevel);
    }

    // this version of bt_insert takes the full document, for clustering
    int IndexInterfaceTokuDB::bt_insert_clustering(const DiskLoc thisLoc, const DiskLoc recordLoc,
            const BSONObj& key, const Ordering &order, bool dupsAllowed,
            IndexDetails& idx, const BSONObj &obj, bool toplevel) const {
        // pass the address of the object so the inserts clusters
        return insert(thisLoc, recordLoc, key, order, dupsAllowed, idx, &obj, toplevel);
    }

    // unindex the given key. the index key is bson obj key + recordloc
    bool IndexInterfaceTokuDB::unindex(const DiskLoc thisLoc, IndexDetails &idx,
            const BSONObj& key, const DiskLoc recordLoc) const {
        DB *db = toku::env_get_db_handle_by_idx(idx);
        // get an index key that is (bson obj key, recordLoc)
        char key_buf[toku::index_key_size(key)];
        DBT index_key = toku::generate_index_key(key_buf, key, recordLoc);
        const int flags = DB_DELETE_ANY;
        int r = db->del(db, NULL, &index_key, flags);
        invariant(r == 0);
        return r == 0 ? true : false;
    }
#endif
} /* namespace mongo */
