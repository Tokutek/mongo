/** @file index.cpp */

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

#include "pch.h"

#include <boost/checked_delete.hpp>
#include <db.h>

#include "mongo/db/namespace.h"
#include "mongo/db/index.h"
#include "mongo/db/cursor.h"
#include "mongo/db/background.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/storage/key.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/scopeguard.h"

namespace mongo {

    IndexDetails::IndexDetails(const BSONObj &info, bool may_create) : _info(info.getOwned()) {
        string dbname = indexNamespace();
        tokulog(1) << "Opening IndexDetails " << dbname << endl;
        // Open the dictionary. Creates it if necessary.
        int r = storage::db_open(&_db, dbname, info, may_create);
        verify(r == 0);
        if (may_create) {
            addNewNamespaceToCatalog(dbname);
        }
    }

    IndexDetails::~IndexDetails() {
        tokulog(1) << "Closing IndexDetails " << indexNamespace() << endl;
        if (_db) {
            storage::db_close(_db);
        }
    }

    int IndexDetails::keyPatternOffset( const string& key ) const {
        BSONObjIterator i( keyPattern() );
        int n = 0;
        while ( i.more() ) {
            BSONElement e = i.next();
            if ( key == e.fieldName() )
                return n;
            n++;
        }
        return -1;
    }

    int removeFromSysIndexes(const char *ns, const char *name) {
        string system_indexes = cc().database()->name + ".system.indexes";
        BSONObj obj = BSON("ns" << ns << "name" << name);
        tokulog(2) << "removeFromSysIndexes removing " << obj << endl;
        return (int) _deleteObjects(system_indexes.c_str(), obj, false, false);
    }

    /* delete this index.  does NOT clean up the system catalog
       (system.indexes or system.namespaces) -- only NamespaceIndex.
    */
    void IndexDetails::kill_idx(bool can_drop_system) {
        string ns = indexNamespace(); // e.g. foo.coll.$ts_1
        try {

            string pns = parentNS(); // note we need a copy, as parentNS() won't work after the drop() below

            // clean up parent namespace index cache
            NamespaceDetailsTransient::get( pns.c_str() ).deletedIndex();

            storage::db_close(_db);
            _db = NULL;
            storage::db_remove(ns);

            /* important to catch exception here so we can finish cleanup below. */
            try {
                dropNS(ns.c_str(), false, can_drop_system);
            }
            catch(DBException& ) {
                log(2) << "IndexDetails::kill(): couldn't drop ns " << ns << endl;
            }

            if (!mongoutils::str::endsWith(pns.c_str(), ".system.indexes")) {
                int n = removeFromSysIndexes(pns.c_str(), indexName().c_str());
                wassert( n == 1 );
            }
        }
        catch ( DBException &e ) {
            log() << "exception in kill_idx: " << e << ", ns: " << ns << endl;
        }
    }

    void IndexDetails::getKeysFromObject( const BSONObj& obj, BSONObjSet& keys) const {
        getSpec().getKeys( obj, keys );
    }

    const IndexSpec& IndexDetails::getSpec() const {
        SimpleMutex::scoped_lock lk(NamespaceDetailsTransient::_qcMutex);
        return NamespaceDetailsTransient::get_inlock( info()["ns"].valuestr() ).getIndexSpec( this );
    }

    void IndexDetails::insert(const BSONObj &obj, const BSONObj &primary_key, bool overwrite) {
        BSONObjSet keys;
        getKeysFromObject(obj, keys);
        if (keys.size() > 1) {
            const char *ns = parentNS().c_str();
            NamespaceDetails *d = nsdetails(ns);
            const int idxNo = d->idxNo(*this);
            dassert(idxNo >= 0);
            d->setIndexIsMultikey(ns, idxNo);
        }

        for (BSONObjSet::const_iterator ki = keys.begin(); ki != keys.end(); ++ki) {
            if (isIdIndex()) {
                insertPair(*ki, NULL, obj, overwrite);
            } else if (clustering()) {
                insertPair(*ki, &primary_key, obj, overwrite);
            } else {
                insertPair(*ki, &primary_key, BSONObj(), overwrite);
            }
        }
    }

    void IndexDetails::insertPair(const BSONObj &key, const BSONObj *pk, const BSONObj &val, bool overwrite) {
        const size_t buflen = storage::index_key_size(key, pk);
        char buf[buflen];
        storage::index_key_init(buf, buflen, key, pk);

        DBT kdbt, vdbt;
        storage::dbt_init(&kdbt, buf, buflen);
        storage::dbt_init(&vdbt, val.objdata(), val.objsize());

        const int flags = (unique() && !overwrite) ? DB_NOOVERWRITE : 0;
        int r = _db->put(_db, cc().getContext()->transaction().txn(), &kdbt, &vdbt, flags);
        uassert(16433, "key already exists in unique index", r != DB_KEYEXIST);
        if (r != 0) {
            tokulog() << "error inserting " << key << ", " << val << endl;
        } else {
            tokulog(3) << "index " << info()["key"].Obj() << ": inserted " << key << ", pk " << (pk ? *pk : BSONObj()) << ", val " << val << endl;
        }
        verify(r == 0);
    }

    void IndexDetails::deleteObject(const BSONObj &pk, const BSONObj &obj) {
        BSONObjSet keys;
        getKeysFromObject(obj, keys);
        for (BSONObjSet::const_iterator ki = keys.begin(); ki != keys.end(); ++ki) {
            const BSONObj &key = *ki;
            const size_t buflen = storage::index_key_size(key, !isIdIndex() ? &pk : NULL);
            char buf[buflen];
            storage::index_key_init(buf, buflen, key, !isIdIndex() ? &pk : NULL);

            DBT kdbt;
            storage::dbt_init(&kdbt, buf, buflen);

            const int flags = DB_DELETE_ANY;
            int r = _db->del(_db, cc().getContext()->transaction().txn(), &kdbt, flags);
            verify(r == 0);
        }
    }

    // Get a DBC over an index. Must already be in the context of a transction.
    DBC *IndexDetails::cursor() const {
        DBC *cursor;
        const Client::Context::Transaction &txn = cc().getContext()->transaction();
        int r = _db->cursor(_db, txn.txn(), &cursor, 0);
        verify(r == 0);
        return cursor;
    }

    enum toku_compression_method IndexDetails::get_compression_method() {
        enum toku_compression_method ret;
        int r = _db->get_compression_method(_db, &ret);
        verify(r == 0);
        return ret;
    }
    uint32_t IndexDetails::get_page_size() {
        uint32_t ret;
        int r = _db->get_pagesize(_db, &ret);
        verify(r == 0);
        return ret;
    }
    uint32_t IndexDetails::get_read_page_size() {
        uint32_t ret;
        int r = _db->get_readpagesize(_db, &ret);
        verify(r == 0);
        return ret;
    }

    void IndexSpec::reset( const IndexDetails * details ) {
        _details = details;
        reset( details->info() );
    }

    void IndexSpec::reset( const BSONObj& _info ) {
        info = _info;
        keyPattern = info["key"].Obj();
        if ( keyPattern.objsize() == 0 ) {
            out() << info.toString() << endl;
            verify(false);
        }
        _init();
    }
}
