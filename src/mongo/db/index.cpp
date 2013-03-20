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

    IndexDetails::IndexDetails(const BSONObj &info, bool may_create) :
        _info(info.copy()),
        _keyPattern(info["key"].Obj().copy()),
        _unique(info["unique"].trueValue() || isIdIndexPattern(_keyPattern)),
        _clustering(info["clustering"].trueValue()) {

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

    void IndexDetails::getKeysFromObject(const BSONObj& obj, BSONObjSet& keys) const {
        getSpec().getKeys( obj, keys );
    }

    const IndexSpec& IndexDetails::getSpec() const {
        SimpleMutex::scoped_lock lk(NamespaceDetailsTransient::_qcMutex);
        return NamespaceDetailsTransient::get_inlock( info()["ns"].valuestr() ).getIndexSpec( this );
    }

    void IndexDetails::uniqueCheckCallback(const BSONObj &newkey, const BSONObj &oldkey, bool &isUnique) const {
        const BSONObj keyPattern(static_cast<char *>(_db->cmp_descriptor->dbt.data));
        const Ordering ordering = Ordering::make(keyPattern);
        const int c = newkey.woCompare(oldkey, ordering);
        if (c == 0) {
            isUnique = false;
        }
    }

    struct UniqueCheckExtra {
        const IndexDetails &d;
        const BSONObj &newkey;
        bool &isUnique;
        UniqueCheckExtra(const IndexDetails &_d, const BSONObj &_newkey, bool &_isUnique)
                : d(_d), newkey(_newkey), isUnique(_isUnique) {}
    };

    int uniqueCheckCallback(const DBT *key, const DBT *val, void *extra) {
        if (key != NULL) {
            const BSONObj oldkey(static_cast<char *>(key->data));
            verify(oldkey.objsize() <= (int) key->size);
            verify(!oldkey.isEmpty());
            if (oldkey.objsize() < (int) key->size) {
                // Sanity check that the pk is what we expect, but we won't use it to check uniqueness.
                const BSONObj pk(static_cast<char *>(key->data) + oldkey.objsize());
                verify(!pk.isEmpty());
                verify(pk.objsize() == ((int) key->size) - oldkey.objsize());
            }
            UniqueCheckExtra *e = static_cast<UniqueCheckExtra *>(extra);
            e->d.uniqueCheckCallback(e->newkey, oldkey, e->isUnique);
        }
        return 0;
    }

    void IndexDetails::uniqueCheck(const BSONObj &key, const BSONObj *pk) const {
        BSONObjIterator it(key);
        while (it.more()) {
            BSONElement id = it.next();
            if (!id.ok()) {
                // If one of the key fields is null, we just insert it.
                return;
            }
        }

        IndexDetails::Cursor c(this, DB_SERIALIZABLE);
        DBC *cursor = c.dbc();

        storage::Key skey(key, pk != NULL ? &minKey : NULL);
        DBT kdbt = skey.dbt();

        bool isUnique = true;
        UniqueCheckExtra extra(*this, key, isUnique);
        int r = cursor->c_getf_set_range(cursor, 0, &kdbt, mongo::uniqueCheckCallback, &extra);
        verify(r == 0 || r == DB_NOTFOUND || r == DB_LOCK_NOTGRANTED || r == DB_LOCK_DEADLOCK);

        uassert(16453, "tokudb lock not granted", r != DB_LOCK_NOTGRANTED);
        uassert(16454, "tokudb deadlock", r != DB_LOCK_DEADLOCK);

        uassert(ASSERT_ID_DUPKEY, mongoutils::str::stream() << "E11000 duplicate key error, " << key << " already exists in unique index", isUnique);
    }

    void IndexDetails::insertPair(const BSONObj &key, const BSONObj *pk, const BSONObj &val, bool overwrite) {
        if (unique()) {
            uniqueCheck(key, pk);
        }

        storage::Key skey(key, pk);
        DBT kdbt = skey.dbt();
        DBT vdbt = storage::make_dbt(val.objdata(), val.objsize());

        // TODO: We already did the unique check above. Can we just pass flags of 0?
        const int flags = (unique() && !overwrite) ? DB_NOOVERWRITE : 0;
        int r = _db->put(_db, cc().txn().db_txn(), &kdbt, &vdbt, flags);
        if (r != 0) {
            tokulog() << "error inserting " << key << ", " << val << endl;
        } else {
            tokulog(3) << "index " << info()["key"].Obj() << ": inserted " << key << ", pk " << (pk ? *pk : BSONObj()) << ", val " << val << endl;
        }
        verify(r == 0); // TODO: Lock not granted, deadlock?
    }

    void IndexDetails::deletePair(const BSONObj &key, const BSONObj *pk, const BSONObj &obj) {
        storage::Key skey(key, pk);
        DBT kdbt = skey.dbt();

        const int flags = DB_DELETE_ANY;
        int r = _db->del(_db, cc().txn().db_txn(), &kdbt, flags);
        verify(r == 0); // TODO: Lock not granted, deadlock?
    }

    enum toku_compression_method IndexDetails::getCompressionMethod() const {
        enum toku_compression_method ret;
        int r = _db->get_compression_method(_db, &ret);
        verify(r == 0);
        return ret;
    }
    uint32_t IndexDetails::getPageSize() const {
        uint32_t ret;
        int r = _db->get_pagesize(_db, &ret);
        verify(r == 0);
        return ret;
    }
    uint32_t IndexDetails::getReadPageSize() const {
        uint32_t ret;
        int r = _db->get_readpagesize(_db, &ret);
        verify(r == 0);
        return ret;
    }
    void IndexDetails::getStat64(DB_BTREE_STAT64* stats) const {
        int r = _db->stat64(_db, NULL, stats);
        verify(r == 0);
    }

    static int hot_opt_callback(void *extra, float progress) {
        int retval = 0;
        uint64_t iter = *(uint64_t *)extra;
        try {
            if ((iter % 1000) == 0) {                
                killCurrentOp.checkForInterrupt(false); // uasserts if we should stop
            }
        } catch (DBException &e) {
            retval = 1;
        }
        iter++;
        return retval;
    }

    void IndexDetails::optimize() {        
        int error = _db->optimize(_db);
        verify(error == 0);
        uint64_t iter = 0;
        error = _db->hot_optimize(_db, hot_opt_callback, &iter);
        if (error) {
            uassert(16450, mongoutils::str::stream() << "reIndex query killed ", false);
        }
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

    IndexStats::IndexStats(const IndexDetails &idx)
            : _name(idx.indexName()),
              _compressionMethod(idx.getCompressionMethod()),
              _readPageSize(idx.getReadPageSize()),
              _pageSize(idx.getPageSize()) {
        idx.getStat64(&_stats);
    }
    
    BSONObj IndexStats::bson(int scale) const {
        BSONObjBuilder b;
        b.append("name", _name);
        b.appendNumber("count", (long long) _stats.bt_nkeys);
        b.appendNumber("size", (long long) _stats.bt_dsize/scale);
        b.appendNumber("avgObjSize", (_stats.bt_nkeys == 0
                                      ? 0.0
                                      : ((double)_stats.bt_dsize/_stats.bt_nkeys)));
        b.appendNumber("storageSize", (long long) _stats.bt_fsize / scale);
        b.append("pageSize", _pageSize / scale);
        b.append("readPageSize", _readPageSize / scale);
        // fill compression
        switch(_compressionMethod) {
        case TOKU_NO_COMPRESSION:
            b.append("compression", "uncompressed");
            break;
        case TOKU_ZLIB_METHOD:
            b.append("compression", "zlib");
            break;
        case TOKU_ZLIB_WITHOUT_CHECKSUM_METHOD:
            b.append("compression", "zlib");
            break;
        case TOKU_QUICKLZ_METHOD:
            b.append("compression", "quicklz");
            break;
        case TOKU_LZMA_METHOD:
            b.append("compression", "lzma");
            break;
        case TOKU_FAST_COMPRESSION_METHOD:
            b.append("compression", "fast");
            break;
        case TOKU_SMALL_COMPRESSION_METHOD:
            b.append("compression", "small");
            break;
        case TOKU_DEFAULT_COMPRESSION_METHOD:
            b.append("compression", "default");
            break;
        default:
            b.append("compression", "unknown");
            break;
        }
        return b.obj();
        // TODO: (Zardosht) Need to figure out how to display these dates
        /*
        Date_t create_date(_stats.bt_create_time_sec);
        Date_t modify_date(_stats.bt_modify_time_sec);
        bson_stats->append("create time", create_date);
        bson_stats->append("last modify time", modify_date);
        */
    }
}
