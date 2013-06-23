/** @file index.cpp */

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

#include <boost/checked_delete.hpp>

#include "mongo/db/namespace_details.h"
#include "mongo/db/index.h"
#include "mongo/db/curop.h"
#include "mongo/db/cursor.h"
#include "mongo/db/background.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/storage/key.h"
#include "mongo/db/storage/env.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/scopeguard.h"

namespace mongo {

    IndexDetails::IndexDetails(const BSONObj &info, bool may_create) :
        _info(info.copy()),
        _keyPattern(info["key"].Obj().copy()),
        _unique(info["unique"].trueValue()),
        _clustering(info["clustering"].trueValue()) {

        string dbname = indexNamespace();
        TOKULOG(1) << "Opening IndexDetails " << dbname << endl;
        // Open the dictionary. Creates it if necessary.
        const int r = storage::db_open(&_db, dbname, info, may_create);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        if (may_create) {
            addNewNamespaceToCatalog(dbname);
        }
    }

    IndexDetails::~IndexDetails() {
        verify(_db == NULL);
    }

    void IndexDetails::close() {
        TOKULOG(1) << "Closing IndexDetails " << indexNamespace() << endl;
        if (_db) {
            storage::db_close(_db);
            _db = NULL;
        }
    }

    int IndexDetails::keyPatternOffset( const StringData& key ) const {
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

    void IndexDetails::kill_idx() {
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
                removeNamespaceFromCatalog(ns);
            }
            catch(DBException& ) {
                LOG(2) << "IndexDetails::kill(): couldn't drop ns " << ns << endl;
            }

            if (!StringData(pns).endsWith(".system.indexes")) {
                int n = removeFromSysIndexes(pns, indexName());
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
        SimpleRWLock::Exclusive lk(NamespaceDetailsTransient::_qcRWLock);
        return NamespaceDetailsTransient::get_inlock( info()["ns"].String() ).getIndexSpec( this );
    }

    int IndexDetails::uniqueCheckCallback(const DBT *key, const DBT *val, void *extra) {
        UniqueCheckExtra *info = static_cast<UniqueCheckExtra *>(extra);
        try {
            if (key != NULL) {
                const storage::Key sKey(key);
                const int c = info->newkey.woCompare(sKey.key(), info->ordering);
                if (c == 0) {
                    info->isUnique = false;
                }
            }
            return 0;
        } catch (std::exception &e) {
            info->ex = &e;
        }
        return -1;
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

        IndexDetails::Cursor c(*this, DB_SERIALIZABLE);
        DBC *cursor = c.dbc();

        const bool hasPK = pk != NULL;
        storage::Key skey(key, hasPK ? &minKey : NULL);
        DBT kdbt = skey.dbt();

        bool isUnique = true;
        const Ordering &ordering(*reinterpret_cast<const Ordering *>(_db->cmp_descriptor->dbt.data));
        UniqueCheckExtra extra(key, ordering, isUnique);
        // If the key has a PK, we need to set range in order to find the first
        // key greater than { key, minKey }. If there is no pk then there's
        // just one component to the key, so we can just getf_set to that point.
        const int r = hasPK ? cursor->c_getf_set_range(cursor, 0, &kdbt, uniqueCheckCallback, &extra) :
                              cursor->c_getf_set(cursor, 0, &kdbt, uniqueCheckCallback, &extra);
        if (r != 0 && r != DB_NOTFOUND) {
            storage::handle_ydb_error(r);
        }
        if (!isUnique) {
            uassertedDupKey(key);
        }
    }

    void IndexDetails::uassertedDupKey(const BSONObj &key) const {
        uasserted(ASSERT_ID_DUPKEY, mongoutils::str::stream()
                                    << "E11000 duplicate key error, " << key
                                    << " already exists in unique index");
    }

    void IndexDetails::insertPair(const BSONObj &key, const BSONObj *pk, const BSONObj &val, uint64_t flags) {
        if (unique() && !(flags & NamespaceDetails::NO_UNIQUE_CHECKS)) {
            uniqueCheck(key, pk);
        }

        storage::Key skey(key, pk);
        DBT kdbt = skey.dbt();
        DBT vdbt = storage::make_dbt(NULL, 0);
        if (clustering()) {
            vdbt = storage::make_dbt(val.objdata(), val.objsize());
        }

        // We already did the unique check above. We can just pass flags of zero.
        const int put_flags = (flags & NamespaceDetails::NO_LOCKTREE) ? DB_PRELOCKED_WRITE : 0;
        int r = _db->put(_db, cc().txn().db_txn(), &kdbt, &vdbt, put_flags);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        TOKULOG(3) << "index " << info()["key"].Obj() << ": inserted " << key << ", pk " << (pk ? *pk : BSONObj()) << ", val " << val << endl;
    }

    void IndexDetails::deletePair(const BSONObj &key, const BSONObj *pk, uint64_t flags) {
        storage::Key skey(key, pk);
        DBT kdbt = skey.dbt();

        const int del_flags = ((flags & NamespaceDetails::NO_LOCKTREE) ? DB_PRELOCKED_WRITE : 0) | DB_DELETE_ANY;
        int r = _db->del(_db, cc().txn().db_txn(), &kdbt, del_flags);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

    enum toku_compression_method IndexDetails::getCompressionMethod() const {
        enum toku_compression_method ret;
        int r = _db->get_compression_method(_db, &ret);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        return ret;
    }

    uint32_t IndexDetails::getPageSize() const {
        uint32_t ret;
        int r = _db->get_pagesize(_db, &ret);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        return ret;
    }

    uint32_t IndexDetails::getReadPageSize() const {
        uint32_t ret;
        int r = _db->get_readpagesize(_db, &ret);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        return ret;
    }

    void IndexDetails::getStat64(DB_BTREE_STAT64* stats) const {
        int r = _db->stat64(_db, NULL, stats);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

    int IndexDetails::hot_opt_callback(void *extra, float progress) {
        int retval = 0;
        uint64_t iter = *(uint64_t *)extra;
        try {
            killCurrentOp.checkForInterrupt(false); // uasserts if we should stop
        } catch (DBException &e) {
            retval = 1;
        }
        iter++;
        return retval;
    }

    void IndexDetails::optimize() {        
        int r = _db->optimize(_db);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        uint64_t iter = 0;
        r = _db->hot_optimize(_db, hot_opt_callback, &iter);
        if (r != 0) {
            uassert(16810, mongoutils::str::stream() << "reIndex query killed ", false);
        }
    }

    void IndexSpec::reset( const IndexDetails * details ) {
        _details = details;
        reset( details->info() );
    }

    void IndexSpec::reset( const BSONObj& _info ) {
        info = _info;
        keyPattern = info["key"].Obj();
        verify( keyPattern.objsize() != 0 );
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
    
    /* ---------------------------------------------------------------------- */

    IndexDetails::Builder::Builder(IndexDetails &idx) :
        _idx(idx), _loader(_idx._db) {
    }

    void IndexDetails::Builder::insertPair(const BSONObj &key, const BSONObj *pk, const BSONObj &val) {
        storage::Key skey(key, pk);
        DBT kdbt = skey.dbt();
        DBT vdbt = storage::make_dbt(NULL, 0);
        if (_idx.clustering()) {
            vdbt = storage::make_dbt(val.objdata(), val.objsize());
        }
        const int r = _loader.put(&kdbt, &vdbt);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

    void IndexDetails::Builder::done() {
        const int r = _loader.close();
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }
}
