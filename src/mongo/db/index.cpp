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

    // What a mess:
    // - Only the hashed plugin exists. Therefore if we find a plugin
    //   for the key pattern and it's "hashed", you're good.
    // - If it's anything else, there's no such plugin.
    // - If/when we add other plugins, we'll migrate to a better
    //   Index architecture that looks more like Mongo 2.4+
    static bool checkForHashedPlugin(const BSONObj &keyPattern) {
        string pluginName = IndexPlugin::findPluginName( keyPattern );
        if (pluginName == "hashed") {
            return true;
        } else if (pluginName != "") {
            log() << "warning: can't find plugin [" << pluginName << "]" << endl;
        }
        return false;
    }

    IndexDetails::IndexDetails(const BSONObj &info, bool may_create) :
        _db(NULL),
        _info(info.copy()),
        _keyPattern(info["key"].Obj().copy()),
        _unique(info["unique"].trueValue()),
        _hashed(checkForHashedPlugin(_keyPattern)),
        _sparse(info["sparse"].trueValue()),
        _clustering(info["clustering"].trueValue()),
        _descriptor(_keyPattern, _hashed, info["seed"].numberInt(), _sparse, _clustering) {

        // Throws if the index spec is invalid.
        _spec.reset(this);

        string dbname = indexNamespace();
        TOKULOG(1) << "Opening IndexDetails " << dbname << endl;
        // Open the dictionary. Creates it if necessary.
        const int r = storage::db_open(&_db, dbname, info, _descriptor,
                                       may_create, info["background"].trueValue());
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        try {
            if (may_create) {
                addNewNamespaceToCatalog(dbname);
            }
        } catch (...) {
            close();
            throw;
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
                const int n = removeFromSysIndexes(pns, indexName());
                wassert( n == 1 );
            }
        }
        catch ( DBException &e ) {
            log() << "exception in kill_idx: " << e << ", ns: " << ns << endl;
        }
    }

    void IndexDetails::getKeysFromObject(const BSONObj& obj, BSONObjSet& keys) const {
        _descriptor.generateKeys( obj, keys );
    }

    int IndexDetails::uniqueCheckCallback(const DBT *key, const DBT *val, void *extra) {
        UniqueCheckExtra *info = static_cast<UniqueCheckExtra *>(extra);
        try {
            if (key != NULL) {
                // Create two new storage keys that have the pk stripped out. This will tell
                // us whether or not just the 'key' portions are equal, which is what.
                // Stripping out the pk is as easy as calling the key constructor with
                // the original key's buffer but hasPK = false (which will silently ignore
                // any bytes that are found after the first key).
                const storage::Key sKey1(reinterpret_cast<const char *>(key->data), false);
                const storage::Key sKey2(reinterpret_cast<const char *>(info->newKey.buf()), false);
                const int c = info->descriptor.compareKeys(sKey1, sKey2);
                if (c == 0) {
                    info->isUnique = false;
                }
            }
            return 0;
        } catch (const std::exception &ex) {
            info->saveException(ex);
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
        storage::Key sKey(key, hasPK ? &minKey : NULL);
        DBT kdbt = sKey.dbt();

        bool isUnique = true;
        UniqueCheckExtra extra(sKey, _descriptor, isUnique);
        // If the key has a PK, we need to set range in order to find the first
        // key greater than { key, minKey }. If there is no pk then there's
        // just one component to the key, so we can just getf_set to that point.
        const int r = hasPK ? cursor->c_getf_set_range(cursor, 0, &kdbt, uniqueCheckCallback, &extra) :
                              cursor->c_getf_set(cursor, 0, &kdbt, uniqueCheckCallback, &extra);
        if (r != 0 && r != DB_NOTFOUND) {
            extra.throwException();
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

    void IndexDetails::acquireTableLock() {
        const int r = _db->pre_acquire_table_lock(_db, cc().txn().db_txn());
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
            killCurrentOp.checkForInterrupt(); // uasserts if we should stop
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
        _idx(idx), _loader(&_idx._db, 1) {
        _loader.setPollMessagePrefix(str::stream() << "Cold index build progress: "
                                                   << idx.parentNS() << ", key "
                                                   << idx.keyPattern()
                                                   << ":");

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

} // namespace mongo
