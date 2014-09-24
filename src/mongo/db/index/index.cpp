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

#include "mongo/db/collection.h"
#include "mongo/db/index.h"
#include "mongo/db/index/2d.h"
#include "mongo/db/index/hashed.h"
#include "mongo/db/index/haystack.h"
#include "mongo/db/index/partitioned.h"
#include "mongo/db/index/s2.h"
#include "mongo/db/curop.h"
#include "mongo/db/cursor.h"
#include "mongo/db/key_generator.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/storage/key.h"
#include "mongo/db/storage/env.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/stringutils.h"

namespace mongo {

    static string findSpecialIndexName(const BSONObj &keyPattern) {
        string special = "";
        for (BSONObjIterator i(keyPattern); i.more(); ) {
            const BSONElement &e = i.next();
            if (e.type() == String) {
                uassert( 13007, "can only have 1 special index / bad index key pattern" ,
                                special.size() == 0 || special == e.String() );
                special = e.String();
            }
        }
        return special;
    }

    shared_ptr<IndexInterface> IndexInterface::make(const BSONObj &info,
                                                    const bool may_create,
                                                    const bool use_memcmp_magic) {
        shared_ptr<IndexInterface> idx;
        const string special = findSpecialIndexName(info["key"].Obj());
        if (special == "hashed") {
            idx.reset(new HashedIndex(info));
        } else if (special == "2d") {
            idx.reset(new TwoDIndex(info));
        } else if (special == "2dsphere") {
            idx.reset(new S2Index(info));
        } else if (special == "geoHaystack") {
            idx.reset(new HaystackIndex(info));
        } else {
            if (special != "") {
                warning() << "cannot find special index [" << special << "]" << endl;
            }
            idx.reset(new IndexInterface(info));
        }
        bool ok = idx->open(may_create, use_memcmp_magic);
        if (!ok) {
            // This signals Collection::make that we got ENOENT due to #673
            return shared_ptr<IndexInterface>();
        }
        return idx;
    }

    static BSONObj stripDropDups(const BSONObj &obj) {
        BSONObjBuilder b;
        for (BSONObjIterator it(obj); it.more(); ) {
            BSONElement e = it.next();
            if (StringData(e.fieldName()) == "dropDups") {
                warning() << "dropDups is not supported because it deletes arbitrary data." << endl;
                warning() << "We'll proceed without it but if there are duplicates, the index build will fail." << endl;
            } else {
                b.append(e);
            }
        }
        return b.obj();
    }

    IndexDetails::IndexDetails(const BSONObj &info) :
        _info(stripDropDups(info)),
        _keyPattern(info["key"].Obj().copy()),
        _unique(info["unique"].trueValue()),
        _sparse(info["sparse"].trueValue()),
        _clustering(info["clustering"].trueValue()) {
        verify(!_info.isEmpty());
        verify(!_keyPattern.isEmpty());
        if (isIdIndex() && !unique()) {
            uasserted(17365, "_id index cannot be non-unique");
        }

        // Create a standard key generator for this index
        vector<const char *> fieldNames;
        for (BSONObjIterator it(_keyPattern); it.more(); ) {
            const BSONElement e = it.next();
            fieldNames.push_back(e.fieldName());
        }
        _keyGenerator.reset(new KeyGenerator(fieldNames, _sparse));
    }

    IndexInterface::IndexInterface(const BSONObj& info) :
        IndexDetails(info),
        _descriptor(new Descriptor(_keyPattern, Descriptor::Basic, _sparse, _clustering)) {
    }

    // Open the dictionary. Creates it if necessary.
    bool IndexInterface::open(const bool may_create, const bool use_memcmp_magic) {
        const string dname = indexNamespace();

        TOKULOG(1) << "Opening IndexDetails " << dname << endl;
        try {
            // We use the memcmp magic API only for single-key, ascending _id indexes,
            // because the _id field is always unique (and therefore we can simply
            // compare the OID fields if they exist and that will be sufficient)
            if (use_memcmp_magic) {
                verify(_unique);
            }
            _db.reset(new storage::Dictionary(dname, _info, *_descriptor, may_create,
                                              _info["background"].trueValue(), use_memcmp_magic));
            return true;
        } catch (storage::Dictionary::NeedsCreate) {
            if (cc().upgradingSystemUsers() &&
                isSystemUsersCollection(parentNS()) &&
                keyPattern() == oldSystemUsersKeyPattern) {
                // We're upgrading the system.users collection, and we are missing the old index.
                // That's ok, we'll signal the caller about this by returning a NULL pointer from
                // IndexInterface::make.  See #673
                return false;
            }
            // This dictionary must exist on disk if we think it should exist.
            // This error only gets thrown if may_create is false, which happens when we're
            // trying to open a collection for which we have serialized info.
            // Therefore, this is a fatal non-user error.
            msgasserted(16988, mongoutils::str::stream() << "dictionary " << dname
                               << " should exist, but we got ENOENT");
        }
    }

    IndexDetails::~IndexDetails() {
    }

    IndexInterface::~IndexInterface() {
        try {
            close();
        } catch (DBException &ex) {
            problem() << "~IndexDetails " << _keyPattern
                      << ": Caught exception: " << ex.what() << endl;
        }
    }

    void IndexInterface::close() {
        if (_db) {
            shared_ptr<storage::Dictionary> db = _db;
            _db.reset();
            const int r = db->close();
            if (r != 0) {
                storage::handle_ydb_error(r);
            }
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

    void IndexInterface::kill_idx() {
        const string ns = indexNamespace();

        close();
        storage::db_remove(ns);
    }

    bool IndexInterface::changeAttributes(const BSONObj &info, BSONObjBuilder &wasBuilder) {
        if (!_db->changeAttributes(info, wasBuilder)) {
            return false;
        }

        BSONObj was = wasBuilder.done();
        // need to merge new values in
        BSONObjBuilder infoBuilder;
        for (BSONObjIterator it(_info); it.more(); ++it) {
            BSONElement e = *it;
            StringData fn(e.fieldName());
            if (fn != "name" && was.hasField(fn)) {
                dassert(info[fn].ok());
                infoBuilder.append(info[fn]);
            } else {
                infoBuilder.append(_info[fn]);
            }
        }
        for (BSONObjIterator it(was); it.more(); ++it) {
            BSONElement e = *it;
            StringData fn(e.fieldName());
            if (!_info.hasField(fn)) {
                infoBuilder.append(info[fn]);
            }
        }
        _info = infoBuilder.obj();
        return true;
    }

    void IndexInterface::getKeysFromObject(const BSONObj &obj, BSONObjSet &keys) const {
        _keyGenerator->getKeys(obj, keys);
    }

    IndexDetails::Suitability IndexDetails::suitability(const FieldRangeSet &queryConstraints,
                                                        const BSONObj &order) const {
        // This is a quick first pass to determine the suitability of the index.  It produces some
        // false positives (returns HELPFUL for some indexes which are not particularly). When we
        // return HELPFUL a more precise determination of utility is done by the query optimizer.

        // check whether any field in the index is constrained at all by the query
        BSONForEach( elt, _keyPattern ){
            const FieldRange& frange = queryConstraints.range( elt.fieldName() );
            if( ! frange.universal() )
                return IndexDetails::HELPFUL;
        }
        // or whether any field in the desired sort order is in the index
        set<string> orderFields;
        order.getFieldNames( orderFields );
        BSONForEach( k, _keyPattern ) {
            if ( orderFields.find( k.fieldName() ) != orderFields.end() )
                return IndexDetails::HELPFUL;
        }
        return IndexDetails::USELESS;
    }

    int IndexInterface::uniqueCheckCallback(const DBT *key, const DBT *val, void *extra) {
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

    void IndexInterface::uniqueCheck(const BSONObj &key, const BSONObj &pk) const {
        shared_ptr<storage::Cursor> c = getCursor(DB_SERIALIZABLE | DB_RMW);
        DBC *cursor = c->dbc();

        // We need to check if a secondary key, 'key', exists. We'd like to only
        // lock just the range of the index that may contain that secondary key,
        // if it exists. That range is { key, minKey } -> { key, maxKey }, where
        // the second part of the compound key is the appended primary key.
        storage::Key leftSKey(key, &minKey);
        storage::Key rightSKey(key, &maxKey);
        DBT start = leftSKey.dbt();
        DBT end = rightSKey.dbt();
        int r = cursor->c_set_bounds(cursor, &start, &end, true, 0);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }

        bool isUnique = true;
        UniqueCheckExtra extra(leftSKey, *_descriptor, isUnique);
        const int flags = DB_PRELOCKED | DB_PRELOCKED_WRITE; // prelocked above
        r = cursor->c_getf_set_range(cursor, flags, &start, uniqueCheckCallback, &extra);
                              
        if (r != 0 && r != DB_NOTFOUND) {
            extra.throwException();
            storage::handle_ydb_error(r);
        }
        if (!isUnique) {
            uassertedDupKey(key);
        }
    }

    void IndexInterface::uassertedDupKey(const BSONObj &key) const {
        uasserted(ASSERT_ID_DUPKEY, mongoutils::str::stream()
                                    << "E11000 duplicate key error, " << key
                                    << " already exists in unique index");
    }

    void IndexInterface::acquireTableLock() {
        const int r = db()->pre_acquire_table_lock(db(), cc().txn().db_txn());
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

    void IndexInterface::updatePair(const BSONObj &key, const BSONObj *pk, const BSONObj &msg, uint64_t flags) {
        storage::Key skey(key, pk);
        DBT kdbt = skey.dbt();
        DBT vdbt = storage::dbt_make(msg.objdata(), msg.objsize());

        const int update_flags = (flags & Collection::NO_LOCKTREE) ? DB_PRELOCKED_WRITE : 0;
        const int r = db()->update(db(), cc().txn().db_txn(), &kdbt, &vdbt, update_flags);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        TOKULOG(3) << "index " << info()["key"].Obj() << ": sent update to "
                   << key << ", pk " << (pk ? *pk : BSONObj()) << ", msg " << msg << endl;
    }

    enum toku_compression_method IndexInterface::getCompressionMethod() const {
        enum toku_compression_method ret;
        int r = db()->get_compression_method(db(), &ret);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        return ret;
    }

    uint32_t IndexInterface::getFanout() const {
        uint32_t ret;
        int r = db()->get_fanout(db(), &ret);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        return ret;
    }

    uint32_t IndexInterface::getPageSize() const {
        uint32_t ret;
        int r = db()->get_pagesize(db(), &ret);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        return ret;
    }

    uint32_t IndexInterface::getReadPageSize() const {
        uint32_t ret;
        int r = db()->get_readpagesize(db(), &ret);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        return ret;
    }

    void IndexInterface::getStat64(DB_BTREE_STAT64* stats) const {
        int r = db()->stat64(db(), NULL, stats);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

    int IndexInterface::hot_optimize_callback(void *extra, float progress) {
        struct hot_optimize_callback_extra *info =
                reinterpret_cast<hot_optimize_callback_extra *>(extra);

        try {
            killCurrentOp.checkForInterrupt(); // uasserts if we should stop
            if (info->timeout > 0 && info->timer.seconds() > info->timeout) {
                // optimize timed out
                return 1;
            } else {
                if (info->pm.report(progress) && cc().curop()) {
                    std::string status = info->pm.toString();
                    cc().curop()->setMessage(status.c_str());
                }
                return 0;
            }
        } catch (std::exception &e) {
            info->saveException(e);
            return -1;
        }
    }

    void IndexInterface::optimize(const storage::Key &leftSKey, const storage::Key &rightSKey,
                                const bool sendOptimizeMessage, const int timeout,
                                uint64_t *loops_run) {
        if (sendOptimizeMessage) {
            const int r = db()->optimize(db());
            if (r != 0) {
                storage::handle_ydb_error(r);
            }
        }

        std::stringstream pmss;
        pmss << "Optimizing index " << indexNamespace() << " from " << leftSKey.key() << " to " << rightSKey.key();

        DBT left = leftSKey.dbt();
        DBT right = rightSKey.dbt();
        struct hot_optimize_callback_extra extra(timeout, pmss.str());
        const int r = db()->hot_optimize(db(), &left, &right, hot_optimize_callback, &extra, loops_run);
        if (r < 0) { // we return -1 on interrupt, 1 on timeout (no "error" on timeout)
            extra.throwException();
            storage::handle_ydb_error(r);
        }
    }

    IndexDetails::Stats IndexDetails::getStats() const {
        DB_BTREE_STAT64 st;
        getStat64(&st);
        Stats stats;
        stats.name = indexName();
        stats.count = st.bt_nkeys;
        stats.dataSize = st.bt_dsize;
        stats.storageSize = st.bt_fsize;
        stats.pageSize = getPageSize();
        stats.readPageSize = getReadPageSize();
        stats.compressionMethod = getCompressionMethod();
        stats.fanout = getFanout();
        stats.queries = _accessStats.queries.load();
        stats.nscanned = _accessStats.nscanned.load();
        stats.nscannedObjects = _accessStats.nscannedObjects.load();
        stats.inserts = _accessStats.inserts.load();
        stats.deletes = _accessStats.deletes.load();
        return stats;
    }

    void IndexDetails::Stats::appendInfo(BSONObjBuilder &b, int scale) const {
        b.append("name", name);
        b.appendNumber("count", (long long) count);
        b.appendNumber("size", (long long) dataSize / scale);
        b.appendNumber("avgObjSize", count == 0 ? 0.0 : double(dataSize) / double(count));
        b.appendNumber("storageSize", (long long) storageSize / scale);
        b.append("pageSize", pageSize / scale);
        b.append("readPageSize", readPageSize / scale);
        b.append("fanout", fanout);
        switch(compressionMethod) {
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
        b.appendNumber("queries", queries);
        b.appendNumber("nscanned", nscanned);
        b.appendNumber("nscannedObjects", nscannedObjects);
        b.appendNumber("inserts", inserts);
        b.appendNumber("deletes", deletes);
        // TODO: (Zardosht) Need to figure out how to display these dates
        /*
        Date_t create_date(_stats.bt_create_time_sec);
        Date_t modify_date(_stats.bt_modify_time_sec);
        bson_stats->append("create time", create_date);
        bson_stats->append("last modify time", modify_date);
        */
    }
    
    /* ---------------------------------------------------------------------- */

    IndexInterface::Builder::Builder(IndexInterface &idx)
            : _idx(idx),
              _db(_idx.db()),
              _loader(&_db, 1,
                      str::stream() << "Foreground index build progress (sort phase) for "
                                    << idx.parentNS() << ", key "
                                    << idx.keyPattern()) {}

    void IndexInterface::Builder::insertPair(const BSONObj &key, const BSONObj *pk, const BSONObj &val) {
        storage::Key skey(key, pk);
        DBT kdbt = skey.dbt();
        DBT vdbt = storage::dbt_make(NULL, 0);
        if (_idx.clustering()) {
            vdbt = storage::dbt_make(val.objdata(), val.objsize());
        }
        const int r = _loader.put(&kdbt, &vdbt);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

    void IndexInterface::Builder::done() {
        const int r = _loader.close();
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

} // namespace mongo
