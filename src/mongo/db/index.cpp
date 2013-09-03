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
#include "mongo/db/curop.h"
#include "mongo/db/cursor.h"
#include "mongo/db/keygenerator.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/storage/key.h"
#include "mongo/db/storage/env.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/stringutils.h"

namespace mongo {

    /* This is an index where the keys are hashes of a given field.
     *
     * Optional arguments:
     *  "seed" : int (default = 0, a seed for the hash function)
     *  "hashVersion : int (default = 0, determines which hash function to use)
     *
     * Example use in the mongo shell:
     * > db.foo.ensureIndex({a : "hashed"}, {seed : 3, hashVersion : 0})
     *
     * LIMITATION: Only works with a single field. The HashedIndex
     * constructor uses uassert to ensure that the spec has the form
     * {<fieldname> : "hashed"}, and not, for example,
     * { a : "hashed" , b : 1}
     *
     * LIMITATION: Cannot be used as a unique index.
     * The HashedIndex constructor uses uassert to ensure that
     * the spec does not contain {"unique" : true}
     *
     * LIMITATION: Cannot be used to index arrays.
     * The getKeys function uasserts that value being inserted
     * is not an array.  This index will not be built if any
     * array values of the hashed field exist.
     */
    class HashedIndex : public IndexDetailsBase {
    public:
        HashedIndex(const BSONObj &info) :
            IndexDetailsBase(info),
            _hashedField(_keyPattern.firstElement().fieldName()),
            // Default seed/version to 0 if not specified or not an integer.
            _seed(_info["seed"].numberInt()),
            _hashVersion(_info["hashVersion"].numberInt()),
            _hashedNullObj(BSON("" << HashKeyGenerator::makeSingleKey(nullElt, _seed, _hashVersion))) {

            // change these if single-field limitation lifted later
            uassert( 16241, "Currently only single field hashed index supported.",
                            _keyPattern.nFields() == 1 );
            uassert( 16242, "Currently hashed indexes cannot guarantee uniqueness. Use a regular index.",
                            !unique() );

            // Create a descriptor with hashed = true and the appropriate hash seed.
            _descriptor.reset(new Descriptor(_keyPattern, true, _seed, _sparse, _clustering));

        }

        // @return the "special" name for this index.
        const string &getSpecialIndexName() const {
            static string name = "hashed";
            return name;
        }

        bool special() const {
            return true;
        }

        Suitability suitability(const FieldRangeSet &queryConstraints,
                                const BSONObj &order) const {
            if (queryConstraints.isPointIntervalSet(_hashedField)) {
                return HELPFUL;
            }
            return USELESS;
        }

        /* The newCursor method works for suitable queries by generating a IndexCursor
         * using the hash of point-intervals parsed by FieldRangeSet.
         * For unsuitable queries it just instantiates a cursor over the whole index.
         */
        shared_ptr<mongo::Cursor> newCursor(const BSONObj &query,
                                            const BSONObj &order,
                                            const int numWanted = 0) const {

            // Use FieldRangeSet to parse the query into a vector of intervals
            // These should be point-intervals if this cursor is ever used
            // So the FieldInterval vector will be, e.g. <[1,1], [3,3], [6,6]>
            FieldRangeSet frs("" , query , true, true);
            const vector<FieldInterval> &intervals = frs.range(_hashedField.c_str()).intervals();

            // Force a match of the query against the actual document by giving
            // the cursor a matcher with an empty indexKeyPattern.  This insures the
            // index is not used as a covered index.
            // NOTE: this forcing is necessary due to potential hash collisions
            const shared_ptr<CoveredIndexMatcher> forceDocMatcher(
                    new CoveredIndexMatcher(query, BSONObj()));

            Collection *cl = getCollection(parentNS());

            // Construct a new query based on the hashes of the previous point-intervals
            // e.g. {a : {$in : [ hash(1) , hash(3) , hash(6) ]}}
            BSONObjBuilder newQueryBuilder;
            BSONObjBuilder inObj(newQueryBuilder.subobjStart(_hashedField));
            BSONArrayBuilder inArray(inObj.subarrayStart("$in"));
            for (vector<FieldInterval>::const_iterator i = intervals.begin();
                 i != intervals.end(); ++i ){
                if (!i->equality()){
                    const shared_ptr<mongo::Cursor> cursor =
                        mongo::Cursor::make(cl, *this, 1);
                    cursor->setMatcher(forceDocMatcher);
                    return cursor;
                }
                inArray.append(HashKeyGenerator::makeSingleKey(i->_lower._bound, _seed, _hashVersion));
            }
            inArray.done();
            inObj.done();

            // Use the point-intervals of the new query to create an index cursor
            const BSONObj newQuery = newQueryBuilder.obj();
            FieldRangeSet newfrs("" , newQuery, true, true);
            shared_ptr<FieldRangeVector> newVector(
                    new FieldRangeVector(newfrs, _keyPattern, 1));

            const shared_ptr<mongo::Cursor> cursor =
                mongo::Cursor::make(cl, *this, newVector, false, 1, numWanted);
            cursor->setMatcher(forceDocMatcher);
            return cursor;
        }

        // A missing field is represented by a hashed null element.
        virtual BSONElement missingField() const {
            return _hashedNullObj.firstElement();
        }

    private:
        const string _hashedField;
        const HashSeed _seed;
        // In case we have hashed indexes based on other hash functions in
        // the future, we store a hashVersion number.
        const HashVersion _hashVersion;
        const BSONObj _hashedNullObj;
    };

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

    shared_ptr<IndexDetailsBase> IndexDetailsBase::make(const BSONObj &info, const bool may_create) {
        shared_ptr<IndexDetailsBase> idx;
        const string special = findSpecialIndexName(info["key"].Obj());
        if (special == "hashed") {
            idx.reset(new HashedIndex(info));
        } else {
            if (special != "") {
                warning() << "cannot find special index [" << special << "]" << endl;
            }
            idx.reset(new IndexDetailsBase(info));
        }
        bool ok = idx->open(may_create);
        if (!ok) {
            // This signals Collection::make that we got ENOENT due to #673
            return shared_ptr<IndexDetailsBase>();
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
    }

    IndexDetailsBase::IndexDetailsBase(const BSONObj& info) :
        IndexDetails(info),
        _descriptor(new Descriptor(_keyPattern, false, 0, _sparse, _clustering)) {
    }


    // Open the dictionary. Creates it if necessary.
    bool IndexDetailsBase::open(const bool may_create) {
        const string dname = indexNamespace();

        TOKULOG(1) << "Opening IndexDetails " << dname << endl;
        try {
            _db.reset(new storage::Dictionary(dname, _info, *_descriptor, may_create,
                                              _info["background"].trueValue()));
            return true;
        } catch (storage::Dictionary::NeedsCreate) {
            if (cc().upgradingSystemUsers() &&
                isSystemUsersCollection(parentNS()) &&
                keyPattern() == v0SystemUsersKeyPattern) {
                // We're upgrading the system.users collection, and we are missing the old index.
                // That's ok, we'll signal the caller about this by returning a NULL pointer from
                // IndexDetailsBase::make.  See #673
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

    IndexDetailsBase::~IndexDetailsBase() {
        try {
            close();
        } catch (DBException &ex) {
            problem() << "~IndexDetails " << _keyPattern
                      << ": Caught exception: " << ex.what() << endl;
        }
    }

    void IndexDetailsBase::close() {
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

    void IndexDetailsBase::kill_idx() {
        const string ns = indexNamespace();

        close();
        storage::db_remove(ns);
    }

    bool IndexDetailsBase::changeAttributes(const BSONObj &info, BSONObjBuilder &wasBuilder) {
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

    void IndexDetailsBase::getKeysFromObject(const BSONObj &obj, BSONObjSet &keys) const {
        _descriptor->generateKeys(obj, keys);
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

    int IndexDetailsBase::uniqueCheckCallback(const DBT *key, const DBT *val, void *extra) {
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

    void IndexDetailsBase::uniqueCheck(const BSONObj &key, const BSONObj &pk) const {
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

    void IndexDetailsBase::uassertedDupKey(const BSONObj &key) const {
        uasserted(ASSERT_ID_DUPKEY, mongoutils::str::stream()
                                    << "E11000 duplicate key error, " << key
                                    << " already exists in unique index");
    }

    void IndexDetailsBase::acquireTableLock() {
        const int r = db()->pre_acquire_table_lock(db(), cc().txn().db_txn());
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

    void IndexDetailsBase::updatePair(const BSONObj &key, const BSONObj *pk, const BSONObj &msg, uint64_t flags) {
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

    enum toku_compression_method IndexDetailsBase::getCompressionMethod() const {
        enum toku_compression_method ret;
        int r = db()->get_compression_method(db(), &ret);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        return ret;
    }

    uint32_t IndexDetailsBase::getFanout() const {
        uint32_t ret;
        int r = db()->get_fanout(db(), &ret);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        return ret;
    }

    uint32_t IndexDetailsBase::getPageSize() const {
        uint32_t ret;
        int r = db()->get_pagesize(db(), &ret);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        return ret;
    }

    uint32_t IndexDetailsBase::getReadPageSize() const {
        uint32_t ret;
        int r = db()->get_readpagesize(db(), &ret);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        return ret;
    }

    void IndexDetailsBase::getStat64(DB_BTREE_STAT64* stats) const {
        int r = db()->stat64(db(), NULL, stats);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

    int IndexDetailsBase::hot_optimize_callback(void *extra, float progress) {
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

    void IndexDetailsBase::optimize(const storage::Key &leftSKey, const storage::Key &rightSKey,
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

    IndexDetailsBase::Builder::Builder(IndexDetailsBase &idx)
            : _idx(idx),
              _db(_idx.db()),
              _loader(&_db, 1,
                      str::stream() << "Foreground index build progress (sort phase) for "
                                    << idx.parentNS() << ", key "
                                    << idx.keyPattern()) {}

    void IndexDetailsBase::Builder::insertPair(const BSONObj &key, const BSONObj *pk, const BSONObj &val) {
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

    void IndexDetailsBase::Builder::done() {
        const int r = _loader.close();
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

    
    enum toku_compression_method PartitionedIndexDetails::getCompressionMethod() const {
        return _pc->getPartition(0)->idx(_idxNum).getCompressionMethod();
    }

    uint32_t PartitionedIndexDetails::getFanout() const {
        return _pc->getPartition(0)->idx(_idxNum).getFanout();
    }

    uint32_t PartitionedIndexDetails::getPageSize() const {
        return _pc->getPartition(0)->idx(_idxNum).getPageSize();
    }

    uint32_t PartitionedIndexDetails::getReadPageSize() const {
        return _pc->getPartition(0)->idx(_idxNum).getReadPageSize();
    }

    void PartitionedIndexDetails::getStat64(DB_BTREE_STAT64* stats) const {
        DB_BTREE_STAT64 ret;
        memset(&ret, 0, sizeof(ret));
        // TODO: figure out what the proper way to set max is
        ret.bt_verify_time_sec = (uint64_t)-1;
        for (uint64_t i = 0; i < _pc->numPartitions(); i++) {
            DB_BTREE_STAT64 curr;
            _pc->getPartition(i)->idx(_idxNum).getStat64(&curr);
            ret.bt_nkeys += curr.bt_nkeys;
            ret.bt_ndata += curr.bt_ndata;
            ret.bt_dsize += curr.bt_dsize;
            ret.bt_fsize += curr.bt_fsize;
            if (curr.bt_create_time_sec > ret.bt_create_time_sec) {
                ret.bt_create_time_sec = curr.bt_create_time_sec;
            }
            if (curr.bt_modify_time_sec > ret.bt_modify_time_sec) {
                ret.bt_modify_time_sec = curr.bt_modify_time_sec;
            }
            if (curr.bt_verify_time_sec < ret.bt_verify_time_sec) {
                ret.bt_verify_time_sec = curr.bt_verify_time_sec;
            }
        }
        *stats = ret;
    }
    
    // find a way to remove this eventually and have callers get
    // access to IndexDetailsBase directly somehow
    // This is a workaround to get going for now
    shared_ptr<storage::Cursor> PartitionedIndexDetails::getCursor(const int flags) const {
        uasserted(17243, "should not call getCursor on a PartitionedIndexDetails");
    }

} // namespace mongo
