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
    class HashedIndex : public IndexDetails {
    public:
        HashedIndex(const BSONObj &info) :
            IndexDetails(info),
            _hashedField(_keyPattern.firstElement().fieldName()),
            // Default seed/version to 0 if not specified or not an integer.
            _seed(_info["seed"].numberInt()),
            _hashVersion(_info["hashVersion"].numberInt()) {

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

            NamespaceDetails *d = nsdetails(parentNS());

            // Construct a new query based on the hashes of the previous point-intervals
            // e.g. {a : {$in : [ hash(1) , hash(3) , hash(6) ]}}
            BSONObjBuilder newQueryBuilder;
            BSONObjBuilder inObj(newQueryBuilder.subobjStart(_hashedField));
            BSONArrayBuilder inArray(inObj.subarrayStart("$in"));
            for (vector<FieldInterval>::const_iterator i = intervals.begin();
                 i != intervals.end(); ++i ){
                if (!i->equality()){
                    const shared_ptr<mongo::Cursor> exhaustiveCursor(
                            new IndexScanCursor(d, *this, 1));
                    exhaustiveCursor->setMatcher(forceDocMatcher);
                    return exhaustiveCursor;
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
            const shared_ptr<mongo::Cursor> cursor(
                    IndexCursor::make(d, *this, newVector, false, 1, numWanted));
            cursor->setMatcher(forceDocMatcher);
            return cursor;
        }

    private:
        const string _hashedField;
        const HashSeed _seed;
        // In case we have hashed indexes based on other hash functions in
        // the future, we store a hashVersion number.
        const HashVersion _hashVersion;
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

    shared_ptr<IndexDetails> IndexDetails::make(const BSONObj &info, const bool may_create) {
        shared_ptr<IndexDetails> idx;
        const string special = findSpecialIndexName(info["key"].Obj());
        if (special == "hashed") {
            idx.reset(new HashedIndex(info));
        } else {
            if (special != "") {
                warning() << "cannot find special index [" << special << "]" << endl;
            }
            idx.reset(new IndexDetails(info));
        }
        idx->open(may_create);
        return idx;
    }

    IndexDetails::IndexDetails(const BSONObj &info) :
        _info(info.copy()),
        _keyPattern(info["key"].Obj().copy()),
        _unique(info["unique"].trueValue()),
        _sparse(info["sparse"].trueValue()),
        _clustering(info["clustering"].trueValue()),
        _descriptor(new Descriptor(_keyPattern, false, 0, _sparse, _clustering)) {
        verify(!_info.isEmpty());
        verify(!_keyPattern.isEmpty());
    }

    // Open the dictionary. Creates it if necessary.
    void IndexDetails::open(const bool may_create) {
        const string dname = indexNamespace();
        if (may_create) {
            addNewNamespaceToCatalog(dname);
        }

        TOKULOG(1) << "Opening IndexDetails " << dname << endl;
        try {
            _db.reset(new storage::Dictionary(dname, _info, *_descriptor, may_create,
                                              _info["background"].trueValue()));
        } catch (storage::Dictionary::NeedsCreate) {
            // Unlike for NamespaceIndex, this dictionary must exist on disk if we think it should
            // exist.  This error only gets thrown if may_create is false, which happens when we're
            // trying to open a collection for which we have serialized info.  Therefore, this is a
            // fatal non-user error.
            msgasserted(16988, mongoutils::str::stream() << "dictionary " << dname
                               << " should exist, but we got ENOENT");
        }
    }

    IndexDetails::~IndexDetails() {
        try {
            close();
        } catch (DBException &ex) {
            problem() << "~IndexDetails " << _keyPattern
                      << ": Caught exception: " << ex.what() << endl;
        }
    }

    void IndexDetails::close() {
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

    void IndexDetails::kill_idx() {
        const string ns = indexNamespace();
        const string parentns = parentNS();

        close();
        storage::db_remove(ns);

        // Removing this index's ns from the system.indexes/namespaces catalog.
        removeNamespaceFromCatalog(ns);
        if (nsToCollectionSubstring(parentns) != "system.indexes") {
            removeFromSysIndexes(parentns, indexName());
        }
    }

    void IndexDetails::getKeysFromObject(const BSONObj &obj, BSONObjSet &keys) const {
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
        UniqueCheckExtra extra(sKey, *_descriptor, isUnique);
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
        const int r = db()->pre_acquire_table_lock(db(), cc().txn().db_txn());
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

    enum toku_compression_method IndexDetails::getCompressionMethod() const {
        enum toku_compression_method ret;
        int r = db()->get_compression_method(db(), &ret);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        return ret;
    }

    uint32_t IndexDetails::getPageSize() const {
        uint32_t ret;
        int r = db()->get_pagesize(db(), &ret);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        return ret;
    }

    uint32_t IndexDetails::getReadPageSize() const {
        uint32_t ret;
        int r = db()->get_readpagesize(db(), &ret);
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        return ret;
    }

    void IndexDetails::getStat64(DB_BTREE_STAT64* stats) const {
        int r = db()->stat64(db(), NULL, stats);
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

    void IndexDetails::optimize(const storage::Key &leftSKey, const storage::Key &rightSKey,
                                const bool sendOptimizeMessage) {
        if (sendOptimizeMessage) {
            const int r = db()->optimize(db());
            if (r != 0) {
                storage::handle_ydb_error(r);
            }
        }

        uint64_t iter = 0;
        DBT left = leftSKey.dbt();
        DBT right = rightSKey.dbt();
        const int r = db()->hot_optimize(db(), &left, &right, hot_opt_callback, &iter);
        if (r != 0) {
            uassert(16810, mongoutils::str::stream() << "reIndex query killed ", false);
        }
    }

    IndexStats::IndexStats(const IndexDetails &idx)
            : _name(idx.indexName()),
              _compressionMethod(idx.getCompressionMethod()),
              _readPageSize(idx.getReadPageSize()),
              _pageSize(idx.getPageSize()),
              _accessStats(idx.getAccessStats()) {
        idx.getStat64(&_stats);
    }
    
    BSONObj IndexStats::obj(int scale) const {
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
        b.appendNumber("queries", _accessStats.queries.load());
        b.appendNumber("nscanned", _accessStats.nscanned.load());
        b.appendNumber("nscannedObjects", _accessStats.nscannedObjects.load());
        b.appendNumber("inserts", _accessStats.inserts.load());
        b.appendNumber("deletes", _accessStats.deletes.load());
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
        _idx(idx), _db(_idx.db()), _loader(&_db, 1) {
        _loader.setPollMessagePrefix(str::stream() << "Cold index build progress: "
                                                   << idx.parentNS() << ", key "
                                                   << idx.keyPattern()
                                                   << ":");

    }

    void IndexDetails::Builder::insertPair(const BSONObj &key, const BSONObj *pk, const BSONObj &val) {
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

    void IndexDetails::Builder::done() {
        const int r = _loader.close();
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

} // namespace mongo
