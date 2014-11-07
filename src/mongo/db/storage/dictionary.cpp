/**
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

#include "mongo/base/units.h"
#include "mongo/db/client.h"
#include "mongo/db/descriptor.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/storage/dictionary.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/key.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    namespace storage {

        // set a descriptor for the given dictionary.
        static void set_db_descriptor(DB **db, const Descriptor &descriptor,
                                      const char* dname,
                                      const bool hot_index) {
            int r = (*db)->close(*db, 0);
            *db = NULL;
            const int flags = (hot_index ? DB_IS_HOT_INDEX : 0);
            DBT desc = descriptor.dbt();
            r = env->db_change_descriptor(env, cc().txn().db_txn(), dname, &desc);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
            
            // reopen db
            r = db_create(db, env, 0);
            if (r != 0) {
                handle_ydb_error(r);
            }
            r = (*db)->open(*db, cc().txn().db_txn(), dname, NULL,
                                    DB_BTREE,
                                    flags, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
            if (r != 0) {
                handle_ydb_error(r);
            }
        }

        static void verify_or_upgrade_db_descriptor(DB **db, const Descriptor &descriptor,
                                                    const char* dname,
                                                    const bool hot_index) {
            const DBT *desc = &((*db)->descriptor->dbt);
            if (desc->data == NULL && desc->size < 4) {
                set_db_descriptor(db, descriptor, dname, hot_index);
            } else if (desc->size == 4) {
                // existing descriptor is from before descriptors were even versioned.
                // it's only an ordering. make sure it matches, then upgrade.
                const Ordering &ordering(*reinterpret_cast<const Ordering *>(desc->data));
                const Ordering &expected(descriptor.ordering());
                verify(memcmp(&ordering, &expected, 4) == 0);
                set_db_descriptor(db, descriptor, dname, hot_index);
            } else {
                const Descriptor existing(reinterpret_cast<const char *>(desc->data), desc->size);
                if (existing.version() < descriptor.version()) {
                    // existing descriptor is out-dated. upgrade to the current version.
                    set_db_descriptor(db, descriptor, dname, hot_index);
                } else if (existing.version() > descriptor.version()) {
                    problem() << "Detected a \"dictionary descriptor\" version that is too new: "
                              << existing.version() << ". The highest known version is " << descriptor.version()
                              << "This data may have already been upgraded by a newer version of "
                              << "TokuMX and is now no longer usable by this version."
                              << endl << endl
                              << "The assertion failure you are about to see is intentional."
                              << endl;
                    verify(false);
                } else {
                    // same version, ensure the contents of the descriptor are correct
                    verify(existing == descriptor);
                }
            }
        }

        MONGO_EXPORT_SERVER_PARAMETER(defaultCompression, std::string, "zlib");
        MONGO_EXPORT_SERVER_PARAMETER(defaultPageSize, BytesQuantity<int>, StringData("4MB"));
        MONGO_EXPORT_SERVER_PARAMETER(defaultReadPageSize, BytesQuantity<int>, StringData("64KB"));
        MONGO_EXPORT_SERVER_PARAMETER(defaultFanout, int, 16);

        // Get an object with default attribute values, overriden 
        // by anyting present in the given info object.
        //
        // Currently, these include:
        // - compression
        // - readPageSize
        // - pageSize
        static BSONObj fillDefaultAttributes(const BSONObj &info) {
            // zlib compression
            const std::string compression = (info.hasField("compression")
                                             ? info["compression"].valuestrsafe()
                                             : defaultCompression);

            // 4mb fractal tree nodes
            const int pageSize = (info.hasField("pageSize")
                                  ? BytesQuantity<int>(info["pageSize"])
                                  : defaultPageSize);

            // 64kb basement nodes
            const int readPageSize = (info.hasField("readPageSize")
                                      ? BytesQuantity<int>(info["readPageSize"])
                                      : defaultReadPageSize);

            // fractal tree fanout is 16
            const int fanout = (info.hasField("fanout")
                                ? info["fanout"].numberInt()
                                : defaultFanout);

            return BSON("compression" << compression <<
                        "readPageSize" << readPageSize <<
                        "pageSize" << pageSize <<
                        "fanout" << fanout);
        }
            
        Dictionary::Dictionary(const string &dname, const BSONObj &info,
                               const mongo::Descriptor &descriptor,
                               const bool may_create, const bool hot_index,
                               const bool set_memcmp_magic) :
            _dname(dname), _db(NULL) {
            const int r = db_create(&_db, env, 0);
            if (r != 0) {
                handle_ydb_error(r);
            }
            try {
                open(descriptor, may_create, hot_index, set_memcmp_magic);
                const BSONObj attr = fillDefaultAttributes(info);
                BSONObjBuilder unusedBuilder;
                changeAttributes(attr, unusedBuilder);
            } catch (...) {
                close();
                throw;
            }
        }

        Dictionary::~Dictionary() {
            const int r = close();
            if (r != 0) {
                problem() << "storage::Dictionary destructor: failed to close dname "
                          << _dname << endl;
            }
        }

        void Dictionary::open(const mongo::Descriptor &descriptor, const bool may_create,
                              const bool hot_index, const bool set_memcmp_magic) {
            // If this is a non-creating open for a read-only (or non-existent)
            // transaction, we can use an alternate stack since there's nothing
            // to roll back and no locktree locks to hold.
            const bool needAltTxn = !may_create && (!cc().hasTxn() || cc().txn().readOnly());
            scoped_ptr<Client::AlternateTransactionStack> altStack(!needAltTxn ? NULL :
                                                                   new Client::AlternateTransactionStack());
            scoped_ptr<Client::Transaction> altTxn(!needAltTxn ? NULL :
                                                   new Client::Transaction(0));

            if (set_memcmp_magic) {
                const int r = _db->set_memcmp_magic(_db, memcmpMagic());
                if (r != 0) {
                    handle_ydb_error_fatal(r);
                }
            }

            const int db_flags = may_create ? DB_CREATE : 0;
            const int r = _db->open(_db, cc().txn().db_txn(), _dname.c_str(), NULL,
                                    // But I thought there were fractal trees! Yes, this is for bdb compatibility.
                                    DB_BTREE,
                                    db_flags, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
            if (r == ENOENT && !may_create) {
                throw NeedsCreate();
            }
            if (r != 0) {
                handle_ydb_error(r);
            }
            // if we've just created the dictionary, the descriptor
            // will not have been set yet, and will be set with
            // this function call
            verify_or_upgrade_db_descriptor(&_db, descriptor, _dname.c_str(), hot_index);

            if (altTxn.get() != NULL) {
                altTxn->commit();
            }
        }

        class DBParameterSetter : public boost::noncopyable {
          public:
            virtual ~DBParameterSetter() {}
            virtual bool finalize(BSONObjBuilder &wasBuilder) = 0;
        };

        template<typename T>
        class DBParameterSetterImpl : public DBParameterSetter {
            typedef int (*db_getter_fun)(DB *db, T *val);
            typedef int (*db_setter_fun)(DB *db, T val);
            DB *_db;
            string _name;
            T _value;
            T _oldValue;
            db_getter_fun _get;
            db_setter_fun _set;
            bool _didSet;
            bool _shouldUnset;
          public:
            DBParameterSetterImpl(DB *db, const string &name, T value,
                                  db_getter_fun get, db_setter_fun set)
                    : _db(db), _name(name), _value(value), _get(get), _set(set),
                      _didSet(false), _shouldUnset(true) {
                int r = _get(_db, &_oldValue);
                if (r != 0) {
                    problem() << "error getting parameter " << _name << endl;
                    handle_ydb_error(r);
                }
                if (_oldValue == _value) {
                    return;
                }
                r = _set(_db, _value);
                if (r != 0) {
                    problem() << "error setting parameter " << _name << endl;
                    handle_ydb_error(r);
                }
                _didSet = true;
            }
            ~DBParameterSetterImpl() {
                if (_didSet && _shouldUnset) {
                    int r = _set(_db, _oldValue);
                    if (r != 0) {
                        problem() << "error " << r
                                  << " when trying to reset parameter " << _name
                                  << endl;
                    }
                }
            }
            virtual bool finalize(BSONObjBuilder &wasBuilder) {
                _shouldUnset = false;
                wasBuilder.append(_name, _oldValue);
                return _didSet;
            }
        };

        static string compressionMethodToString(TOKU_COMPRESSION_METHOD c) {
            switch (c) {
                case TOKU_SMALL_COMPRESSION_METHOD:
                case TOKU_LZMA_METHOD:
                    return "lzma";
                case TOKU_DEFAULT_COMPRESSION_METHOD:
                case TOKU_FAST_COMPRESSION_METHOD:
                case TOKU_QUICKLZ_METHOD:
                    return "quicklz";
                case TOKU_ZLIB_WITHOUT_CHECKSUM_METHOD:
                case TOKU_ZLIB_METHOD:
                    return "zlib";
                case TOKU_NO_COMPRESSION:
                    return "none";
                default:
                    msgasserted(17233, mongoutils::str::stream() << "invalid compression method " << c);
            }
        }

        template<>
        bool DBParameterSetterImpl<TOKU_COMPRESSION_METHOD>::finalize(BSONObjBuilder &wasBuilder) {
            _shouldUnset = false;
            wasBuilder.append(_name, compressionMethodToString(_oldValue));
            return _didSet;
        }

        // @param info describes the attributes to be changed
        bool Dictionary::changeAttributes(const BSONObj &info, BSONObjBuilder &wasBuilder) {
            map<string, shared_ptr<DBParameterSetter> > setMap;
            for (BSONObjIterator it(info); it.more(); ++it) {
                BSONElement e = *it;
                string fn(e.fieldName());
                if (setMap.find(fn) != setMap.end()) {
                    uasserted(17235, mongoutils::str::stream() << "can't set " << fn << " twice");
                }
                if (fn == "readPageSize") {
                    const uint32_t readPageSize = BytesQuantity<uint32_t>(e);
                    uassert(16743, "readPageSize must be a number > 0.", readPageSize > 0);
                    setMap[fn] = boost::make_shared<DBParameterSetterImpl<uint32_t> >(
                        _db, fn, readPageSize, _db->get_readpagesize, _db->change_readpagesize);
                } else if (fn == "pageSize") {
                    const uint32_t pageSize = BytesQuantity<uint32_t>(e);
                    uassert(16445, "pageSize must be a number > 0.", pageSize > 0);
                    setMap[fn] = boost::make_shared<DBParameterSetterImpl<uint32_t> >(
                        _db, fn, pageSize, _db->get_pagesize, _db->change_pagesize);
                } else if (fn == "compression") {
                    TOKU_COMPRESSION_METHOD compression = TOKU_ZLIB_WITHOUT_CHECKSUM_METHOD;
                    const string str = e.String();
                    if (str == "lzma") {
                        compression = TOKU_LZMA_METHOD;
                    } else if (str == "quicklz") {
                        compression = TOKU_QUICKLZ_METHOD;
                    } else if (str == "zlib") {
                        compression = TOKU_ZLIB_WITHOUT_CHECKSUM_METHOD;
                    } else if (str == "none") {
                        compression = TOKU_NO_COMPRESSION;
                    } else {
                        uassert(16442, "compression must be one of: lzma, quicklz, zlib, none.", false);
                    }
                    setMap[fn] = boost::make_shared<DBParameterSetterImpl<TOKU_COMPRESSION_METHOD> >(
                        _db, fn, compression, _db->get_compression_method, _db->change_compression_method);
                } else if (fn == "fanout") {
                    int fanout = e.numberInt();
                    uassert(17288, "fanout must be number >= 4", fanout >= 4);
                    setMap[fn] = boost::make_shared<DBParameterSetterImpl<unsigned int> >(
                        _db, fn, (unsigned int) fanout, _db->get_fanout, _db->change_fanout);
                } else {
                    uasserted(17234, mongoutils::str::stream() << "cannot set unknown attribute " << fn);
                }
            }
            bool ret = false;
            for (map<string, shared_ptr<DBParameterSetter> >::const_iterator it = setMap.begin();
                 it != setMap.end(); ++it) {
                if (it->second->finalize(wasBuilder)) {
                    ret = true;
                }
            }
            return ret;
        }

        int Dictionary::close() {
            int r = 0;
            if (_db) {
                r = _db->close(_db, 0);
                _db = NULL;
            }
            return r;
        }

    } // namespace storage

} // namespace mongo

