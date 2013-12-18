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
#include "mongo/db/storage/dictionary.h"
#include "mongo/db/storage/env.h"

namespace mongo {

    namespace storage {

        // set a descriptor for the given dictionary.
        static void set_db_descriptor(DB *db, const Descriptor &descriptor,
                                      const bool hot_index) {
            const int flags = DB_UPDATE_CMP_DESCRIPTOR | (hot_index ? DB_IS_HOT_INDEX : 0);
            DBT desc = descriptor.dbt();
            const int r = db->change_descriptor(db, cc().txn().db_txn(), &desc, flags);
            if (r != 0) {
                handle_ydb_error_fatal(r);
            }
        }

        static void verify_or_upgrade_db_descriptor(DB *db, const Descriptor &descriptor,
                                                    const bool hot_index) {
            const DBT *desc = &db->cmp_descriptor->dbt;
            verify(desc->data != NULL && desc->size >= 4);

            if (desc->size == 4) {
                // existing descriptor is from before descriptors were even versioned.
                // it's only an ordering. make sure it matches, then upgrade.
                const Ordering &ordering(*reinterpret_cast<const Ordering *>(desc->data));
                const Ordering &expected(descriptor.ordering());
                verify(memcmp(&ordering, &expected, 4) == 0);
                set_db_descriptor(db, descriptor, hot_index);
            } else {
                const Descriptor existing(reinterpret_cast<const char *>(desc->data), desc->size);
                if (existing.version() < descriptor.version()) {
                    // existing descriptor is out-dated. upgrade to the current version.
                    set_db_descriptor(db, descriptor, hot_index);
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

        Dictionary::Dictionary(const string &dname, const BSONObj &info,
                               const mongo::Descriptor &descriptor, const bool may_create,
                               const bool hot_index) :
            _dname(dname), _db(NULL) {
            const int r = db_create(&_db, env, 0);
            if (r != 0) {
                handle_ydb_error(r);
            }
            try {
                open(info, descriptor, may_create, hot_index);
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

        void Dictionary::open(const BSONObj &info,
                              const mongo::Descriptor &descriptor, const bool may_create,
                              const bool hot_index) {
            int readPageSize = 65536;
            int pageSize = 4 * 1024 * 1024;
            TOKU_COMPRESSION_METHOD compression = TOKU_ZLIB_WITHOUT_CHECKSUM_METHOD;
            BSONObj key_pattern = info["key"].Obj();
            
            BSONElement e;
            e = info["readPageSize"];
            if (e.ok() && !e.isNull()) {
                readPageSize = BytesQuantity<int>(e);
                uassert(16743, "readPageSize must be a number > 0.", readPageSize > 0);
                TOKULOG(1) << "db " << _dname << ", using read page size " << readPageSize << endl;
            }
            e = info["pageSize"];
            if (e.ok() && !e.isNull()) {
                pageSize = BytesQuantity<int>(e);
                uassert(16445, "pageSize must be a number > 0.", pageSize > 0);
                TOKULOG(1) << "db " << _dname << ", using page size " << pageSize << endl;
            }
            e = info["compression"];
            if (e.ok() && !e.isNull()) {
                std::string str = e.String();
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
                TOKULOG(1) << "db " << _dname << ", using compression method \"" << str << "\"" << endl;
            }

            int r = _db->set_readpagesize(_db, readPageSize);
            if (r != 0) {
                handle_ydb_error(r);
            }

            r = _db->set_pagesize(_db, pageSize);
            if (r != 0) {
                handle_ydb_error(r);
            }

            r = _db->set_compression_method(_db, compression);
            if (r != 0) {
                handle_ydb_error(r);
            }

            // If this is a non-creating open for a read-only (or non-existent)
            // transaction, we can use an alternate stack since there's nothing
            // to roll back and no locktree locks to hold.
            const bool needAltTxn = !may_create && (!cc().hasTxn() || cc().txn().readOnly());
            scoped_ptr<Client::AlternateTransactionStack> altStack(!needAltTxn ? NULL :
                                                                   new Client::AlternateTransactionStack());
            scoped_ptr<Client::Transaction> altTxn(!needAltTxn ? NULL :
                                                   new Client::Transaction(0));

            const int db_flags = may_create ? DB_CREATE : 0;
            r = _db->open(_db, cc().txn().db_txn(), _dname.c_str(), NULL,
                          DB_BTREE, db_flags, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
            if (r == ENOENT && !may_create) {
                throw NeedsCreate();
            }
            if (r != 0) {
                handle_ydb_error(r);
            }
            if (may_create) {
                set_db_descriptor(_db, descriptor, hot_index);
            }
            verify_or_upgrade_db_descriptor(_db, descriptor, hot_index);

            if (altTxn.get() != NULL) {
                altTxn->commit();
            }
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

