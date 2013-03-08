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

#ifndef MONGO_DB_STORAGE_KEY_H
#define MONGO_DB_STORAGE_KEY_H

#include "mongo/pch.h"

#include <db.h>

// The dictionary key format is as follows:
//
//    Primary key:
//    { _id value, no name }
//    eg: { : 4 } for _id:4
//
//    Secondary keys:
//    { key values, no names } { associated PK, no field name }
//    eg: { : 4, : 5 } { : 1 } for key a:4, b:5 ---> _id:1
//
// The dictionary val format is either the entire BSON object, or nothing at all.
// If there's nothing, there must be an associated primary key.

namespace mongo {

    namespace storage {

        inline DBT make_dbt(const char *buf, size_t size) {
            DBT dbt;
            dbt.data = const_cast<char *>(buf);
            dbt.size = size;
            dbt.ulen = 0;
            dbt.flags = 0;
            return dbt;
        }

        class Key {
        public:
            Key(const BSONObj &key, const BSONObj *pk) :
                _size(key.objsize() + (pk != NULL ? pk->objsize() : 0)),
                _buf(new char[_size])
            {
                memcpy(_buf.get(), key.objdata(), key.objsize());
                if (pk != NULL) {
                    memcpy(_buf.get() + key.objsize(), pk->objdata(), pk->objsize());
                }
            }
            DBT dbt() const {
                return make_dbt(_buf.get(), _size);
            }
        private:
            size_t _size;
            scoped_ptr<char> _buf;
        };

    } // namespace storage

} // namespace mongo

#endif // MONGO_DB_STORAGE_KEY_H
