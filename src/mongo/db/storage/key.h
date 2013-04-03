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

        /** Key class for precomputing a small format index key that is denser than a traditional BSONObj. */
        class KeyV1Owned;

        class KeyV1 {
            void operator=(const KeyV1&); // disallowed just to make people be careful as we don't own the buffer
            KeyV1(const KeyV1Owned&);     // disallowed as this is not a great idea as KeyV1Owned likely will go out of scope
        public:
            KeyV1() { _keyData = 0; }
            ~KeyV1() { DEV _keyData = (const unsigned char *) 1; }

            KeyV1(const KeyV1& rhs) : _keyData(rhs._keyData) { 
                dassert( _keyData > (const unsigned char *) 1 );
            }

            // explicit version of operator= to be safe
            void assign(const KeyV1& rhs) { 
                _keyData = rhs._keyData;
            }

            /** @param keyData can be a buffer containing data in either BSON format, OR in KeyV1 format. 
                       when BSON, we are just a wrapper
            */
            explicit KeyV1(const char *keyData) : _keyData((unsigned char *) keyData) { }

            int woCompare(const KeyV1& r, const Ordering &o) const;
            bool woEqual(const KeyV1& r) const;
            BSONObj toBson() const;
            string toString() const { return toBson().toString(); }

            /** get the key data we want to store in the btree bucket */
            const char * data() const { return (const char *) _keyData; }

            /** @return size of data() */
            int dataSize() const;

            /** only used by geo, which always has bson keys */
            BSONElement _firstElement() const { return bson().firstElement(); }
            bool isCompactFormat() const { return *_keyData != IsBSON; }

            bool isValid() const { return _keyData > (const unsigned char*)1; }
        protected:
            enum { IsBSON = 0xff };
            const unsigned char *_keyData;
            BSONObj bson() const {
                dassert( !isCompactFormat() );
                return BSONObj((const char *) _keyData+1);
            }
        private:
            int compareHybrid(const KeyV1& right, const Ordering& order) const;
        };

        class KeyV1Owned : public KeyV1 {
            void operator=(const KeyV1Owned&);
        public:
            /** @obj a BSON object to be translated to KeyV1 format.  If the object isn't 
                     representable in KeyV1 format (which happens, intentionally, at times)
                     it will stay as bson herein.
            */
            KeyV1Owned(const BSONObj& obj);

            /** makes a copy (memcpy's the whole thing) */
            KeyV1Owned(const KeyV1& rhs);

        private:
            StackBufBuilder b;
            void traditional(const BSONObj& obj); // store as traditional bson not as compact format
        };

        inline DBT make_dbt(const char *buf, size_t size) {
            DBT dbt;
            dbt.data = const_cast<char *>(buf);
            dbt.size = size;
            dbt.ulen = 0;
            dbt.flags = 0;
            return dbt;
        }

        // Dictionary key format:
        // KeyV1Owned key [, BSONObj primary key]
        class Key {
        public:
            Key(const BSONObj &key, const BSONObj *pk) {
                KeyV1Owned keyOwned(key);
                _b.appendBuf(keyOwned.data(), keyOwned.dataSize());
                if (pk != NULL) {
                    _b.appendBuf(pk->objdata(), pk->objsize());
                }
            }
            DBT dbt() const {
                return make_dbt(_b.buf(), _b.len());
            }

            Key(const DBT *dbt) {
                storage::KeyV1 kv1(static_cast<const char *>(dbt->data));
                _key = kv1.toBson();
                const int keyv1Size = kv1.dataSize();
                if (keyv1Size < (int) dbt->size) {
                    _pk = BSONObj(static_cast<const char *>(dbt->data) + keyv1Size);
                    dassert(keyv1Size + _pk.objsize() == (int) dbt->size);
                }
                else {
                    dassert(keyv1Size == (int) dbt->size);
                }
            }
            const BSONObj &key() const {
                return _key;
            }
            const BSONObj &pk() const {
                return _pk;
            }

        private:
            StackBufBuilder _b;
            BSONObj _key;
            BSONObj _pk;
        };

    } // namespace storage

} // namespace mongo

#endif // MONGO_DB_STORAGE_KEY_H
