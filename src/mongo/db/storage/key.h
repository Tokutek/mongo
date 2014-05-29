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

#pragma once

#include "mongo/pch.h"

#include "mongo/db/storage/dbt.h"

#include <db.h>

// The dictionary key format is as follows:
//
//    Primary key:
//    { primaryKey }
//    eg: { : 4 } if pk pattern is { _id: 1 } and extracted primary key is { _id: 4 }
//    eg: { : "hello", : 7 } if pk pattern is { a: 1, _id: 1 } and extracted primary key
//        is { a: "hello", _id: 7 }
//
//    Secondary keys:
//    { key values, no names } { associated PK, no field name }
//    eg: { : 4, : 5 } { : 1 } for key a:4, b:5 ---> pk { : 1 }
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

            static int compare(const unsigned char *&l, const unsigned char *&r);
            static int binDataCodeToLength(int codeByte);
            static unsigned sizeOfElement(const unsigned char *p);

        public:
            // [ ][HASMORE][x][y][canontype_4bits]
            enum CanonicalsEtc { 
                cminkey=1,
                cnull=2,
                cdouble=4,
                cstring=6,
                cbindata=7,
                coid=8,
                cfalse=10,
                ctrue=11,
                cdate=12,
                cmaxkey=14,
                cCANONTYPEMASK = 0xf,
                cY = 0x10,
                cint = cY | cdouble,
                cX = 0x20,
                clong = cX | cdouble,
                cZ = 0x30,
                cint64 = cZ | cdouble,
                cHASMORE = 0x40,
                cNOTUSED = 0x80 // but see IsBSON sentinel - this bit not usable without great care
            };

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
            BSONObj toBson() const {
                BufBuilder bb;
                return toBson(bb).getOwned();
            }
            BSONObj toBson(BufBuilder &bb) const;
            string toString() const { return toBson().toString(); }

            /** get the key data we want to store as the index key */
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

        // Dictionary key format:
        // { KeyV1 key [, BSONObj primary key] }
        class Key {
        public:
            // For serializing
            Key(const BSONObj &key, const BSONObj *pk) {
                KeyV1Owned keyOwned(key);
                _b.appendBuf(keyOwned.data(), keyOwned.dataSize());
                if (pk != NULL) {
                    _b.appendBuf(pk->objdata(), pk->objsize());
                }
                _buf = _b.buf();
                _size = _b.len();
            }

            // For deserializing
            Key() : _buf(NULL), _size(0) {
            }

            Key(const DBT *dbt) :
                _buf(static_cast<const char *>(dbt->data)), _size(dbt->size) {
            }

            Key(const char *buf, const bool hasPK) : _buf(buf) {
                storage::KeyV1 kv1(_buf);
                const size_t keySize = kv1.dataSize();
                _size = keySize + (hasPK ? BSONObj(_buf + keySize).objsize() : 0);
            }

            static int woCompare(const Key &key1, const Key &key2, const Ordering &ordering) {
                // Interpret the beginning of the Key's buf as KeyV1. The size of the Key
                // must be at least as big as the size of the KeyV1 (otherwise format error).
                dassert(key1.buf());
                dassert(key2.buf());
                const KeyV1 k1(static_cast<const char *>(key1.buf()));
                const KeyV1 k2(static_cast<const char *>(key2.buf()));
                dassert((int) key1.size() >= k1.dataSize());
                dassert((int) key2.size() >= k2.dataSize());

                // Compare by the first key in KeyV1 format.
                {
                    const int c = k1.woCompare(k2, ordering);
                    if (c < 0) {
                        return -1;
                    } else if (c > 0) {
                        return 1;
                    }
                }

                // Compare by the second key in BSON format, if it exists.
                const int k1_size = k1.dataSize();
                const int k2_size = k2.dataSize();
                const int key1_bytes_left = key1.size() - k1_size;
                const int key2_bytes_left = key2.size() - k2_size;
                if (key1_bytes_left > 0 && key2_bytes_left > 0) {
                    const BSONObj other_k1(static_cast<const char *>(key1.buf()) + k1_size);
                    const BSONObj other_k2(static_cast<const char *>(key2.buf()) + k2_size);
                    dassert(k1_size + other_k1.objsize() == (int) key1.size());
                    dassert(k2_size + other_k2.objsize() == (int) key2.size());

                    // Note: The ordering here is unintuitive.
                    //
                    // We arbitrarily chose 'ascending' ordering for each part of the primary key.
                    // It doesn't matter what the _real_ ordering of the primary key is here,
                    // because there are no ordered scans over this part of the key. All that 
                    // matters is that we're consistent.
                    static const unsigned ordering_bits = 0;
                    static const Ordering &pk_ordering = *reinterpret_cast<const Ordering *>(&ordering_bits);
                    const int c = other_k1.woCompare(other_k2, pk_ordering);
                    if (c < 0) {
                        return -1;
                    } else if (c > 0) {
                        return 1;
                    }
                } else {
                    // The associated primary key must exist in both keys, or neither.
                    dassert(key1_bytes_left == 0 && key2_bytes_left == 0);
                }
                return 0;
            }

            // The real comparison function is a function of two keys
            // and an ordering, which is a bit more clear.
            int woCompare(const Key &key, const Ordering &ordering) const {
                return woCompare(*this, key, ordering);
            }

            DBT dbt() const {
                return dbt_make(_buf, _size);
            }

            // HACK This isn't so nice.
            void set(const char *buf, size_t size) {
                _buf = buf;
                _size = size;
            }

            void reset(const BSONObj &other, const BSONObj *pk) {
                _b.reset();
                KeyV1Owned otherOwned(other);
                _b.appendBuf(otherOwned.data(), otherOwned.dataSize());
                if (pk != NULL) {
                    _b.appendBuf(pk->objdata(), pk->objsize());
                }
                _buf = _b.buf();
                _size = _b.len();
            }

            void reset(const KeyV1 &other, const BSONObj *pk) {
                _b.reset();
                _b.appendBuf(other.data(), other.dataSize());
                if (pk != NULL) {
                    _b.appendBuf(pk->objdata(), pk->objsize());
                }
                _buf = _b.buf();
                _size = _b.len();
            }

            BSONObj key() const {
                BufBuilder bb;
                return key(bb).getOwned();
            }

            BSONObj key(BufBuilder &bb) const {
                storage::KeyV1 kv1(_buf);
                return kv1.toBson(bb);
            }

            BSONObj pk() const {
                storage::KeyV1 kv1(_buf);
                const size_t keySize = kv1.dataSize();
                return keySize < _size ? BSONObj(_buf + keySize) : BSONObj();
            }

            const char *buf() const {
                return _buf;
            }

            int size() const {
                return _size;
            }

        private:
            StackBufBuilder _b;
            const char *_buf;
            size_t _size;
        };

        inline int KeyV1::binDataCodeToLength(int codeByte) { 
            static const int BinDataCodeToLength[] = { 
                0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 32
            };
            return BinDataCodeToLength[codeByte >> 4];
        }

        inline unsigned KeyV1::sizeOfElement(const unsigned char *p) { 
            static unsigned sizes[] = {
                0,
                1, //cminkey=1,
                1, //cnull=2,
                0,
                9, //cdouble=4,
                0,
                0, //cstring=6,
                0,
                13, //coid=8,
                0,
                1, //cfalse=10,
                1, //ctrue=11,
                9, //cdate=12,
                0,
                1, //cmaxkey=14,
                0
            };
            unsigned type = *p & cCANONTYPEMASK;
            unsigned sz = sizes[type];
            if( sz == 0 ) {
                if( type == cstring ) { 
                    sz = ((unsigned) p[1]) + 2;
                }
                else {
                    verify( type == cbindata );
                    sz = binDataCodeToLength(p[1]) + 2;
                }
            }
            return sz;
        }

        inline int KeyV1::compare(const unsigned char *&l, const unsigned char *&r) {
            int lt_real = *l;
            int rt_real = *r;
            int lt = (lt_real & cCANONTYPEMASK);
            int rt = (rt_real & cCANONTYPEMASK);
            int x = lt - rt;
            if( MONGO_unlikely(x) ) 
                return x;

            l++; r++;

            // same type
            switch( lt ) { 
            case cdouble:
                {
                    if (MONGO_unlikely(lt_real == cint64 && rt_real == cint64)) {
                        long long L = *reinterpret_cast<const long long *>(l);
                        long long R = *reinterpret_cast<const long long *>(r);
                        if (L < R) {
                            return -1;
                        }
                        if (L != R) {
                            return 1;
                        }
                    } else {
                        // We only pack numbers as cint64 if they are larger than the largest thing
                        // we would store as a double.  However, user inputted doubles can be larger
                        // than 2^52 and just be packed as doubles because they came that way, so we
                        // need to actually do the comparison, not just take the one that's packed
                        // as an int to be greater or lesser.
                        double L = (MONGO_unlikely(lt_real == cint64)
                                    ? double(*reinterpret_cast<const long long *>(l))
                                    : (reinterpret_cast<const PackedDouble *>(l))->d);
                        double R = (MONGO_unlikely(rt_real == cint64)
                                    ? double(*reinterpret_cast<const long long *>(r))
                                    : (reinterpret_cast<const PackedDouble *>(r))->d);
                        if (L < R) {
                            return -1;
                        }
                        if (L != R) {
                            return 1;
                        }
                    }
                    l += 8; r += 8;
                    break;
                }
            case cstring:
                {
                    int lsz = *l;
                    int rsz = *r;
                    int common = min(lsz, rsz);
                    l++; r++; // skip the size byte
                    // use memcmp as we (will) allow zeros in UTF8 strings
                    int res = memcmp(l, r, common);
                    if( res ) 
                        return res;
                    // longer string is the greater one
                    int diff = lsz-rsz;
                    if( diff ) 
                        return diff;
                    l += lsz; r += lsz;
                    break;
                }
            case cbindata:
                {
                    int L = *l;
                    int R = *r;
                    int llen = binDataCodeToLength(L);
                    int diff = L-R; // checks length and subtype simultaneously
                    if( diff ) {
                        // unfortunately nibbles are backwards to do subtype and len in one check (could bit swap...)
                        int rlen = binDataCodeToLength(R);
                        if( llen != rlen ) 
                            return llen - rlen;
                        return diff;
                    }
                    // same length, same type
                    l++; r++;
                    int res = memcmp(l, r, llen);
                    if( res ) 
                        return res;
                    l += llen; r += llen;
                    break;
                }
            case cdate:
                {
                    long long L = *((long long *) l);
                    long long R = *((long long *) r);
                    if( L < R )
                        return -1;
                    if( L > R )
                        return 1;
                    l += 8; r += 8;
                    break;
                }
            case coid:
                {
                    int res = memcmp(l, r, sizeof(OID));
                    if( res ) 
                        return res;
                    l += 12; r += 12;
                    break;
                }
            default:
                // all the others are a match -- e.g. null == null
                ;
            }

            return 0;
        }

        inline int KeyV1::woCompare(const KeyV1& right, const Ordering &order) const {
            const unsigned char *l = _keyData;
            const unsigned char *r = right._keyData;

            if(MONGO_unlikely((*l|*r) == IsBSON )) // only can do this if cNOTUSED maintained
                return compareHybrid(right, order);

            unsigned mask = 1;
            while( 1 ) { 
                char lval = *l; 
                char rval = *r;
                {
                    int x = compare(l, r); // updates l and r pointers
                    if( x ) {
                        if( order.descending(mask) )
                            x = -x;
                        return x;
                    }
                }

                {
                    int x = ((int)(lval & cHASMORE)) - ((int)(rval & cHASMORE));
                    if( x ) 
                        return x;
                    if( (lval & cHASMORE) == 0 )
                        break;
                }

                mask <<= 1;
            }

            return 0;
        }

        inline int KeyV1::dataSize() const {
            const unsigned char *p = _keyData;
            if( !isCompactFormat() ) {
                return bson().objsize() + 1;
            }

            bool more;
            do { 
                unsigned z = sizeOfElement(p);
                more = (*p & cHASMORE) != 0;
                p += z;
            } while( more );
            return p - _keyData;
        }

    } // namespace storage

} // namespace mongo

