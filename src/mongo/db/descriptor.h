/** @file descriptor.h */

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

#include <string>

#include "mongo/pch.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/storage/dbt.h"
#include "mongo/db/storage/key.h"
#include "mongo/util/assert_util.h"

namespace mongo {

    // A Descriptor contains the necessary information for comparing
    // and generating index keys and values.
    //
    // Descriptors are stored on disk as a ydb dictionary's DESCRIPTOR (excuse the screaming typedef).
    //
    // It provides the API glue for the ydb's environment-wide comparison
    // function as well as the generate rows for put/del/update functions.

    class Descriptor {
    public:
        // This enum may never be rearranged - we serialize these values to disk
        // and depend on them to unambiguously indentify the descriptor type.
        enum Type {
            Basic = 0,
            Hashed = 1,
            S2 = 2,
            Haystack = 3,
            TwoD = 4,
        };

        enum Version {
            // Version 0 is kind of a fake version.
            VERSION_0 = 0,
            // 'hashed' was the only special kind of index
            VERSION_1 = 1,
            // - 'hashed' bool is now a type char, to support geo / FTS etc
            // - no longer have hashSeed where it's not needed
            // - stores the length of the field names so can determine how big
            //   this portion of the desc is for nesting purposes
            VERSION_2 = 2,
        };
        static const int CURRENT_VERSION = (int) VERSION_2;

        // For creating a brand new descriptor at the current version.
        Descriptor(const BSONObj &keyPattern,
                   const Type type = Basic,
                   const bool sparse = false,
                   const bool clustering = false);

        // For interpretting a memory buffer as a descriptor.
        Descriptor(const char *data, const size_t size) :
            _data(data), _size(size) {
            verify(_data != NULL);
            // Strictly greater, since there should be
            // at least one field after the header
            verify(_size > _headerSize());
        }

        // For getting a specific implementation of descriptor serialized in a DBT
        static void getFromDBT(const DBT *dbt, scoped_ptr<Descriptor> &ptr);

        virtual ~Descriptor() { }

        bool operator==(const Descriptor &rhs) const {
            // We use size() here because it reflects the actual size of the
            // descriptor, not the size of the buffer that the descriptor lives in.
            return size() == rhs.size() && memcmp(_data, rhs._data, size()) == 0;
        }

        int version() const {
            const Header &h(*reinterpret_cast<const Header *>(_data));
            return h.version;
        }

        const Ordering &ordering() const {
            const Header &h(*reinterpret_cast<const Header *>(_data));
            return h.ordering;
        }

        int compareKeys(const storage::Key &key1, const storage::Key &key2) const {
            return key1.woCompare(key2, ordering());
        }

        char type() const {
            const Header &h(*reinterpret_cast<const Header *>(_data));
            return h.type;
        }

        bool sparse() const {
            const Header &h(*reinterpret_cast<const Header *>(_data));
            return h.sparse;
        }

        bool clustering() const {
            const Header &h(*reinterpret_cast<const Header *>(_data));
            return h.clustering;
        }

        uint32_t numFields() const {
            const Header &h(*reinterpret_cast<const Header *>(_data));
            return h.numFields;
        }

        void fieldNames(vector<const char *> &fields) const;

        BSONObj fillKeyFieldNames(const BSONObj &key) const;

        // Subclasses should implement this if they wish to store more than
        // just the basic amount of information in the dictionary's descriptor.
        // Useful for hashed, geo, fts, etc indexes.
        virtual DBT dbt(scoped_array<char> &buf) const;

        // Subclasses should implement this if key generation is to be non-standard.
        virtual void generateKeys(const BSONObj &obj, BSONObjSet &keys) const;

    private:
        // Helps us figure out how big a descriptor will be when serialized
        // with the given key pattern using the current layout version.
        static size_t serializedSizeCurrentVersion(const BSONObj &keyPattern);

#pragma pack(1)
        // Descriptor format:
        //   [
        //     <epoch: no descriptor versioning>
        //       4 bytes: ordering
        //     <new in v1>
        //       1 byte: version
        //       1 byte: type char (had values 0 or 1 in v1, expanded in v2)
        //       1 byte: sparse boolean
        //       1 byte: clustering boolean
        //       4 bytes: hash seed (deprecated by v2, see HashedDescriptor)
        //       4 bytes: integer number of fields
        //     <new in v2>
        //       4 bytes: size of the byte array containing field strings
        //       integer array: array of offsets into subsequent byte array for each field string
        //       byte array: array of null terminated field strings
        //     <from here on, subclasses of Descriptor add any required data, see index/*descriptor.h>
        //   ]
        struct Header {
            Header(const Ordering &o, char ty, char sp, char c, uint32_t n, uint32_t fl)
                : ordering(o), version((char) CURRENT_VERSION), type(ty), sparse(sp), clustering(c),
                  _hashSeedDeprecated(0), numFields(n), fieldsLength(fl) {
            }
            Ordering ordering;
            char version;

            // The type field is compatible with v1 since the only type was a
            // bool for hashed, and now hashed indexes have type == 1
            char type;
            char sparse;
            char clustering;

            int _hashSeedDeprecated;
            uint32_t numFields;

            // This field was added in v2
            uint32_t fieldsLength;
        };
#pragma pack()

        // important: we use descriptor->size == sizeof(Ordering) to detect
        //            descriptors that have a layout format from before descriptors
        //            had a version at all.
        BOOST_STATIC_ASSERT(sizeof(Header) != sizeof(Ordering));
        BOOST_STATIC_ASSERT(sizeof(Header) == 20);

        size_t _headerSize() const {
            // Headers at version 1 and below did not have the 4 byte `fieldsLength'
            // field that exists in the current header (see struct Header)
            return version() <= 1 ? sizeof(Header) - sizeof(uint32_t) : sizeof(Header);
        }

        uint32_t _fieldsLength() const {
            verify(version() >= 2);
            const Header &h(*reinterpret_cast<const Header *>(_data));
            return h.fieldsLength;
        }

        const char *_data;
        const size_t _size;
        scoped_array<char> _dataOwned;

    public:
        size_t size() const {
            // caution: _fieldsLength asserts that the desc is at least v2
            return _headerSize() + 4 * numFields() + _fieldsLength();
        }

    protected:
        // Used only by the HashedDescriptor to access the
        // hash seed in the header of a v1 descriptor
        int _hashSeedDeprecated() const {
            verify(version() == 1);
            const Header &h(*reinterpret_cast<const Header *>(_data));
            return h._hashSeedDeprecated;
        }
    };

} // namespace mongo

