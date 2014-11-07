/** @file index_descriptor.h */

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
#include "mongo/db/jsobj.h"
#include "mongo/db/storage/key.h"

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
        // For creating a brand new descriptor.
        Descriptor(const BSONObj &keyPattern,
                   const bool hashed = false,
                   const int hashSeed = 0,
                   const bool sparse = false,
                   const bool clustering = false);
        // For interpretting a memory buffer as a descriptor.
        Descriptor(const char *data, const size_t size);

        bool operator==(const Descriptor &rhs) const;

        int version() const;

        const Ordering &ordering() const;

        DBT dbt() const;

        int compareKeys(const storage::Key &key1, const storage::Key &key2) const {
            return key1.woCompare(key2);
        }

        void generateKeys(const BSONObj &obj, BSONObjSet &keys) const;

        BSONObj fillKeyFieldNames(const BSONObj &key) const;

        bool clustering() const {
            const Header &h(*reinterpret_cast<const Header *>(_data));
            return h.clustering;
        }

        static size_t serializedSize(const BSONObj &keyPattern);

    private:
        void fieldNames(vector<const char *> &fields) const;

#pragma pack(1)
        // Descriptor format:
        //   [
        //     4 bytes: ordering,
        //     1 byte: version,
        //     1 byte: hashed boolean,
        //     1 byte: sparse boolean,
        //     1 byte: clustering boolean,
        //     4 bytes: hash seed integer,
        //     4 bytes: integer number of fields
        //     integer array: array of offsets into subsequent byte array for each field string
        //     byte array: array of null terminated field strings
        //   ]
        struct Header {
        private:
            enum Version {
                // Version 0 is kind of a fake version.
                VERSION_0 = 0,
                VERSION_1 = 1,
                NEXT_VERSION = 2
            };
            static const int CURRENT_VERSION = (int) NEXT_VERSION - 1;

        public:
            Header(const Ordering &o, char h, char s, char c, int hs, uint32_t n)
                : ordering(o), version((char) CURRENT_VERSION), hashed(h), sparse(s), clustering(c),
                  hashSeed(hs), numFields(n) {
            }

            Ordering ordering;
            char version;
            char hashed;
            char sparse;
            char clustering;
            int hashSeed;
            uint32_t numFields;
        };

        static const int FixedSize = sizeof(Header);
        BOOST_STATIC_ASSERT(FixedSize == 16);
#pragma pack()

        const char *_data;
        const size_t _size;
        scoped_array<char> _dataOwned;
    };

} // namespace mongo

