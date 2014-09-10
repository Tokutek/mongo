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
#include "mongo/db/descriptor.h"
#include "mongo/db/index/s2.h"
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

    class S2Descriptor : public Descriptor {
    public:
        // For creating a brand new descriptor.
        S2Descriptor(const BSONObj &keyPattern,
                     const S2IndexingParams &params,
                     const vector<S2Index::IndexedField> &fields,
                     const bool sparse = false,
                     const bool clustering = false);

        // For interpretting a memory buffer as a descriptor.
        S2Descriptor(const char *data, const size_t size);

        // Subclasses should implement this if they wish to store more than
        // just the basic amount of information in the dictionary's descriptor.
        // Useful for hashed, geo, fts, etc indexes.
        DBT dbt(scoped_array<char> &buf) const;

        // Subclasses should implement this if key generation is to be non-standard.
        void generateKeys(const BSONObj &obj, BSONObjSet &keys) const;

    private:
        S2IndexingParams _params;
        vector<S2Index::IndexedField> _fields;
    };

} // namespace mongo

