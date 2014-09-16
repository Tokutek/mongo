// index.h

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

#pragma once

#include "mongo/pch.h"

#include <db.h>
#include <vector>

#include "mongo/db/jsobj.h"
#include "mongo/db/index.h"

namespace mongo {

    class Cursor;
    class PartitionedCollection;

    // IndexDetails class for PartitionedCollections
    class PartitionedIndexDetails : public IndexDetails {
    public:
        PartitionedIndexDetails(const BSONObj &info, PartitionedCollection *pc); 

        ~PartitionedIndexDetails();

        virtual enum toku_compression_method getCompressionMethod() const;

        virtual uint32_t getFanout() const;

        virtual uint32_t getPageSize() const;

        virtual uint32_t getReadPageSize() const;

        virtual void getStat64(DB_BTREE_STAT64* stats) const;

        // find a way to remove this eventually and have callers get
        // access to IndexInterface directly somehow
        // This is a workaround to get going for now
        virtual shared_ptr<storage::Cursor> getCursor(const int flags) const;

    private:
        IndexDetails& getIndexDetailsOfPartition(uint64_t i)  const;
        // This cannot be a shared_ptr, as this is a circular reference
        // _pic has a reference to this as well.
        PartitionedCollection *_pc;
    };

} // namespace mongo

