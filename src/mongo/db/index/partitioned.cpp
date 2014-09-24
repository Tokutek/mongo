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

#include "mongo/db/collection.h"
#include "mongo/db/cursor.h"
#include "mongo/db/index.h"
#include "mongo/db/index/partitioned.h"
#include "mongo/db/storage/env.h"

namespace mongo {

    PartitionedIndexDetails::PartitionedIndexDetails(const BSONObj &info,
                                                     PartitionedCollection *pc) :
        IndexDetails(info), _pc(pc) {
    }

    PartitionedIndexDetails::~PartitionedIndexDetails() { }

    enum toku_compression_method PartitionedIndexDetails::getCompressionMethod() const {
        return getIndexDetailsOfPartition(0).getCompressionMethod();
    }

    uint32_t PartitionedIndexDetails::getFanout() const {
        return getIndexDetailsOfPartition(0).getFanout();
    }

    uint32_t PartitionedIndexDetails::getPageSize() const {
        return getIndexDetailsOfPartition(0).getPageSize();
    }

    uint32_t PartitionedIndexDetails::getReadPageSize() const {
        return getIndexDetailsOfPartition(0).getReadPageSize();
    }

    void PartitionedIndexDetails::getStat64(DB_BTREE_STAT64* stats) const {
        DB_BTREE_STAT64 ret;
        memset(&ret, 0, sizeof(ret));
        ret.bt_verify_time_sec = static_cast<uint64_t>(-1);
        for (uint64_t i = 0; i < _pc->numPartitions(); i++) {
            DB_BTREE_STAT64 curr;
            getIndexDetailsOfPartition(i).getStat64(&curr);
            ret.bt_nkeys += curr.bt_nkeys;
            ret.bt_ndata += curr.bt_ndata;
            ret.bt_dsize += curr.bt_dsize;
            ret.bt_fsize += curr.bt_fsize;
            if (curr.bt_create_time_sec > ret.bt_create_time_sec) {
                ret.bt_create_time_sec = curr.bt_create_time_sec;
            }
            if (curr.bt_modify_time_sec > ret.bt_modify_time_sec) {
                ret.bt_modify_time_sec = curr.bt_modify_time_sec;
            }
            if (curr.bt_verify_time_sec < ret.bt_verify_time_sec) {
                ret.bt_verify_time_sec = curr.bt_verify_time_sec;
            }
        }
        *stats = ret;
    }
    
    // find a way to remove this eventually and have callers get
    // access to IndexDetailsBase directly somehow
    // This is a workaround to get going for now
    shared_ptr<storage::Cursor> PartitionedIndexDetails::getCursor(const int flags) const {
        uasserted(17243, "should not call getCursor on a PartitionedIndexDetails");
    }
    
    IndexDetails& PartitionedIndexDetails::getIndexDetailsOfPartition(uint64_t i) const {
        const int idxNum = _pc->findIndexByName(indexName());
        verify(idxNum >= 0);
        return _pc->getPartition(i)->idx(idxNum);
    }

} // namespace mongo
