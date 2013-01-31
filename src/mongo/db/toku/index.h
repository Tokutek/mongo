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

#ifndef TOKUDB_INDEX_H
#define TOKUDB_INDEX_H

#include "db/index.h"
#include "db/queryutil.h"

#include "db/toku/invariant.h"

namespace mongo {

class IndexInterfaceTokuDB : public IndexInterface {
public:

    virtual void dropIndex(const IndexDetails &idx);
    
    virtual int keyCompare(const BSONObj &k1, const BSONObj &k2, const Ordering &ordering) {
        return k1.woCompare(k2, ordering, false);    
    }

    virtual long long fullValidate(const DiskLoc& thisLoc, const BSONObj &order) { 
        printf("IndexInterfaceTokudb: %s not supported, doing nothing.\n", __FUNCTION__);
        return true;
    }

    virtual DiskLoc findSingle(const IndexDetails &indexdetails , const DiskLoc& thisLoc, const BSONObj& key) const;

    virtual bool unindex(const DiskLoc thisLoc, IndexDetails& id, const BSONObj& key, const DiskLoc recordLoc) const;

    virtual int bt_insert(const DiskLoc thisLoc, const DiskLoc recordLoc,
            const BSONObj& key, const Ordering &order, bool dupsAllowed,
            IndexDetails& idx, bool toplevel = true) const;

    // this version of bt_insert takes the full document, for clustering
    virtual int bt_insert_clustering(const DiskLoc thisLoc, const DiskLoc recordLoc,
            const BSONObj& key, const Ordering &order, bool dupsAllowed,
            IndexDetails& idx, const BSONObj &obj, bool toplevel = true) const;

    // for geo: 
    // all unsupported for tokudb
    virtual bool isUsed(DiskLoc thisLoc, int pos) { 
        printf("IndexInterfaceTokudb: %s not supported, returning false.\n", __FUNCTION__);
        return false;
    }
    
    virtual void keyAt(DiskLoc thisLoc, int pos, BSONObj& key, DiskLoc& recordLoc) {
        printf("IndexInterfaceTokudb: %s not supported, returning empty bson and recordloc.\n", __FUNCTION__);
        key = BSONObj();
        recordLoc = DiskLoc();
    }
    
    virtual BSONObj keyAt(DiskLoc thisLoc, int pos) {
        printf("IndexInterfaceTokudb: %s not supported, returning empty bson.\n", __FUNCTION__);
        return BSONObj();
    }
    
    virtual DiskLoc locate(const IndexDetails &idx , const DiskLoc& thisLoc, const BSONObj& key, const Ordering &order,
            int& pos, bool& found, const DiskLoc &recordLoc, int direction=1) { 
        printf("IndexInterfaceTokudb: %s not supported, returning empty diskloc.\n", __FUNCTION__);
        return DiskLoc();
    }
    
    virtual DiskLoc advance(const DiskLoc& thisLoc, int& keyOfs, int direction, const char *caller) { 
        printf("IndexInterfaceTokudb: %s not supported, returning empty diskloc.\n", __FUNCTION__);
        return DiskLoc();
    }

private:
    // if obj != NULL, then cluster the object with the index row
    int insert(const DiskLoc thisLoc, const DiskLoc recordLoc,
            const BSONObj& key, const Ordering &order, bool dupsAllowed,
            IndexDetails& idx, const BSONObj *obj, bool toplevel = true) const;
};

} /* namespace mongo */

#endif /* TOKUDB_INDEX_H */
