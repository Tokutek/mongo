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

#include <tokudb.h>

#include "bson/bsonobj.h"

#define UNUSED __attribute__((__unused__))

namespace toku {

// index keys are big enough to fit the bson object plus a diskloc
UNUSED
static size_t index_key_size(const mongo::BSONObj &obj) {
    return obj.objsize() + sizeof(mongo::DiskLoc);
}

// generate an index key dbt for the given bson obj and diskloc. 
// the buf must be index_key_size(obj) so everything fits.
UNUSED
static DBT generate_index_key(char *key_buf, const mongo::BSONObj &obj, const mongo::DiskLoc &loc) {
    DBT index_key;
    index_key.data = key_buf;
    index_key.size = index_key_size(obj);
    index_key.ulen = index_key_size(obj);
    index_key.flags = DB_DBT_USERMEM;
    // copy the object and diskloc into the dbt's buf
    memcpy(key_buf, obj.objdata(), obj.objsize());
    memcpy(key_buf + obj.objsize(), &loc, sizeof(mongo::DiskLoc));
    return index_key;
}

// get a dbt whose data buffer and size are borrowed from the given bsonobj
UNUSED
static DBT init_dbt_from_bson_obj(const mongo::BSONObj &obj) {
    DBT dbt;
    dbt.data = const_cast<char *>(obj.objdata());
    dbt.size = obj.objsize();
    dbt.ulen = obj.objsize();
    dbt.flags = DB_DBT_USERMEM;
    return dbt;
}

// create a bson object from an index key. the index key contains the
// bson object followed by the diskloc. if we initialize the object's
// internal buffer to the index key's buffer, it will only 'see' the
// bson object and ignore that the recordloc is afterwards.
UNUSED
static inline mongo::BSONObj init_bson_from_dbt(const DBT *index_key) {
    const char *bson_buf = reinterpret_cast<char *>(index_key->data);
    mongo::BSONObj obj = mongo::BSONObj(bson_buf);
    return obj;
}

// create a diskloc object from the given index key
// the recordloc lives at the end of the index key buffer
UNUSED
static inline mongo::DiskLoc init_diskloc_from_dbt(const DBT *index_key) {
    mongo::DiskLoc loc;
    size_t size = sizeof(mongo::DiskLoc);
    const char *buf = reinterpret_cast<char *>(index_key->data);
    memcpy(&loc, buf + index_key->size - size, size);
    return loc;
}

// dump the contents of a dbt in a non-compact, visually pleasing way
UNUSED
static inline void dump_dbt(const char *name, const DBT *dbt) {
    printf("%s: {\n", name);    
    printf("   size = %d,\n", dbt->size);
    printf("   data = [");
    for (uint32_t i = 0; i < dbt->size; i++) {
    printf("%3u ", ((unsigned char *)dbt->data)[i]);
    }
    printf("]\n");
    printf("}\n");
}

} /* namespace toku */
