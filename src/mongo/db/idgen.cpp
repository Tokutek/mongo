//@file idgen.cpp

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

#include "idgen.h"

#include "pch.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/oid.h"

namespace mongo {

#pragma pack(1)
    struct IDToInsert_ {
        char type;
        char _id[4];
        OID oid;
        IDToInsert_() {
            type = (char) jstOID;
            strcpy(_id, "_id");
            verify( sizeof(IDToInsert_) == 17 );
        }
    } idToInsert_;
    struct IDToInsert : public BSONElement {
        IDToInsert() : BSONElement( ( char * )( &idToInsert_ ) ) {}
    } idToInsert;
#pragma pack()

    BSONObj addIdField(const BSONObj &orig) {
        static SimpleMutex mutex("addIdField");

        if (orig.hasField("_id")) {
            return orig;
        } else {
            SimpleMutex::scoped_lock lk(mutex);
            BSONObjBuilder b;
            b.appendElements(orig);
            idToInsert_.oid.init();
            b.append(idToInsert);
            return b.obj();
        }
    }

} // namespace mongo
