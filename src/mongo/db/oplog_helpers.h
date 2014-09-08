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

namespace mongo {

    // objects used for rollback
    class DocID {
    public:
        const string coll; // fully qualified namespace (database_name.collection_name)
        const BSONObj pk;
        DocID(const char* ns, const BSONObj& primaryKey) : coll(ns), pk(primaryKey.getOwned()){
        }
    };

    struct DocIDCmp {
        bool operator()(const DocID& l, const DocID& r) const {
            int stringCmp = l.coll.compare(r.coll);
            if (stringCmp != 0) {
                return stringCmp < 0;
            }
            return l.pk.woCompare(r.pk) < 0;
        }
    };

    class RollbackDocsMap {
        BSONObj _current;
    public:
        RollbackDocsMap() {}
        void initialize();
        static void dropDocsMap();
        bool docExists(const char* ns, const BSONObj pk) const;
        bool docsForNSExists(const char* ns) const;
        void addDoc(const StringData &ns, const BSONObj& pk);
        long long size();
    };

    class RollbackDocsMapIterator {
        BSONObj _current;
    public:
        RollbackDocsMapIterator();
        bool ok();
        void advance();
        DocID current();
    };

    namespace OplogHelpers {

        // helper functions for sharding
        bool shouldLogOpForSharding(const char *opstr);
        bool invalidOpForSharding(const char *opstr);

        // Used by normal operations to write to the oplog

        void logComment(const BSONObj &comment);

        void logInsert(const char *ns, const BSONObj &row, bool fromMigrate);

        void logInsertForCapped(const char *ns, const BSONObj &pk, const BSONObj &row);

        void logUpdate(const char *ns, const BSONObj &pk, const BSONObj &oldObj, const BSONObj &newObj, bool fromMigrate);

        void logUpdatePKModsWithRow(const char *ns, const BSONObj &pk, const BSONObj &oldObj, const BSONObj &updateobj, const BSONObj& query, const uint32_t fastUpdateFlags, bool fromMigrate);

        void logUpdateModsWithRow(const char *ns, const BSONObj &pk, const BSONObj &oldObj, const BSONObj &updateobj, bool fromMigrate);

        void logDelete(const char *ns, const BSONObj &row, bool fromMigrate);

        void logDeleteForCapped(const char *ns, const BSONObj &pk, const BSONObj &row);

        void logCommand(const char *ns, const BSONObj &row);

        void logUnsupportedOperation(const char *ns);
        // Used by secondaries to process oplog entries

        void applyOperationFromOplog(const BSONObj& op, RollbackDocsMap* docsMap);

        void rollbackOperationFromOplog(const BSONObj& op, RollbackDocsMap* docsMap);

    }

} // namespace mongo

