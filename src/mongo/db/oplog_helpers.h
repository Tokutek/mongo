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
        const string ns; // fully qualified namespace (database_name.collection_name)
        const BSONObj pk;
        DocID(const char* nsParam, const BSONObj& primaryKey) : ns(nsParam), pk(primaryKey.getOwned()){
        }
    };

    class RollbackDocsMap {
        BSONObj _current;
        static void _dropDocsMap();
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
        // BSON fields for oplog entries
        const char *const KEY_STR_OP_NAME = "op";
        const char *const KEY_STR_NS = "ns";
        const char *const KEY_STR_ROW = "o";
        const char *const KEY_STR_OLD_ROW = "o";
        const char *const KEY_STR_NEW_ROW = "o2";
        const char *const KEY_STR_MODS = "m";
        const char *const KEY_STR_QUERY = "q";
        const char *const KEY_STR_FLAGS = "f";
        const char *const KEY_STR_PK = "pk";
        const char *const KEY_STR_COMMENT = "o";
        const char *const KEY_STR_MIGRATE = "fromMigrate";

        // values for types of operations in oplog
        const char OP_STR_INSERT[] = "i"; // normal insert
        const char OP_STR_CAPPED_INSERT[] = "ci"; // insert into capped collection
        const char OP_STR_UPDATE[] = "u"; // normal update with full pre-image and full post-image
        const char OP_STR_UPDATE_ROW_WITH_MOD[] = "ur"; // update with full pre-image and mods to generate post-image
        const char OP_STR_DELETE[] = "d"; // delete with full pre-image
        const char OP_STR_CAPPED_DELETE[] = "cd"; // delete from capped collection
        const char OP_STR_COMMENT[] = "n"; // a no-op
        const char OP_STR_COMMAND[] = "c"; // command

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

        void applyOperationFromOplog(const BSONObj& op, RollbackDocsMap* docsMap, const bool inRollback);

        void rollbackOperationFromOplog(const BSONObj& op, RollbackDocsMap* docsMap);

    }

} // namespace mongo

