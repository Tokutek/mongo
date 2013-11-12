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

    namespace OpLogHelpers {

        // values for types of operations in oplog
        static const char OP_STR_INSERT[] = "i";
        static const char OP_STR_CAPPED_INSERT[] = "ci";
        static const char OP_STR_UPDATE[] = "u";
        static const char OP_STR_DELETE[] = "d";
        static const char OP_STR_CAPPED_DELETE[] = "cd";
        static const char OP_STR_COMMENT[] = "n";
        static const char OP_STR_COMMAND[] = "c";

        // Used by normal operations to write to the oplog

        void logComment(const BSONObj &comment);

        void logInsert(const char *ns, const BSONObj &row);    

        void logInsertForCapped(const char *ns, const BSONObj &pk, const BSONObj &row);

        void logUpdate(const char *ns, const BSONObj& pk, const BSONObj& oldRow, const BSONObj& newRow, bool fromMigrate);

        void logDelete(const char *ns, const BSONObj &row, bool fromMigrate);

        void logDeleteForCapped(const char *ns, const BSONObj &pk, const BSONObj &row);

        void logCommand(const char *ns, const BSONObj &row);

        // Used by secondaries to process oplog entries

        void applyOperationFromOplog(const BSONObj& op);

        void rollbackOperationFromOplog(const BSONObj& op);

    }

} // namespace mongo

