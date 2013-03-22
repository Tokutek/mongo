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

#include "mongo/pch.h"
#include "oplog_helpers.h"
#include "txn_context.h"


#define KEY_STR_OP_NAME "op"
#define KEY_STR_NS "ns"
#define KEY_STR_ROW "o";
#define KEY_STR_OLD_ROW "o";
#define KEY_STR_NEW_ROW "o2";
#define KEY_STR_COMMENT "o";
#define KEY_STR_MIGRATE "fromMigrate";

// values for types of operations in opLog
#define OP_STR_INSERT "i"
#define OP_STR_UPDATE "u"
#define OP_STR_DELETE "d"
#define OP_STR_COMMENT "n"

namespace mongo {
namespace OpLogHelpers{

    static inline void appendOpType(const char *opstr, BSONObjBuilder* b) {
        b->append(KEY_STR_OP_NAME, opstr);
    }
    static inline void appendNsStr(const char *ns, BSONObjBuilder* b) {
        b->append(KEY_STR_NS, ns);
    }
    static inline void appendMigrate(bool fromMigrate, BSONObjBuilder* b) {
        if (fromMigrate) {
            b->append(KEY_STR_MIGRATE, true);
        }
    }
    
    void logComment(BSONObj comment, const TxnContext* txn) {
        BSONObjBuilder b;
        appendOpType(OP_STR_COMMENT, &b);
        b.append(KEY_STR_COMMENT, comment);
        txn->logOp(b.obj());
    }
    
    void logInsert(const char* ns, BSONObj row, const TxnContext* txn) {
        BSONObjBuilder b;
        appendOpType(OP_STR_INSERT, &b);
        appendNsStr(ns, &b);
        b.append(KEY_STR_ROW, row);
        txn->logOp(b.obj());
    }

    void logUpdate(
        const char* ns, 
        BSONObj oldRow, 
        BSONObj newRow,
        bool fromMigrate,
        const TxnContext* txn
        ) 
    {
        BSONObjBuilder b;
        appendOpType(OP_STR_UPDATE, &b);
        appendNsStr(ns, &b);
        appendMigrate(fromMigrate, &b);
        b.append(KEY_STR_OLD_ROW, oldRow);
        b.append(KEY_STR_NEW_ROW, newRow);
        txn->logOp(b.obj());
    }

    void logDelete(const char* ns, BSONObj row, bool fromMigrate, const TxnContext* txn) {
        BSONObjBuilder b;
        appendOpType(OP_STR_DELETE, &b);
        appendNsStr(ns, &b);
        appendMigrate(fromMigrate, &b);
        b.append(KEY_STR_ROW, row);
        txn->logOp(b.obj());
    }
    
} // namespace OpLogHelpers
} // namespace mongo
