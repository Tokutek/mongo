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

#pragma once

#include "mongo/pch.h"
#include "mongo/bson/bsonobjbuilder.h"

namespace mongo {

    class TxnContext;

// helpers for opLog stuff
namespace OpLogHelpers{

    void logComment(BSONObj comment, const TxnContext* txn);
    void logInsert(const char* ns, BSONObj row, const TxnContext* txn);
    void logUpdate(const char* ns, BSONObj oldRow, BSONObj newRow, bool fromMigrate, const TxnContext* txn);
    void logDelete(const char* ns, BSONObj row, bool fromMigrate, const TxnContext* txn);

}


} // namespace mongo

