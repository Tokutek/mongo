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

namespace mongo {

    typedef enum {
        DEFAULT_LOCK_CURSOR, // Cursors will pass no flags and use default settings of transaction
        READ_LOCK_CURSOR, //Cursors are serializable and grab read locks
        WRITE_LOCK_CURSOR // cursors are serializable and grab write locks (DB_RMW)
    } QueryCursorMode;

    class OpSettings {
        QueryCursorMode _queryCursorMode; // default DEFAULT_LOCK_CURSOR
        bool _shouldBulkFetch; // default false
        bool _shouldAppendPKForCapped; // if true, cursor->current should append the pk before returning the row
      public:
        OpSettings();

        OpSettings& setQueryCursorMode(QueryCursorMode mode);
        QueryCursorMode getQueryCursorMode();
        bool shouldBulkFetch();
        OpSettings& setBulkFetch(bool val);
        bool shouldCappedAppendPK();
        OpSettings& setCappedAppendPK(bool val);
    };

} // namespace mongo
