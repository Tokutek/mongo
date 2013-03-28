// client.cpp

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

#include "pch.h"

#include "mongo/db/toku_command_settings.h"

namespace mongo {

    TokuCommandSettings::TokuCommandSettings() 
        : _queryCursorMode(DEFAULT_LOCK_CURSOR),
          _shouldBulkFetch(false) {
    }

    TokuCommandSettings::~TokuCommandSettings() {
    }

    void TokuCommandSettings::setQueryCursorMode(QueryCursorMode mode) {
        _queryCursorMode = mode;
    }

    QueryCursorMode TokuCommandSettings::getQueryCursorMode() {
        return _queryCursorMode;
    }

    bool TokuCommandSettings::shouldBulkFetch() {
        return _shouldBulkFetch;
    }

    void TokuCommandSettings::setBulkFetch(bool val) {
        _shouldBulkFetch = val;
    }

} // namespace mongo
