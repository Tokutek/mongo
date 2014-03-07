/**
*    Copyright (C) 2014 Tokutek Inc.
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

#include "mongo/base/string_data.h"
#include "mongo/util/assert_util.h"

namespace mongo {

    // dumpCrashInfo will log as much information as we can get, along
    // with bug report instructions.  It is suitable for use within
    // terminate and signal handlers (i.e. doesn't malloc), but shouldn't
    // be used directly by normal code, it's really intended to be used by
    // the signal handlers.
    void dumpCrashInfo(const StringData &reason);
    void dumpCrashInfo(const DBException &e);

} // namespace mongo
