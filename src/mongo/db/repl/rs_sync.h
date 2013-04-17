/**
 *    Copyright (C) 2008 10gen Inc.
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

#include <deque>
#include <vector>

#include "mongo/db/client.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/oplog.h"
#include "mongo/util/concurrency/thread_pool.h"

namespace mongo {

    class BackgroundSyncInterface;

    /**
     * "Normal" replica set syncing
     */

    // TODO: move hbmsg into an error-keeping class (SERVER-4444)
    void sethbmsg(const string& s, const int logLevel=0);

} // namespace mongo
