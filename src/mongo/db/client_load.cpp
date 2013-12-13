// client_load.cpp

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

/**
 * Implements collection loading for a client.
 */

#include "mongo/pch.h"

#include <db.h>

#include "mongo/db/client.h"
#include "mongo/db/namespace_details.h"

namespace mongo {

    // The client begin/commit/abort load functions handle locking/context,
    // creating a child transaction for the load, and ensuring that this client
    // only loads one ns at a time.

    void Client::beginClientLoad(const StringData &ns, const vector<BSONObj> &indexes,
                                 const BSONObj &options) {
        uassert( 16915, "Cannot begin load, one is already in progress",
                        !loadInProgress() );

        shared_ptr<Client::LoadInfo> loadInfo(new Client::LoadInfo(ns));
        LOCK_REASON(lockReason, "loader: beginning load");
        Client::WriteContext ctx(ns, lockReason);
        beginBulkLoad(ns, indexes, options);
        _loadInfo = loadInfo;
    }

    void Client::commitClientLoad() {
        uassert( 16876, "Cannot commit client load, none in progress.",
                        loadInProgress() );

        shared_ptr<Client::LoadInfo> loadInfo = _loadInfo;
        _loadInfo.reset();
        const string &ns = loadInfo->bulkLoadNS();

        {
            LOCK_REASON(lockReason, "loader: committing load");
            Client::WriteContext ctx(ns, lockReason);
            commitBulkLoad(ns);
        }
        loadInfo->commitTxn();
    }

    void Client::abortClientLoad() {
        uassert( 16888, "Cannot abort client load, none in progress.",
                        loadInProgress() );

        shared_ptr<Client::LoadInfo> loadInfo = _loadInfo;
        _loadInfo.reset();
        const string &ns = loadInfo->bulkLoadNS();
        LOCK_REASON(lockReason, "loader: aborting load");
        Client::WriteContext ctx(ns, lockReason);
        abortBulkLoad(ns);
    }

    bool Client::loadInProgress() const {
        return _loadInfo;
    }

} // namespace mongo
