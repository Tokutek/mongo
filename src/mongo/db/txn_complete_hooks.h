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

#include "mongo/db/client.h"
#include "mongo/db/txn_context.h"

namespace mongo {

    class TxnCompleteHooks {
    public:
        virtual ~TxnCompleteHooks() { }
        virtual void noteTxnCompletedInserts(const string &ns, const BSONObj &minPK,
                                             long long nDelta, long long sizeDelta,
                                             bool committed) {
            assertNotImplemented();
        }
        virtual void noteTxnAbortedFileOps(const set<string> &namespaces, const set<string> &dbs) {
            assertNotImplemented();
        }
        virtual void noteTxnCompletedCursors(const set<long long> &cursorIds) {
            assertNotImplemented();
        }
    private:
        void assertNotImplemented() {
            msgasserted(16778, "bug: TxnCompleteHooks not set");
        }
    };

    // On startup, we install these hooks for txn completion.
    // In a perfect world we don't need an interface and we could just link
    // directly with these functions, but linking is funny right new between
    // coredb/mongos/mongod etc.
    class TxnCompleteHooksImpl : public TxnCompleteHooks {
    public:
        void noteTxnCompletedInserts(const string &ns, const BSONObj &minPK,
                                     long long nDelta, long long sizeDelta,
                                     bool committed);

        void noteTxnAbortedFileOps(const set<string> &namespaces, const set<string> &dbs);

        void noteTxnCompletedCursors(const set<long long> &cursorIds);

    };

    extern TxnCompleteHooksImpl _txnCompleteHooks;

} // namespace mongo
