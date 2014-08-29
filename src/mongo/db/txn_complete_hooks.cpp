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


#include "mongo/pch.h"

#include "mongo/db/client.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/collection.h"
#include "mongo/db/collection_map.h"
#include "mongo/db/databaseholder.h"
#include "mongo/db/txn_context.h"

namespace mongo {

    TxnCompleteHooksImpl _txnCompleteHooks;

    void TxnCompleteHooksImpl::noteTxnCompletedInserts(const string &ns, const BSONObj &minPK,
                                         long long nDelta, long long sizeDelta,
                                         bool committed) {
        LOCK_REASON(lockReason, "txn: noting completed inserts");
        Lock::DBRead lk(ns, lockReason);
        if (dbHolder().__isLoaded(ns, storageGlobalParams.dbpath)) {
            scoped_ptr<Client::Context> ctx(cc().getContext() == NULL ?
                                            new Client::Context(ns) : NULL);
            // Because this transaction did inserts, we're guaranteed to be the
            // only party capable of closing/reopening the ns due to file-ops.
            // So, if the ns is open, note the commit/abort to fix up in-memory
            // stats and do nothing otherwise since there are no stats to fix.
            //
            // Only matters for capped collections.
            CollectionMap *cm = collectionMap(ns);
            Collection *cl = cm->find_ns(ns);
            if (cl != NULL && cl->isCapped()) {
                CappedCollection *cappedCl = cl->as<CappedCollection>();
                if (committed) {
                    cappedCl->noteCommit(minPK, nDelta, sizeDelta);
                } else {
                    cappedCl->noteAbort(minPK, nDelta, sizeDelta);
                }
            }
        }
    }

    void TxnCompleteHooksImpl::noteTxnAbortedFileOps(const set<string> &namespaces, const set<string> &dbs) {
        for (set<string>::const_iterator i = namespaces.begin(); i != namespaces.end(); i++) {
            const char *ns = i->c_str();

            // We cannot be holding a read lock at this point, since we're in one of two situations:
            // - Single-statement txn is aborting. If it did fileops, it had to hold a write lock,
            //   and therefore it still is.
            // - Multi-statement txn is aborting. The only way to do this is through a command that
            //   takes no lock, therefore we're not read locked.
            verify(!Lock::isReadLocked());

            // If something is already write locked we must be in the single-statement case, so
            // assert that the write locked namespace is this one.
            if (Lock::somethingWriteLocked()) {
                verify(Lock::isWriteLocked(ns));
            }

            // The ydb requires that a txn closes any dictionaries it created beforeaborting.
            // Hold a write lock while trying to close the namespace in the collection map.
            LOCK_REASON(lockReason, "txn: closing created dictionaries during txn abort");
            Lock::DBWrite lk(ns, lockReason);
            if (dbHolder().__isLoaded(ns, storageGlobalParams.dbpath)) {
                scoped_ptr<Client::Context> ctx(cc().getContext() == NULL ?
                                                new Client::Context(ns) : NULL);
                // Pass aborting = true to close_ns(), which hints to the implementation
                // that the calling transaction is about to abort.
                (void) collectionMap(ns)->close_ns(ns, true);
            }
        }

        for (set<string>::const_iterator it = dbs.begin(); it != dbs.end(); ++it) {
            const string &db = *it;

            // The same locking rules above apply here.
            verify(!Lock::isReadLocked());
            if (Lock::somethingWriteLocked()) {
                verify(Lock::isWriteLocked(db));
            }

            LOCK_REASON(lockReason, "txn: rolling back db creates");
            Lock::DBWrite lk(db, lockReason);
            if (dbHolder().__isLoaded(db, storageGlobalParams.dbpath)) {
                scoped_ptr<Client::Context> ctx(cc().getContext() == NULL ?
                                                new Client::Context(db) : NULL);
                collectionMap(db)->rollbackCreate();
            }
        }
    }

    // If a txn is completing, the cursors it created
    // must be killed before it can commit or abort.
    void TxnCompleteHooksImpl::noteTxnCompletedCursors(const set<long long> &cursorIds) {
        for (set<long long>::const_iterator i = cursorIds.begin(); i != cursorIds.end(); ++i) {
            ClientCursor::erase(*i);
        }
    }

} // namespace mongo
