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
#include "mongo/db/databaseholder.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/txn_context.h"

namespace mongo {

    // On startup, we install these hooks for txn completion.
    // In a perfect world we don't need an interface and we could just link
    // directly with these functions, but linking is funny right new between
    // coredb/mongos/mongod etc.
    class TxnCompleteHooksImpl : public TxnCompleteHooks {
    public:
        virtual void noteTxnCompletedInserts(const string &ns, const BSONObj &minPK,
                                             long long nDelta, long long sizeDelta,
                                             bool committed) {
            Lock::DBRead lk(ns);
            if (dbHolder().__isLoaded(ns, dbpath)) {
                scoped_ptr<Client::Context> ctx(cc().getContext() == NULL ?
                                                new Client::Context(ns) : NULL);
                // Because this transaction did inserts, we're guarunteed to be the
                // only party capable of closing/reopening the ns due to file-ops.
                // So, if the ns is open, note the commit/abort to fix up in-memory
                // stats and do nothing otherwise since there are no stats to fix.
                NamespaceIndex *ni = nsindex(ns.c_str());
                NamespaceDetails *d = ni->find_ns(ns.c_str());
                if (d != NULL) {
                    if (committed) {
                        d->noteCommit(minPK, nDelta, sizeDelta);
                    } else {
                        d->noteAbort(minPK, nDelta, sizeDelta);
                    }
                }
            }
        }

        virtual void noteTxnAbortedFileOps(const set<string> &namespaces, const set<string> &dbs) {
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
                // Hold a write lock while trying to close the namespace in the nsindex.
                Lock::DBWrite lk(ns);
                if (dbHolder().__isLoaded(ns, dbpath)) {
                    scoped_ptr<Client::Context> ctx(cc().getContext() == NULL ?
                                                    new Client::Context(ns) : NULL);
                    (void) nsindex(ns)->close_ns(ns);
                }
            }

            for (set<string>::const_iterator it = dbs.begin(); it != dbs.end(); ++it) {
                const string &db = *it;

                // The same locking rules above apply here.
                verify(!Lock::isReadLocked());
                if (Lock::somethingWriteLocked()) {
                    verify(Lock::isWriteLocked(db));
                }

                Lock::DBWrite lk(db);
                if (dbHolder().__isLoaded(db, dbpath)) {
                    scoped_ptr<Client::Context> ctx(cc().getContext() == NULL ?
                                                    new Client::Context(db) : NULL);
                    nsindex(db.c_str())->rollbackCreate();
                }
            }
        }

        // If a txn is completing, the cursors it created
        // must be killed before it can commit or abort.
        virtual void noteTxnCompletedCursors(const set<long long> &cursorIds) {
            for (set<long long>::const_iterator i = cursorIds.begin(); i != cursorIds.end(); ++i) {
                ClientCursor::erase(*i);
            }
        }

    } _txnCompleteHooks;

} // namespace mongo
