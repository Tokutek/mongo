/**
*    Copyright (C) 2008 10gen Inc.
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

#include "mongo/db/namespace_details.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/json.h"
#include "mongo/db/relock.h"
#include "mongo/db/storage/dictionary.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/key.h"

#include "mongo/util/stringutils.h"

namespace mongo {

    NamespaceIndex::NamespaceIndex(const string &dir, const StringData& database) :
            _dir(dir),
            _nsdbFilename(database.toString() + ".ns"),
            _database(database.toString()),
            _openRWLock("nsOpenRWLock")
    {}

    NamespaceIndex::~NamespaceIndex() {
        for (NamespaceDetailsMap::const_iterator it = _namespaces.begin(); it != _namespaces.end(); ++it) {
            shared_ptr<NamespaceDetails> d = it->second;
            try {
                d->close();
            }
            catch (DBException &e) {
                // shouldn't throw in destructor
                msgasserted(16779, mongoutils::str::stream() << "caught exception while closing " << (string) it->first << " to close NamespaceIndex " << _database << ": " << e.what());
            }
        }
        if (_nsdb != NULL) {
            TOKULOG(1) << "Closing NamespaceIndex " << _database << endl;
            const int r = _nsdb->close();
            if (r != 0) {
                msgasserted(16920, mongoutils::str::stream() << "failed to close nsdb for NamespaceIndex " << _database);
            }
        }
    }

    void NamespaceIndex::init(bool may_create) {
        Lock::assertAtLeastReadLocked(_database);
        if (!allocated()) {
            SimpleRWLock::Exclusive lk(_openRWLock);
            if (!allocated()) {
                _init(may_create);
            }
        }
    }

    NOINLINE_DECL void NamespaceIndex::_init(bool may_create) {
        const BSONObj keyPattern = BSON("ns" << 1 );
        const BSONObj info = BSON("key" << keyPattern);
        Descriptor descriptor(keyPattern);

        try {
            // Try first without the create flag, because we're not sure if we
            // have a write lock just yet. It won't matter if the nsdb exists.
            _nsdb.reset(new storage::Dictionary(_nsdbFilename, info, descriptor, false, false));
        } catch (storage::Dictionary::NeedsCreate) {
            if (!may_create) {
                // didn't find on disk and we can't create it
                return;
            } else if (!Lock::isWriteLocked(_database)) {
                // We would create it, but we're not write locked. Retry.
                throw RetryWithWriteLock("creating new database " + _database);
            }
            NamespaceIndexRollback &rollback = cc().txn().nsIndexRollback();
            rollback.noteCreate(_database);
            _nsdb.reset(new storage::Dictionary(_nsdbFilename, info, descriptor, true, false));
        }
    }

    void NamespaceIndex::rollbackCreate() {
        if (!allocated()) {
            return;
        }
        // If we are rolling back the database creation, then any collections in that database were
        // created in this transaction.  Since we roll back collection creates before dictionary
        // creates, we would have already rolled back the collection creation, which does close_ns,
        // which removes the NamespaceDetails from the map.  So this must be empty.
        verify(_namespaces.empty());

        // Closing the DB before the transaction aborts will allow the abort to do the dbremove for us.
        shared_ptr<storage::Dictionary> nsdb = _nsdb;
        _nsdb.reset();
        const int r = nsdb->close();
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

    struct getNamespacesExtra {
        list<string> &tofill;
        std::exception *ex;
        getNamespacesExtra(list<string> &l) : tofill(l) {}
    };

    static int getNamespacesCallback(const DBT *key, const DBT *val, void *extra) {
        getNamespacesExtra *e = static_cast<getNamespacesExtra *>(extra);
        try {
            if (key != NULL) {
                verify(val != NULL);
                const BSONObj obj(static_cast<char *>(val->data));
                const string &ns = obj["ns"].String();
                e->tofill.push_back(ns);
            }
            return 0;
        }
        catch (std::exception &ex) {
            // Can't throw back through the ydb, so return -1 here and we'll throw on the other side.
            e->ex = &ex;
            return -1;
        }
    }

    void NamespaceIndex::getNamespaces( list<string>& tofill ) {
        init();
        if (!allocated()) {
            return;
        }

        getNamespacesExtra extra(tofill);
        storage::Cursor c(_nsdb->db());
        int r = 0;
        while (r != DB_NOTFOUND) {
            r = c.dbc()->c_getf_next(c.dbc(), 0, getNamespacesCallback, &extra);
            if (r == -1) {
                verify(extra.ex != NULL);
                throw *extra.ex;
            }
            if (r != 0 && r != DB_NOTFOUND) {
                storage::handle_ydb_error(r);
            }
        }
    }

    void NamespaceIndex::kill_ns(const StringData& ns) {
        init();
        if (!allocated()) { // that's ok, may dropping something that doesn't exist
            return;
        }
        if (!Lock::isWriteLocked(ns)) {
            throw RetryWithWriteLock();
        }

        NamespaceDetailsMap::const_iterator it = _namespaces.find(ns);
        if (it != _namespaces.end()) {
            // Might not be in the _namespaces map if the ns exists but is closed.
            // Note this ns in the rollback, since we are about to modify its entry.
            NamespaceIndexRollback &rollback = cc().txn().nsIndexRollback();
            rollback.noteNs(ns);
            shared_ptr<NamespaceDetails> d = it->second;
            const int r = _namespaces.erase(ns);
            verify(r == 1);
            d->close();
        }

        BSONObj nsobj = BSON("ns" << ns);
        storage::Key sKey(nsobj, NULL);
        DBT ndbt = sKey.dbt();
        DB *db = _nsdb->db();
        int r = db->del(db, cc().txn().db_txn(), &ndbt, 0);
        if (r != 0) {
            storage::handle_ydb_error_fatal(r);
        }
    }

    static int getf_serialized(const DBT *key, const DBT *val, void *extra) {
        BSONObj *serialized = reinterpret_cast<BSONObj *>(extra);
        if (key != NULL) {
            verify(val != NULL);
            BSONObj obj(static_cast<char *>(val->data));
            *serialized = obj.copy();
        }
        return 0;
    }

    // on input, _initLock is held, so this can be called by only one thread at a time,
    // also, on input, the NamespaceIndex must be allocated
    NamespaceDetails *NamespaceIndex::open_ns(const StringData& ns, const bool bulkLoad) {
        verify(allocated());
        BSONObj serialized;
        BSONObj nsobj = BSON("ns" << ns);
        storage::Key sKey(nsobj, NULL);
        DBT ndbt = sKey.dbt();

        // If this transaction is read only, then we cannot possible already
        // hold a lock in the nsindex and we certainly don't need to hold one
        // for the duration of this operation. So we use an alternate txn stack.
        const bool needAltTxn = !cc().hasTxn() || cc().txn().readOnly();
        scoped_ptr<Client::AlternateTransactionStack> altStack(!needAltTxn ? NULL :
                                                               new Client::AlternateTransactionStack());
        scoped_ptr<Client::Transaction> altTxn(!needAltTxn ? NULL :
                                               new Client::Transaction(0));

        // Pass flags that get us a write lock on the nsindex row
        // for the ns we'd like to open.
        DB *db = _nsdb->db();
        const int r = db->getf_set(db, cc().txn().db_txn(), DB_SERIALIZABLE | DB_RMW,
                                   &ndbt, getf_serialized, &serialized);
        if (r == 0) {
            // We found an entry for this ns and we have the row lock.
            // First check if someone got the lock before us and already
            // did the open.
            NamespaceDetails *d = find_ns(ns);
            if (d != NULL) {
                return d;
            }
            // No need to hold the openRWLock during NamespaceDetails::make(),
            // the fact that we have the row lock ensures only one thread will
            // be here for a particular ns at a time.
            shared_ptr<NamespaceDetails> details = NamespaceDetails::make( serialized, bulkLoad );
            SimpleRWLock::Exclusive lk(_openRWLock);
            verify(!_namespaces[ns]);
            _namespaces[ns] = details;
            return details.get();
        } else if (r != DB_NOTFOUND) {
            storage::handle_ydb_error(r);
        }
        return NULL;
    }

    bool NamespaceIndex::close_ns(const StringData& ns, const bool aborting) {
        Lock::assertWriteLocked(ns);
        // No need to initialize first. If the nsdb is null at this point,
        // we simply say that the ns you want to close wasn't open.
        if (!allocated()) {
            return false;
        }

        // Find and erase the old entry, if it exists.
        NamespaceDetailsMap::const_iterator it = _namespaces.find(ns);
        if (it != _namespaces.end()) {
            // TODO: Handle the case where a client tries to close a load they didn't start.
            shared_ptr<NamespaceDetails> d = it->second;
            _namespaces.erase(ns);
            d->close(aborting);
            return true;
        }
        return false;
    }

    void NamespaceIndex::add_ns(const StringData& ns, shared_ptr<NamespaceDetails> details) {
        if (!Lock::isWriteLocked(ns)) {
            throw RetryWithWriteLock();
        }

        init();
        dassert(allocated()); // cannot add to a non-existent nsdb

        // Note this ns in the rollback, since we are about to modify its entry.
        NamespaceIndexRollback &rollback = cc().txn().nsIndexRollback();
        rollback.noteNs(ns);

        verify(!_namespaces[ns]);
        _namespaces[ns] = details;
    }

    void NamespaceIndex::update_ns(const StringData& ns, const BSONObj &serialized, bool overwrite) {
        if (!Lock::isWriteLocked(ns)) {
            throw RetryWithWriteLock();
        }

        init();
        dassert(allocated()); // cannot update a non-existent nsdb

        // Note this ns in the rollback, even though we aren't modifying
        // _namespaces directly. But we know this operation is part of
        // a scheme to create this namespace or change something about it.
        NamespaceIndexRollback &rollback = cc().txn().nsIndexRollback();
        rollback.noteNs(ns);

        BSONObj nsobj = BSON("ns" << ns);
        storage::Key sKey(nsobj, NULL);
        DBT ndbt = sKey.dbt();
        DBT ddbt = storage::dbt_make(serialized.objdata(), serialized.objsize());
        DB *db = _nsdb->db();
        const int flags = overwrite ? 0 : DB_NOOVERWRITE;
        const int r = db->put(db, cc().txn().db_txn(), &ndbt, &ddbt, flags);
        if (r != 0) {
            storage::handle_ydb_error_fatal(r);
        }
    }

    void NamespaceIndex::drop() {
        Lock::assertWriteLocked(_database);
        init();
        if (!allocated()) {
            return;
        }

        string errmsg;
        BSONObjBuilder result;

        // This implementation is inefficient and slightly messy, but it was easy.
        // Feel free to improve it as necessary:
        // - The getCursor call will grab a table lock on .system.namespaces.
        // - We'll look at the entire system.namespaces collection just for one database.
        // - Code is duplicated to handle dropping system system collections in stages.
        vector<string> sysIndexesEntries;
        const string systemNamespacesNs = getSisterNS(_database, "system.namespaces");
        NamespaceDetails *sysNsd = nsdetails(systemNamespacesNs);
        for (shared_ptr<Cursor> c(BasicCursor::make(sysNsd)); c->ok(); c->advance()) {
            const BSONObj nsObj = c->current();
            const StringData ns = nsObj["name"].Stringdata();
            if (nsToDatabaseSubstring(ns) != _database) {
                // Not part of this database, skip.
                continue;
            }
            if (nsToCollectionSubstring(ns) == "system.indexes") {
                // Save .system.indexes collection for last, because dropCollection deletes from it.
                sysIndexesEntries.push_back(ns.toString());
            } else {
                dropCollection(ns, errmsg, result, true);
            }
        }
        if (sysNsd != NULL) {
            // The .system.namespaces collection does not include itself.
            dropCollection(systemNamespacesNs, errmsg, result, true);
        }
        // Now drop the system.indexes entries.
        for (vector<string>::const_iterator it = sysIndexesEntries.begin(); it != sysIndexesEntries.end(); it++) {
            // Need to close any existing handle before drop.
            const string &ns = *it;
            dropCollection(ns, errmsg, result, true);
        }
        // Everything that was open should have been closed due to drop.
        verify(_namespaces.empty());

        shared_ptr<storage::Dictionary> nsdb = _nsdb;
        _nsdb.reset();
        const int r = nsdb->close();
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        storage::db_remove(_nsdbFilename);
    }

} // namespace mongo
