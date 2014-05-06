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

#include "mongo/db/cursor.h"
#include "mongo/db/collection.h"
#include "mongo/db/collection_map.h"
#include "mongo/db/json.h"
#include "mongo/db/relock.h"
#include "mongo/db/storage/dictionary.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/key.h"
#include "mongo/util/stringutils.h"

namespace mongo {

    CollectionMap::CollectionMap(const string &dir, const StringData& database) :
        _dir(dir),
        _metadname(database.toString() + ".ns"),
        _database(database.toString()),
        _openRWLock("nsOpenRWLock") {
    }

    CollectionMap::~CollectionMap() {
        for (CollectionStringMap::const_iterator it = _collections.begin(); it != _collections.end(); ++it) {
            shared_ptr<Collection> cl = it->second;
            try {
                cl->close();
            }
            catch (DBException &e) {
                // shouldn't throw in destructor
                msgasserted(16779, mongoutils::str::stream() << "caught exception while closing " << (string) it->first << " to close CollectionMap " << _database << ": " << e.what());
            }
        }
        if (_metadb != NULL) {
            TOKULOG(1) << "Closing CollectionMap " << _database << endl;
            const int r = _metadb->close();
            if (r != 0) {
                msgasserted(16920, mongoutils::str::stream() << "failed to close metadb for CollectionMap " << _database);
            }
        }
    }

    void CollectionMap::init(bool may_create) {
        Lock::assertAtLeastReadLocked(_database);
        if (!allocated()) {
            SimpleRWLock::Exclusive lk(_openRWLock);
            if (!allocated()) {
                _init(may_create);
            }
        }
    }

    NOINLINE_DECL void CollectionMap::_init(bool may_create) {
        const BSONObj keyPattern = BSON("ns" << 1 );
        const BSONObj info = BSON("key" << keyPattern);
        Descriptor descriptor(keyPattern);

        try {
            // Try first without the create flag, because we're not sure if we
            // have a write lock just yet. It won't matter if the metadb exists.
            _metadb.reset(new storage::Dictionary(_metadname, info, descriptor, false, false));
        } catch (storage::Dictionary::NeedsCreate) {
            if (!may_create) {
                // didn't find on disk and we can't create it
                return;
            } else if (!Lock::isWriteLocked(_database)) {
                // We would create it, but we're not write locked. Retry.
                throw RetryWithWriteLock("creating new database " + _database);
            }
            CollectionMapRollback &rollback = cc().txn().collectionMapRollback();
            rollback.noteCreate(_database);
            _metadb.reset(new storage::Dictionary(_metadname, info, descriptor, true, false));
        }
    }

    void CollectionMap::rollbackCreate() {
        if (!allocated()) {
            return;
        }
        // If we are rolling back the database creation, then any collections in that database were
        // created in this transaction.  Since we roll back collection creates before dictionary
        // creates, we would have already rolled back the collection creation, which does close_ns,
        // which removes the Collection from the map.  So this must be empty.
        verify(_collections.empty());

        // Closing the DB before the transaction aborts will allow the abort to do the dbremove for us.
        shared_ptr<storage::Dictionary> metadb = _metadb;
        _metadb.reset();
        const int r = metadb->close();
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
    }

    struct getNamespacesExtra : public ExceptionSaver {
        list<string> &tofill;
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
            return TOKUDB_CURSOR_CONTINUE;
        }
        catch (std::exception &ex) {
            e->saveException(ex);
            return -1;
        }
    }

    void CollectionMap::getNamespaces( list<string>& tofill ) {
        init();
        if (!allocated()) {
            return;
        }

        getNamespacesExtra extra(tofill);
        storage::Cursor c(_metadb->db());
        int r = 0;
        while (r != DB_NOTFOUND) {
            r = c.dbc()->c_getf_next(c.dbc(), 0, getNamespacesCallback, &extra);
            if (r == -1) {
                extra.throwException();
                msgasserted(17322, "got -1 from cursor iteration but didn't save an exception");
            }
            if (r != 0 && r != DB_NOTFOUND) {
                storage::handle_ydb_error(r);
            }
        }
    }

    void CollectionMap::kill_ns(const StringData& ns) {
        init();
        if (!allocated()) { // that's ok, may dropping something that doesn't exist
            return;
        }
        if (!Lock::isWriteLocked(ns)) {
            throw RetryWithWriteLock();
        }

        // Must copy ns before we delete the collection, otherwise ns will be pointing to freed
        // memory.
        BSONObj nsobj = BSON("ns" << ns);

        CollectionStringMap::const_iterator it = _collections.find(ns);
        if (it != _collections.end()) {
            // Might not be in the _collections map if the ns exists but is closed.
            // Note this ns in the rollback, since we are about to modify its entry.
            CollectionMapRollback &rollback = cc().txn().collectionMapRollback();
            rollback.noteNs(ns);
            shared_ptr<Collection> cl = it->second;
            const int r = _collections.erase(ns);
            verify(r == 1);
            cl->close();
        }

        storage::Key sKey(nsobj, NULL);
        DBT ndbt = sKey.dbt();
        DB *db = _metadb->db();
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
    // also, on input, the CollectionMap must be allocated
    Collection *CollectionMap::open_ns(const StringData& ns, const bool bulkLoad) {
        verify(allocated());
        BSONObj serialized;
        BSONObj nsobj = BSON("ns" << ns);
        storage::Key sKey(nsobj, NULL);
        DBT ndbt = sKey.dbt();

        // If this transaction is read only, then we cannot possible already
        // hold a lock in the metadb and we certainly don't need to hold one
        // for the duration of this operation. So we use an alternate txn stack.
        const bool needAltTxn = !cc().hasTxn() || cc().txn().readOnly();
        scoped_ptr<Client::AlternateTransactionStack> altStack(!needAltTxn ? NULL :
                                                               new Client::AlternateTransactionStack());
        scoped_ptr<Client::Transaction> altTxn(!needAltTxn ? NULL :
                                               new Client::Transaction(0));

        // Pass flags that get us a write lock on the metadb row
        // for the ns we'd like to open.
        DB *db = _metadb->db();
        const int r = db->getf_set(db, cc().txn().db_txn(), DB_SERIALIZABLE | DB_RMW,
                                   &ndbt, getf_serialized, &serialized);
        if (r == 0) {
            // We found an entry for this ns and we have the row lock.
            // First check if someone got the lock before us and already
            // did the open.
            Collection *cl = find_ns(ns);
            if (cl != NULL) {
                return cl;
            }
            // No need to hold the openRWLock during Collection::make(),
            // the fact that we have the row lock ensures only one thread will
            // be here for a particular ns at a time.
            shared_ptr<Collection> details = Collection::make( serialized, bulkLoad );
            SimpleRWLock::Exclusive lk(_openRWLock);
            verify(!_collections[ns]);
            _collections[ns] = details;
            return details.get();
        } else if (r != DB_NOTFOUND) {
            storage::handle_ydb_error(r);
        }
        return NULL;
    }

    bool CollectionMap::close_ns(const StringData& ns, const bool aborting) {
        Lock::assertWriteLocked(ns);
        // No need to initialize first. If the metadb is null at this point,
        // we simply say that the ns you want to close wasn't open.
        if (!allocated()) {
            return false;
        }

        // Find and erase the old entry, if it exists.
        CollectionStringMap::const_iterator it = _collections.find(ns);
        if (it != _collections.end()) {
            // TODO: Handle the case where a client tries to close a load they didn't start.
            shared_ptr<Collection> cl = it->second;
            _collections.erase(ns);
            cl->close(aborting);
            return true;
        }
        return false;
    }

    void CollectionMap::add_ns(const StringData& ns, shared_ptr<Collection> cl) {
        if (!Lock::isWriteLocked(ns)) {
            throw RetryWithWriteLock();
        }

        init();
        dassert(allocated()); // cannot add to a non-existent metadb

        // Note this ns in the rollback, since we are about to modify its entry.
        CollectionMapRollback &rollback = cc().txn().collectionMapRollback();
        rollback.noteNs(ns);

        verify(!_collections[ns]);
        _collections[ns] = cl;
    }

    void CollectionMap::update_ns(const StringData& ns, const BSONObj &serialized, bool overwrite) {
        if (!Lock::isWriteLocked(ns)) {
            throw RetryWithWriteLock();
        }

        init();
        dassert(allocated()); // cannot update a non-existent metadb

        // Note this ns in the rollback, even though we aren't modifying
        // _collections directly. But we know this operation is part of
        // a scheme to create this namespace or change something about it.
        CollectionMapRollback &rollback = cc().txn().collectionMapRollback();
        rollback.noteNs(ns);

        BSONObj nsobj = BSON("ns" << ns);
        storage::Key sKey(nsobj, NULL);
        DBT ndbt = sKey.dbt();
        DBT ddbt = storage::dbt_make(serialized.objdata(), serialized.objsize());
        DB *db = _metadb->db();
        const int flags = overwrite ? 0 : DB_NOOVERWRITE;
        const int r = db->put(db, cc().txn().db_txn(), &ndbt, &ddbt, flags);
        if (r != 0) {
            storage::handle_ydb_error_fatal(r);
        }
    }

    Collection *CollectionMap::find_ns(const StringData& ns) {
        init();
        if (!allocated()) {
            return NULL;
        }

        SimpleRWLock::Shared lk(_openRWLock);
        return find_ns_locked(ns);
    }

    Collection *CollectionMap::getCollection(const StringData &ns) {
        init();
        if (!allocated()) {
            return NULL;
        }

        Collection *cl = NULL;
        {
            // Try to find the ns in a shared lock. If it's there, we're done.
            SimpleRWLock::Shared lk(_openRWLock);
            cl = find_ns_locked(ns);
        }

        if (cl == NULL) {
            // The ns doesn't exist, or it's not opened.
            cl = open_ns(ns);
        }

        // Possibly validate the connection if the collection
        // is under-going bulk load.
        if (cl != NULL && cl->bulkLoading()) {
            BulkLoadedCollection *bulkCl = cl->as<BulkLoadedCollection>();
            bulkCl->validateConnectionId(cc().getConnectionId());
        }
        return cl;
    }

    void CollectionMap::drop() {
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
        Collection *sysCl = getCollection(systemNamespacesNs);
        for (shared_ptr<Cursor> c(Cursor::make(sysCl)); c->ok(); c->advance()) {
            const BSONObj nsObj = c->current();
            const StringData ns = nsObj["name"].Stringdata();
            if (nsToDatabaseSubstring(ns) != _database) {
                // Not part of this database, skip.
                continue;
            }
            if (nsToCollectionSubstring(ns) == "system.indexes") {
                // Save .system.indexes collection for last, because drop() deletes from it.
                sysIndexesEntries.push_back(ns.toString());
            } else {
                Collection *cl = getCollection(ns);
                if (cl != NULL) {
                    cl->drop(errmsg, result, true);
                }
            }
        }
        if (sysCl != NULL) {
            // The .system.namespaces collection does not include itself.
            sysCl->drop(errmsg, result, true);
        }
        // Now drop the system.indexes entries.
        for (vector<string>::const_iterator it = sysIndexesEntries.begin(); it != sysIndexesEntries.end(); it++) {
            // Need to close any existing handle before drop.
            Collection *cl = getCollection(*it);
            if (cl != NULL) {
                cl->drop(errmsg, result, true);
            }
        }
        // Everything that was open should have been closed due to drop.
        verify(_collections.empty());

        shared_ptr<storage::Dictionary> metadb = _metadb;
        _metadb.reset();
        const int r = metadb->close();
        if (r != 0) {
            storage::handle_ydb_error(r);
        }
        storage::db_remove(_metadname);
    }

} // namespace mongo
