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

#include "mongo/pch.h"

#include "mongo/db/namespace_details.h"
#include "mongo/db/json.h"
#include "mongo/db/relock.h"
#include "mongo/db/storage/key.h"
#include "mongo/db/storage/env.h"

namespace mongo {

    NamespaceIndex::NamespaceIndex(const string &dir, const string &database) :
        _nsdb(NULL), _namespaces(NULL),
        _dir(dir), _database(database), _openMutex("nsOpenMutex") {
    }

    NamespaceIndex::~NamespaceIndex() {
        if (_nsdb != NULL) {
            TOKULOG(1) << "Closing NamespaceIndex " << _database << endl;
            storage::db_close(_nsdb);
        }
    }

    void NamespaceIndex::init(bool may_create) {
        Lock::assertAtLeastReadLocked(_database);
        if (!allocated()) {
            SimpleMutex::scoped_lock lk(_openMutex);
            if (!allocated()) {
                _init(may_create);
            }
            verify((_namespaces.get() != NULL) == (_nsdb != NULL));
        }
    }

    NOINLINE_DECL void NamespaceIndex::_init(bool may_create) {
        string nsdbname(_database + ".ns");
        BSONObj info = BSON("key" << fromjson("{\"ns\":1}" ));

        // Try first without the create flag, because we're not sure if we
        // have a write lock just yet. It won't matter if the nsdb exists.
        int r = storage::db_open(&_nsdb, nsdbname, info, false);
        if (r == ENOENT) {
            if (!may_create) {
                // didn't find on disk and we can't create it
                return;
            } else if (!Lock::isWriteLocked(_database)) {
                // We would create it, but we're not write locked. Retry.
                throw RetryWithWriteLock();
            }
            // Try opening again with may_create = true
            r = storage::db_open(&_nsdb, nsdbname, info, true);
        } else if (r != 0) {
            storage::handle_ydb_error_fatal(r);
        }

        // Start with an empty map. Existing namespaces are opened lazily.
        _namespaces.reset(new NamespaceDetailsMap());
        verify(_nsdb != NULL);
    }

    void NamespaceIndex::getNamespaces( list<string>& tofill ) {
        init();
        if (!allocated()) {
            return;
        }

        for (NamespaceDetailsMap::const_iterator it = _namespaces->begin(); it != _namespaces->end(); it++) {
            const Namespace &n = it->first;
            tofill.push_back((string) n);
        }
    }

    void NamespaceIndex::kill_ns(const char *ns) {
        init();
        if (!allocated()) { // that's ok, may dropping something that doesn't exist
            return;
        }
        if (!Lock::isWriteLocked(ns)) {
            throw RetryWithWriteLock();
        }

        Namespace n(ns);
        NamespaceDetailsMap::iterator it = _namespaces->find(n);
        if (it != _namespaces->end()) {
            // Might not be in the _namespaces map if the ns exists but is closed.
            // Note this ns in the rollback, since we are about to modify its entry.
            NamespaceIndexRollback &rollback = cc().txn().nsIndexRollback();
            rollback.noteNs(ns);
            _namespaces->erase(it);
        }

        BSONObj nsobj = BSON("ns" << ns);
        storage::Key sKey(nsobj, NULL);
        DBT ndbt = sKey.dbt();
        int r = _nsdb->del(_nsdb, cc().txn().db_txn(), &ndbt, 0);
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

    bool NamespaceIndex::open_ns(const char *ns) {
        init();
        dassert(allocated()); // shouldn't try to open from an non-existent nsdb

        Namespace n(ns);
        BSONObj serialized;
        BSONObj nsobj = BSON("ns" << ns);
        storage::Key sKey(nsobj, NULL);
        DBT ndbt = sKey.dbt();
        DB_TXN *db_txn = cc().hasTxn() ? cc().txn().db_txn() : NULL;
        int r = _nsdb->getf_set(_nsdb, db_txn, 0, &ndbt, getf_serialized, &serialized);
        if (r == 0) {
            if (!Lock::isWriteLocked(ns)) {
                // Something exists in the nsdb for this ns, but we're not write locked.
                TOKULOG(1) << "Tried to open ns << " << ns << ", but wasn't write locked." << endl;
                throw RetryWithWriteLock();
            }

            shared_ptr<NamespaceDetails> details = NamespaceDetails::make( serialized );
            std::pair<NamespaceDetailsMap::iterator, bool> ret;
            ret = _namespaces->insert(make_pair(n, details));
            dassert(ret.second == true);
        } else if (r != DB_NOTFOUND) {
            storage::handle_ydb_error(r);
        }
        return r == 0;
    }

    bool NamespaceIndex::close_ns(const char *ns) {
        // No need to initialize first. If the nsdb is null at this point,
        // we simply say that the ns you want to close wasn't open.
        if (!allocated()) {
            return false;
        }

        // Find and erase the old entry, if it exists.
        Namespace n(ns);
        NamespaceDetailsMap::iterator it = _namespaces->find(n);
        if (it != _namespaces->end()) {
            if (!Lock::isWriteLocked(ns)) {
                throw RetryWithWriteLock();
            }
            _namespaces->erase(it);
            return true;
        }
        return false;
    }

    void NamespaceIndex::add_ns(const char *ns, shared_ptr<NamespaceDetails> details) {
        if (!Lock::isWriteLocked(ns)) {
            throw RetryWithWriteLock();
        }

        init();
        dassert(allocated()); // cannot add to a non-existent nsdb

        // Note this ns in the rollback, since we are about to modify its entry.
        NamespaceIndexRollback &rollback = cc().txn().nsIndexRollback();
        rollback.noteNs(ns);

        Namespace n(ns);
        std::pair<NamespaceDetailsMap::iterator, bool> ret;
        ret = _namespaces->insert(make_pair(n, details));
        dassert(ret.second == true);
    }

    void NamespaceIndex::update_ns(const char *ns, const BSONObj &serialized, bool overwrite) {
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
        DBT ddbt = storage::make_dbt(serialized.objdata(), serialized.objsize());
        const int flags = overwrite ? 0 : DB_NOOVERWRITE;
        int r = _nsdb->put(_nsdb, cc().txn().db_txn(), &ndbt, &ddbt, flags);
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
        NamespaceDetails *sysNsd = nsdetails((_database + ".system.namespaces").c_str());
        for (scoped_ptr<Cursor> c(BasicCursor::make(sysNsd)); c->ok(); c->advance()) {
            const BSONObj nsObj = c->current();
            const string ns = nsObj["name"].String();
            if (!str::startsWith(ns, _database)) {
                // Not part of this database, skip.
                continue;
            }
            if (str::contains(ns, ".system.indexes")) {
                // Save .system.indexes collection for last, because dropCollection deletes from it.
                sysIndexesEntries.push_back(ns);
            } else {
                dropCollection(ns, errmsg, result, true);
            }
        }
        if (sysNsd != NULL) {
            // The .system.namespaces collection does not include itself.
            const string ns(_database + ".system.namespaces");
            dropCollection(ns, errmsg, result, true);
        }
        // Now drop the system.indexes entries.
        for (vector<string>::const_iterator it = sysIndexesEntries.begin(); it != sysIndexesEntries.end(); it++) {
            // Need to close any existing handle before drop.
            const string &ns = *it;
            dropCollection(ns, errmsg, result, true);
        }
        // Everything that was open should have been closed due to drop.
        verify(_namespaces->empty());

        DB *db = _nsdb;
        // nsdb and namespaces must be mutually null
        _nsdb = NULL;
        _namespaces.reset();
        storage::db_close(db);
        storage::db_remove(_database + ".ns");
    }

} // namespace mongo

