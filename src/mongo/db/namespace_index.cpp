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
        nsdb(NULL), namespaces(NULL), dir_(dir), database_(database) {
    }

    NamespaceIndex::~NamespaceIndex() {
        if (nsdb != NULL) {
            TOKULOG(1) << "Closing NamespaceIndex " << database_ << endl;
            storage::db_close(nsdb);
            dassert(namespaces.get() != NULL);
        }
    }

    void NamespaceIndex::init(bool may_create) {
        if (namespaces.get() == NULL) {
            _init(may_create);
        }
    }

    static int populateNamespaceIndex(const DBT *key, const DBT *val, void *extra) {
        NamespaceIndex::PopulateExtra *e = static_cast<NamespaceIndex::PopulateExtra *>(extra);
        try {
            const storage::Key sKey(key);
            string ns = sKey.key().firstElement().String();
            Namespace n(ns.c_str());
            BSONObj dobj(static_cast<const char *>(val->data));
            TOKULOG(1) << "Loading NamespaceDetails " << (string) n << endl;
            shared_ptr<NamespaceDetails> d( NamespaceDetails::make(dobj) );

            std::pair<NamespaceIndex::NamespaceDetailsMap::iterator, bool> ret;
            ret = e->map.insert(make_pair(n, d));
            dassert(ret.second == true);
            return 0;
        }
        catch (std::exception &exc) {
            // We're not allowed to throw exceptions through the ydb so trap it here and reclaim it on the other side.
            e->exc = &exc;
            return -1;
        }
    }

    NOINLINE_DECL void NamespaceIndex::_init(bool may_create) {
        int r;

        if (!Lock::isWriteLocked(database_)) {
            throw RetryWithWriteLock();
        }
        verify(namespaces.get() == NULL);
        dassert(nsdb == NULL);

        string nsdbname(database_ + ".ns");
        r = storage::db_open(&nsdb, nsdbname, BSON("key" << fromjson("{\"ns\":1}" )), may_create);
        if (r == ENOENT) {
            // didn't find on disk
            dassert(!may_create);
            return;
        } else if (r != 0) {
            storage::handle_ydb_error_fatal(r);
        }

        namespaces.reset(new NamespaceDetailsMap());

        TOKULOG(1) << "Initializing NamespaceIndex " << database_ << endl;
        scoped_ptr<Client::Transaction> txnp(cc().hasTxn()
                                             ? NULL
                                             : new Client::Transaction(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY));
        {
            storage::Cursor c(nsdb, 0);
            DBC *cursor = c.dbc();
            while (r != DB_NOTFOUND) {
                PopulateExtra e(*namespaces);
                r = cursor->c_getf_next(cursor, 0, populateNamespaceIndex, &e);
                if (r == -1) {
                    dassert(e.exc != NULL);
                    throw *e.exc;
                }
                if (r != 0 && r != DB_NOTFOUND) {
                    storage::handle_ydb_error_fatal(r);
                }
            }
        }

        if (txnp.get()) {
            txnp->commit(0);
        }
    }

    void NamespaceIndex::getNamespaces( list<string>& tofill ) const {
        if (namespaces.get() != NULL) {
            for (NamespaceDetailsMap::const_iterator it = namespaces->begin(); it != namespaces->end(); it++) {
                const Namespace &n = it->first;
                tofill.push_back((string) n);
            }
        }
    }

    void NamespaceIndex::kill_ns(const char *ns) {
        if (!Lock::isWriteLocked(ns)) {
            throw RetryWithWriteLock();
        }
        if (namespaces.get() == NULL) {
            return;
        }
        Namespace n(ns);
        NamespaceDetailsMap::iterator it = namespaces->find(n);
        verify(it != namespaces->end());

        BSONObj nsobj = BSON("ns" << ns);
        storage::Key sKey(nsobj, NULL);
        DBT ndbt = sKey.dbt();
        int r = nsdb->del(nsdb, cc().txn().db_txn(), &ndbt, DB_DELETE_ANY);
        if (r != 0) {
            storage::handle_ydb_error_fatal(r);
        }

        // Should really only do this after the commit of the del.
        namespaces->erase(it);
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

    void NamespaceIndex::open_ns(const char *ns) {
        if (!Lock::isWriteLocked(ns)) {
            throw RetryWithWriteLock();
        }

        init();
        Namespace n(ns);
        BSONObj serialized;

        BSONObj nsobj = BSON("ns" << ns);
        storage::Key sKey(nsobj, NULL);
        DBT ndbt = sKey.dbt();
        int r = nsdb->getf_set(nsdb, NULL, 0, &ndbt, getf_serialized, &serialized);
        if (r != 0) {
            storage::handle_ydb_error_fatal(r);
        }

        shared_ptr<NamespaceDetails> details = NamespaceDetails::make( serialized );
        std::pair<NamespaceDetailsMap::iterator, bool> ret;
        ret = namespaces->insert(make_pair(n, details));
        dassert(ret.second == true);
    }

    void NamespaceIndex::close_ns(const char *ns) {
        if (!Lock::isWriteLocked(ns)) {
            throw RetryWithWriteLock();
        }

        init();
        Namespace n(ns);

        // Find and erase the old entry.
        NamespaceDetailsMap::iterator it = namespaces->find(n);
        verify(it != namespaces->end());
        namespaces->erase(it);

        // Insert it as NULL, marking it as existing but closed.
        std::pair<NamespaceDetailsMap::iterator, bool> ret;
        ret = namespaces->insert(make_pair(n, shared_ptr<NamespaceDetails>()));
        dassert(ret.second == true);
    }

    void NamespaceIndex::add_ns(const char *ns, shared_ptr<NamespaceDetails> details) {
        if (!Lock::isWriteLocked(ns)) {
            throw RetryWithWriteLock();
        }

        init();
        Namespace n(ns);

        std::pair<NamespaceDetailsMap::iterator, bool> ret;
        ret = namespaces->insert(make_pair(n, details));
        dassert(ret.second == true);
    }

    void NamespaceIndex::update_ns(const char *ns, const BSONObj &serialized, bool overwrite) {
        if (!Lock::isWriteLocked(ns)) {
            throw RetryWithWriteLock();
        }
        dassert(namespaces.get() != NULL);

        BSONObj nsobj = BSON("ns" << ns);
        storage::Key sKey(nsobj, NULL);
        DBT ndbt = sKey.dbt();
        DBT ddbt = storage::make_dbt(serialized.objdata(), serialized.objsize());
        const int flags = overwrite ? 0 : DB_NOOVERWRITE;
        int r = nsdb->put(nsdb, cc().txn().db_txn(), &ndbt, &ddbt, flags);
        if (r != 0) {
            storage::handle_ydb_error_fatal(r);
        }
    }

    void NamespaceIndex::drop() {
        if (!allocated()) {
            return;
        }
        string errmsg;
        BSONObjBuilder result;
        // Save .system.indexes collection for last, because dropCollection tries to delete from it.
        // It has itself and its _id index in the namespaces vector, so it is responsible for two entries.
        // Leif can't prove that it will always be exactly 2, so we do this in a slightly more careful, but more robust, way.
        while (!namespaces->empty()) {
            NamespaceDetailsMap::iterator it = namespaces->begin();
            while (it != namespaces->end() && mongoutils::str::contains(it->first, ".system.indexes")) {
                // Skip anything that contains system.indexes for now.
                it++;
            }
            if (it == namespaces->end()) {
                // If we hit the end, we can start dropping from the beginning.
                it = namespaces->begin();
            }
            const Namespace &ns = it->first;
            dropCollection((string) ns, errmsg, result, true);
        }

        dassert(nsdb != NULL);
        storage::db_close(nsdb);
        nsdb = NULL;
        storage::db_remove(database_ + ".ns");
    }

} // namespace mongo

