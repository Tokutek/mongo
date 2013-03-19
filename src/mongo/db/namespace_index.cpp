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
#include "mongo/db/storage/env.h"

namespace mongo {

    NamespaceIndex::NamespaceIndex(const string &dir, const string &database) :
        nsdb(NULL), namespaces(NULL), dir_(dir), database_(database) {
    }

    NamespaceIndex::~NamespaceIndex() {
        if (nsdb != NULL) {
            tokulog(1) << "Closing NamespaceIndex " << database_ << endl;
            storage::db_close(nsdb);
            dassert(namespaces.get() != NULL);
        }
    }

    void NamespaceIndex::init(bool may_create) {
        if (namespaces.get() == NULL) {
            _init(may_create);
        }
    }

    static int populate_nsindex_map(const DBT *key, const DBT *val, void *map_v) {
        BSONObj nobj(static_cast<const char *>(key->data));
        string ns = nobj["ns"].String();
        Namespace n(ns.c_str());
        BSONObj dobj(static_cast<const char *>(val->data));
        tokulog(1) << "Loading NamespaceDetails " << (string) n << endl;
        shared_ptr<NamespaceDetails> d( NamespaceDetails::make(dobj) );

        NamespaceIndex::NamespaceDetailsMap *m = static_cast<NamespaceIndex::NamespaceDetailsMap *>(map_v);
        std::pair<NamespaceIndex::NamespaceDetailsMap::iterator, bool> ret;
        ret = m->insert(make_pair(n, d));
        dassert(ret.second == true);
        return 0;
    }

    NOINLINE_DECL void NamespaceIndex::_init(bool may_create) {
        int r;

        Lock::assertWriteLocked(database_);
        verify(namespaces.get() == NULL);
        dassert(nsdb == NULL);

        string nsdbname(database_ + ".ns");
        r = storage::db_open(&nsdb, nsdbname, BSON("key" << fromjson("{\"ns\":1}" )), may_create);
        if (r == ENOENT) {
            // didn't find on disk
            dassert(!may_create);
            return;
        }
        verify(r == 0);

        namespaces.reset(new NamespaceDetailsMap());

        tokulog(1) << "Initializing NamespaceIndex " << database_ << endl;
        {
            scoped_ptr<Client::Transaction> txnp(cc().hasTxn()
                                                 ? NULL
                                                 : new Client::Transaction(DB_TXN_SNAPSHOT | DB_TXN_READ_ONLY));
            DBC *cursor;
            r = nsdb->cursor(nsdb, cc().txn().db_txn(), &cursor, 0);
            verify(r == 0);

            while (r != DB_NOTFOUND) {
                r = cursor->c_getf_next(cursor, 0, populate_nsindex_map, namespaces.get());
                verify(r == 0 || r == DB_NOTFOUND);
            }

            r = cursor->c_close(cursor);
            verify(r == 0);
            if (txnp.get()) {
                txnp->commit(0);
            }
        }

        /* if someone manually deleted the datafiles for a database,
           we need to be sure to clear any cached info for the database in
           local.*.
        */
        /*
        if ( "local" != database_ ) {
            DBInfo i(database_.c_str());
            i.dbDropped();
        }
        */
    }

    void NamespaceIndex::getNamespaces( list<string>& tofill , bool onlyCollections ) const {
        verify( onlyCollections ); // TODO: need to implement this
        //                                  need boost::bind or something to make this less ugly

        if (namespaces.get() != NULL) {
            for (NamespaceDetailsMap::const_iterator it = namespaces->begin(); it != namespaces->end(); it++) {
                const Namespace &n = it->first;
                tofill.push_back((string) n);
            }
        }
    }

    void NamespaceIndex::kill_ns(const char *ns) {
        Lock::assertWriteLocked(ns);
        if (namespaces.get() == NULL) {
            return;
        }
        Namespace n(ns);
        NamespaceDetailsMap::iterator it = namespaces->find(n);
        verify(it != namespaces->end());
        BSONObj nsobj = BSON("ns" << ns);
        DBT ndbt;
        ndbt.data = const_cast<void *>(static_cast<const void *>(nsobj.objdata()));
        ndbt.size = nsobj.objsize();
        int r = nsdb->del(nsdb, cc().txn().db_txn(), &ndbt, DB_DELETE_ANY);
        verify(r == 0);

        // Should really only do this after the commit of the del.
        namespaces->erase(it);
    }

    void NamespaceIndex::add_ns(const char *ns, shared_ptr<NamespaceDetails> details) {
        Lock::assertWriteLocked(ns);

        init();
        Namespace n(ns);

        std::pair<NamespaceDetailsMap::iterator, bool> ret;
        ret = namespaces->insert(make_pair(n, details));
        dassert(ret.second == true);
    }

    void NamespaceIndex::update_ns(const char *ns, NamespaceDetails *details, bool overwrite) {
        Lock::assertWriteLocked(ns);
        dassert(namespaces.get() != NULL);

        BSONObj nsobj = BSON("ns" << ns);
        DBT ndbt, ddbt;
        ndbt.data = const_cast<void *>(static_cast<const void *>(nsobj.objdata()));
        ndbt.size = nsobj.objsize();
        BSONObj serialized = details->serialize();
        ddbt.data = const_cast<void *>(static_cast<const void *>(serialized.objdata()));
        ddbt.size = serialized.objsize();
        const int flags = overwrite ? 0 : DB_NOOVERWRITE;
        int r = nsdb->put(nsdb, cc().txn().db_txn(), &ndbt, &ddbt, flags);
        verify(r == 0);
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

