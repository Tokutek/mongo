// namespace_details.h

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

#pragma once

#include "mongo/pch.h"

#include "mongo/db/jsobj.h"
#include "mongo/db/storage/dictionary.h"
#include "mongo/util/concurrency/simplerwlock.h"
#include "mongo/util/string_map.h"

namespace mongo {

    class NamespaceDetails;
    class CollectionMap;

    // Gets the collection map (ns -> Collection) for this client threads' current database.
    // You pass the namespace you want so we can verify you're accessing the right database.
    CollectionMap *collectionMap(const StringData &ns);

    /* CollectionMap maps namespace string to collection.
     * If a collection is not found, it may exist, but not be open.
     * If a collectoin is found, it definately exists, and it's open.
     */
    class CollectionMap {
    public:
        CollectionMap(const string &dir, const StringData &database);

        ~CollectionMap();

        void init(bool may_create = false);

        // @return true if the ns existed and was closed, false otherwise.
        bool close_ns(const StringData &ns, const bool aborting = false);

        // The index entry for ns is removed and brought up-to-date with the _metadb on txn abort.
        void add_ns(const StringData &ns, shared_ptr<NamespaceDetails> details);

        // The index entry for ns is removed and brought up-to-date with the _metadb on txn abort.
        void kill_ns(const StringData &ns);

        // If something changes that causes details->serialize() to be different,
        // call this to persist it to the _metadb.
        void update_ns(const StringData &ns, const BSONObj &serialized, bool overwrite);

        // Find an NamespaceDetails in the map.
        // Will not open the ns if it is closed.
        NamespaceDetails *find_ns(const StringData &ns);

        // Every namespace that exists has an entry in _namespaces. Some
        // entries may be "closed" in the sense that the key exists but the
        // value is null. If the desired namespace is closed, we open it,
        // which must succeed, by the first invariant.
        NamespaceDetails *getCollection(const StringData &ns);

        bool allocated() const { return _metadb; }

        void getNamespaces( list<string>& tofill );

        // drop all collections and the metadb, we're removing this database
        void drop();

        void rollbackCreate();

        typedef StringMap<shared_ptr<NamespaceDetails> > NamespaceDetailsMap;

    private:
        int _openMetadb(bool may_create);
        void _init(bool may_create);

        // @return NamespaceDetails object if the ns is currently open, NULL otherwise.
        // requires: openRWLock is locked, either shared or exclusively.
        NamespaceDetails *find_ns_locked(const StringData &ns) {
            NamespaceDetailsMap::const_iterator it = _namespaces.find(ns);
            if (it != _namespaces.end()) {
                verify(it->second.get() != NULL);
                return it->second.get();
            }
            return NULL;
        }

        // @return NamespaceDetails object if the ns existed and is now open, NULL otherwise.
        // called with no locks held - synchronization is done internally.
        NamespaceDetails *open_ns(const StringData &ns, const bool bulkLoad = false);

        // Only beginBulkLoad may call open_ns with bulkLoad = true.
        friend void beginBulkLoad(const StringData &ns, const vector<BSONObj> &indexes, const BSONObj &options);

        NamespaceDetailsMap _namespaces;
        const string _dir;
        const string _metadname;
        const string _database;

        // The underlying ydb dictionary that stores namespace information.
        // - May not transition _metadb from non-null to null in a DBRead lock.
        shared_ptr<storage::Dictionary> _metadb;

        // It isn't necessary to hold either of these locks in a a DBWrite lock.

        // This lock protects access to the _namespaces variable
        // With a DBRead lock and this shared lock, one can retrieve
        // a NamespaceDetails that has already been opened
        SimpleRWLock _openRWLock;
    };

} // namespace mongo
