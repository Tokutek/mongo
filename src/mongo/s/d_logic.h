// @file d_logic.h
/*
 *    Copyright (C) 2010 10gen Inc.
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
#include "mongo/s/d_chunk_manager.h"
#include "mongo/s/chunk_version.h"
#include "mongo/util/concurrency/rwlock.h"
#include "mongo/util/concurrency/ticketholder.h"

namespace mongo {

    class Database;
    //class DiskLoc;
    struct DbResponse;

    typedef ChunkVersion ConfigVersion;

    // --------------
    // --- global state ---
    // --------------

    class ShardingState {
    public:
        ShardingState();

        bool enabled() const { return _enabled; }
        const string& getConfigServer() const { return _configServer; }
        void enable( const string& server );

        // Initialize sharding state and begin authenticating outgoing connections and handling
        // shard versions.  If this is not run before sharded operations occur auth will not work
        // and versions will not be tracked.
        static void initialize(const string& server);

        void gotShardName( const string& name );
        void gotShardHost( string host );

        string getShardName() { return _shardName; }
        string getShardHost() { return _shardHost; }

        /** Reverts back to a state where this mongod is not sharded. */
        void resetShardingState(); 

        // versioning support

        bool hasVersion( const string& ns );
        bool hasVersion( const string& ns , ConfigVersion& version );
        const ConfigVersion getVersion( const string& ns ) const;

        /**
         * Uninstalls the manager for a given collection. This should be used when the collection is dropped.
         *
         * NOTE:
         *   An existing collection with no chunks on this shard will have a manager on version 0, which is different than a
         *   a dropped collection, which will not have a manager.
         *
         * TODO
         *   When sharding state is enabled, absolutely all collections should have a manager. (The non-sharded ones are
         *   a be degenerate case of one-chunk collections).
         *   For now, a dropped collection and an non-sharded one are indistinguishable (SERVER-1849)
         *
         * @param ns the collection to be dropped
         */
        void resetVersion( const string& ns );

        /**
         * Requests to access a collection at a certain version. If the collection's manager is not at that version it
         * will try to update itself to the newest version. The request is only granted if the version is the current or
         * the newest one.
         *
         * @param ns collection to be accessed
         * @param version (IN) the client believe this collection is on and (OUT) the version the manager is actually in
         * @return true if the access can be allowed at the provided version
         */
        bool trySetVersion( const string& ns , ConfigVersion& version );

        void appendInfo( BSONObjBuilder& b );

        // querying support

        bool needShardChunkManager( const string& ns ) const;
        ShardChunkManagerPtr getShardChunkManager( const string& ns );

        // chunk migrate and split support

        /**
         * Creates and installs a new chunk manager for a given collection by "forgetting" about one of its chunks.
         * The new manager uses the provided version, which has to be higher than the current manager's.
         * One exception: if the forgotten chunk is the last one in this shard for the collection, version has to be 0.
         *
         * If it runs successfully, clients need to grab the new version to access the collection.
         *
         * @param ns the collection
         * @param min max the chunk to eliminate from the current manager
         * @param version at which the new manager should be at
         */
        void donateChunk( const string& ns , const BSONObj& min , const BSONObj& max , ChunkVersion version );

        /**
         * Creates and installs a new chunk manager for a given collection by reclaiming a previously donated chunk.
         * The previous manager's version has to be provided.
         *
         * If it runs successfully, clients that became stale by the previous donateChunk will be able to access the
         * collection again.
         *
         * @param ns the collection
         * @param min max the chunk to reclaim and add to the current manager
         * @param version at which the new manager should be at
         */
        void undoDonateChunk( const string& ns , const BSONObj& min , const BSONObj& max , ChunkVersion version );

        /**
         * Creates and installs a new chunk manager for a given collection by splitting one of its chunks in two or more.
         * The version for the first split chunk should be provided. The subsequent chunks' version would be the latter with the
         * minor portion incremented.
         *
         * The effect on clients will depend on the version used. If the major portion is the same as the current shards,
         * clients shouldn't perceive the split.
         *
         * @param ns the collection
         * @param min max the chunk that should be split
         * @param splitKeys point in which to split
         * @param version at which the new manager should be at
         */
        void splitChunk( const string& ns , const BSONObj& min , const BSONObj& max , const vector<BSONObj>& splitKeys ,
                         ChunkVersion version );

        bool inCriticalMigrateSection();

        /**
         * @return true if we are NOT in the critical section
         */
        bool waitTillNotInCriticalSection( int maxSecondsToWait );

        /** A scope in which client threads can read or write data without racing with a chunk
            version change.  The handlePossibleShardedMessage functionality is a method here to make
            sure we only check it inside this scope (except for queries, see below).  Currently is
            just a shared lock of _rwlock.  This class's lifetime and access should be managed
            through Client::ShardedOperationScope. */
        class ShardedOperationScope : public boost::noncopyable {
            scoped_ptr<RWLockRecursive::Shared> _lk;
          public:
            ShardedOperationScope();
            void checkPossiblyShardedMessage(int op, const string &ns) const;
            void checkPossiblyShardedMessage(Message &m) const;
            bool handlePossibleShardedMessage(Message &m, DbResponse *dbresponse) const;
        };

        /** A scope in which it is safe to modify the version information.  Current implementation
            is just an exclusive lock of _rwlock.  Has temprelease because there are situations
            where we need to wait for a migration to exit its critical section, and we'd deadlock
            there without it.  */
        class SetVersionScope : public boost::noncopyable {
            scoped_ptr<RWLockRecursive::Exclusive> _lk;
          public:
            SetVersionScope();
            class temprelease : public boost::noncopyable {
                SetVersionScope& _sc;
              public:
                temprelease(SetVersionScope& sc);
                ~temprelease();
            };
        };

    private:
        bool _enabled;

        string _configServer;

        string _shardName;
        string _shardHost;

        // protects state below
        mutable mongo::mutex _mutex;
        // protects accessing the config server
        // Using a ticket holder so we can have multiple redundant tries at any given time
        mutable TicketHolder _configServerTickets;

        // map from a namespace into the ensemble of chunk ranges that are stored in this mongod
        // a ShardChunkManager carries all state we need for a collection at this shard, including its version information
        typedef map<string,ShardChunkManagerPtr> ChunkManagersMap;
        ChunkManagersMap _chunks;

        mutable RWLockRecursive _rwlock;
    };

    extern ShardingState shardingState;

    /**
     * one per connection from mongos
     * holds version state for each namespace
     */
    class ShardedConnectionInfo {
    public:
        ShardedConnectionInfo();

        const OID& getID() const { return _id; }
        bool hasID() const { return _id.isSet(); }
        void setID( const OID& id );

        const ConfigVersion getVersion( const string& ns ) const;
        void setVersion( const string& ns , const ConfigVersion& version );

        static ShardedConnectionInfo* get( bool create );
        static void reset();
        static void addHook();

        bool inForceVersionOkMode() const {
            return _forceVersionOk;
        }

        void enterForceVersionOkMode() { _forceVersionOk = true; }
        void leaveForceVersionOkMode() { _forceVersionOk = false; }

    private:

        OID _id;
        bool _forceVersionOk; // if this is true, then chunk version #s aren't check, and all ops are allowed

        typedef map<string,ConfigVersion> NSVersionMap;
        NSVersionMap _versions;

        static boost::thread_specific_ptr<ShardedConnectionInfo> _tl;
    };

    struct ShardForceVersionOkModeBlock {
        ShardForceVersionOkModeBlock() {
            info = ShardedConnectionInfo::get( false );
            if ( info )
                info->enterForceVersionOkMode();
        }
        ~ShardForceVersionOkModeBlock() {
            if ( info )
                info->leaveForceVersionOkMode();
        }

        ShardedConnectionInfo * info;
    };

    // -----------------
    // --- core ---
    // -----------------

    unsigned long long extractVersion( BSONElement e , string& errmsg );


    /**
     * @return true if we have any shard info for the ns
     */
    bool haveLocalShardingInfo( const string& ns );

    /**
     * @return true if the current threads shard version is ok, or not in sharded version
     * Also returns an error message and the Config/ChunkVersions causing conflicts
     */
    bool shardVersionOk( const string& ns , string& errmsg, ConfigVersion& received, ConfigVersion& wanted );

    /**
     * MustHandleShardedMessage encapsulates the information we need to return to the client when we detect a chunk version problem.
     * We may need to pass this up the stack to where we have a DbResponse *, which is why this is implemented as an exception (see dbcommands.cpp).
     */
    class MustHandleShardedMessage : public DBException {
        string _shardingError;
        ConfigVersion _received;
        ConfigVersion _wanted;
      public:
        MustHandleShardedMessage(const string &shardingError, const ConfigVersion &received, const ConfigVersion &wanted)
                : DBException("handle sharded message exception", 17222), // msgasserted(17222, "reserve 17222");
                  _shardingError(shardingError),
                  _received(received),
                  _wanted(wanted) {}
        virtual ~MustHandleShardedMessage() throw() {}
        void handleShardedMessage(Message &m, DbResponse *dbresponse) const;
    };

    /**
     * Checks for a chunk version mismatch between the current thread's ShardConnectionInfo and the chunk version for ns.
     * If a mismatch is detected (and the op cares about that), throws MustHandleShardedMessage.
     */
    void _checkPossiblyShardedMessage(int op, const string &ns);

    /**
     * Gets the operation and ns from a Message and calls the above.
     */
    void _checkPossiblyShardedMessage(Message &m);

    /**
     * Queries check the shard version without a lock, and instead make sure the version doesn't change while the query is running,
     * so they need to be able to check without holding the lock.
     */
    inline void checkPossiblyShardedMessageWithoutLock(Message &m) {
        if (!shardingState.enabled()) {
            return;
        }
        _checkPossiblyShardedMessage(m);
    }

    inline ShardingState::ShardedOperationScope::ShardedOperationScope()
            : _lk(shardingState.enabled() ? new RWLockRecursive::Shared(shardingState._rwlock) : NULL) {}

    inline void ShardingState::ShardedOperationScope::checkPossiblyShardedMessage(int op, const string &ns) const {
        if (!shardingState.enabled()) {
            return;
        }
        _checkPossiblyShardedMessage(op, ns);
    }

    /**
     * Convenience function for emulating the old handlePossibleShardedMessage behavior, wrapping the exception implementation.
     */
    inline bool ShardingState::ShardedOperationScope::handlePossibleShardedMessage(Message &m, DbResponse *dbresponse) const {
        if (!shardingState.enabled()) {
            return false;
        }
        try {
            _checkPossiblyShardedMessage(m);
            return false;
        }
        catch (MustHandleShardedMessage &e) {
            e.handleShardedMessage(m, dbresponse);
            return true;
        }
    }

    inline ShardingState::SetVersionScope::SetVersionScope()
            : _lk(new RWLockRecursive::Exclusive(shardingState._rwlock)) {}

    inline ShardingState::SetVersionScope::temprelease::temprelease(ShardingState::SetVersionScope& sc)
            : _sc(sc) {
        _sc._lk.reset();
    }

    inline ShardingState::SetVersionScope::temprelease::~temprelease() {
        _sc._lk.reset(new RWLockRecursive::Exclusive(shardingState._rwlock));
    }

    bool shouldLogOpForSharding(const char *opstr, const char *ns, const BSONObj &obj);
    bool shouldLogUpdateOpForSharding(const char *opstr, const char *ns, const BSONObj &oldObj, const BSONObj &newObj);
    void startObjForMigrateLog(BSONObjBuilder &b);
    void writeObjToMigrateLog(BSONObj &obj);
    void writeObjToMigrateLogRef(BSONObj &obj);

}
