/* @file db/client.h

   "Client" represents a connection to the database (the server-side) and corresponds
   to an open socket (or logical connection if pooling on sockets) from a client.

   todo: switch to asio...this will fit nicely with that.
*/

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

#include <stack>

#include "mongo/db/security.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/stats/top.h"
#include "mongo/db/client_common.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/lockstate.h"
#include "mongo/db/gtid.h"
#include "mongo/db/txn_context.h"
#include "mongo/db/opsettings.h"
#include "mongo/util/paths.h"
#include "mongo/util/concurrency/threadlocal.h"
#include "mongo/util/net/message_port.h"
#include "mongo/util/concurrency/rwlock.h"

namespace mongo {

    class AuthenticationInfo;
    class Database;
    class CurOp;
    class Command;
    class Client;
    class AbstractMessagingPort;
    class LockCollectionForReading;
    class DBClientConnection;
    class ReplSet;

    extern ReplSet *theReplSet;
    extern RWLockRecursive operationLock;

    TSP_DECLARE(Client, currentClient)

    typedef long long ConnectionId;

    /** the database's concept of an outside "client" */
    class Client : public ClientBasic {
    public:
        // always be in clientsMutex when manipulating this. killop stuff uses these.
        static set<Client*>& clients;
        static mongo::mutex& clientsMutex;
        static int getActiveClientCount( int& writers , int& readers );
        class Context;
        ~Client();
        static void getReaderWriterClientCount( int *readers, int *writers );
        /** each thread which does db operations has a Client object in TLS.
         *  call this when your thread starts.
        */
        static Client& initThread(const char *desc, AbstractMessagingPort *mp = 0);

        static void initThreadIfNotAlready(const char *desc) { 
            if( currentClient.get() )
                return;
            initThread(desc);
        }
        static void abortLiveTransactions();


        /** this has to be called as the client goes away, but before thread termination
         *  @return true if anything was done
         */
        bool shutdown();

        string clientAddress(bool includePort=false) const;
        const AuthenticationInfo * getAuthenticationInfo() const { return &_ai; }
        AuthenticationInfo * getAuthenticationInfo() { return &_ai; }
        bool isAdmin() { return _ai.isAuthorized( "admin" ); }
        CurOp* curop() const { return _curOp; }
        Context* getContext() const { return _context; }
        Database* database() const {  return _context ? _context->db() : 0; }
        const char *ns() const { return _context->ns(); }
        const std::string desc() const { return _desc; }

        // these function for threads that do writes to report to the client
        // what the last GTID completed was. When a transaction commits,
        // this value is set. Subsequently, when getLastError is called,
        // this value is read to determine what point slaves should 
        // catch up to in order to satisfy write concern
        void setLastOp( GTID gtid ) { _lastGTID = gtid; }
        GTID getLastOp() const { return _lastGTID; }

        /** caution -- use Context class instead */
        void setContext(Context *c) { _context = c; }

        /* report what the last operation was.  used by getlasterror */
        void appendLastGTID( BSONObjBuilder& b ) const;

        bool isGod() const { return _god; } /* this is for map/reduce writes */
        string toString() const;
        void gotHandshake( const BSONObj& o );
        bool hasRemote() const { return _mp; }
        HostAndPort getRemote() const { verify( _mp ); return _mp->remote(); }
        BSONObj getRemoteID() const { return _remoteId; }
        BSONObj getHandshake() const { return _handshake; }
        AbstractMessagingPort * port() const { return _mp; }
        ConnectionId getConnectionId() const { return _connectionId; }

        LockState& lockState() { return _ls; }

        /**
         * A stack of transactions, with parent/child relationships.
         * There is zero or one of these per Client.
         * Client::Transaction is a proxy for calls to beginTxn, commitTxn, and abortTxn, you should probably use that instead.
         * It is possible for one of these to get stolen from a Client, for example, if a cursor needs to persist it between requests.
         */
        class TransactionStack : boost::noncopyable {
            // If we had emplace we wouldn't need a shared_ptr...
            std::stack<shared_ptr<TxnContext> > _txns;
          public:
            TransactionStack() {}
            ~TransactionStack() {
                // This ensures that things get destroyed in the right order, I don't know if std::stack gives that guarantee.
                while (hasLiveTxn()) {
                    abortTxn();
                }
            }

            /** Begin a new transaction as a child of the innermost, or as a new root. */
            void beginTxn(int flags);
            /** Commit the innermost transaction. */
            void commitTxn(int flags);
            void commitTxn();
            /** Abort the innermost transaction. */
            void abortTxn();
            uint32_t numLiveTxns();

            /** @return true iff this transaction stack has a live txn. */
            bool hasLiveTxn() const;
            /** @return the innermost transaction. */
            TxnContext &txn() const;
        };

        /**
         * A convenience object to create an alternate transaction stack for a temporary scope.
         * Useful if you need a new transaction with a different root than the current client's transactions.
         * Swaps back the old one when it gets destroyed.
         */
        class AlternateTransactionStack : boost::noncopyable {
            shared_ptr<TransactionStack> _saved;
          public:
            AlternateTransactionStack();
            ~AlternateTransactionStack();
        };

        /**
         * A convenience object to create scoped transactions.
         * Knows what txn it created in case the stack gets swapped out underneath.
         * This class doesn't actually store the DB_TXN, that lives in storage::Txn.
         * This class manages the *lifetime* of a transaction.
         * If the TransactionStack gets swapped out during the lifetime of this object, that gets detected and the destructor becomes a no-op.
         */
        class Transaction : boost::noncopyable {
            const TxnContext *_txn;
          public:
            explicit Transaction(int flags);
            ~Transaction();
            void commit(int flags);
            void commit();
            void abort();
        };

        bool hasTxn() const {
            if (!_transactions) {
                return false;
            }
            return _transactions->hasLiveTxn();
        }

        const shared_ptr<TransactionStack> &txnStack() const {
            return _transactions;
        }

        void commitTopTxn() {
            _transactions->commitTxn();
        }

        void abortTopTxn() {
            _transactions->abortTxn();
        }

        void beginClientTxn(int flags) {
            if (!_transactions) {
                _transactions.reset(new TransactionStack());
            }
            _transactions->beginTxn(flags);
        }

        uint32_t txnStackSize() {
            if (!_transactions) {
                return 0;
            }
            return _transactions->numLiveTxns();
        }

        TxnContext &txn() const {
            dassert(hasTxn());
            return _transactions->txn();
        }

        shared_ptr<DBClientConnection> authConn() {
            return _authConn;
        }

        void setAuthConn(shared_ptr<DBClientConnection> conn) {
            _authConn = conn;
        }

        OpSettings opSettings() const {
            return _opSettings;
        }

        void setOpSettings (const OpSettings& settings) {
            _opSettings = settings;
        }

        /**
         * Swap out the transaction stack to another location.
         * This breaks the relationship with any Client::Transaction objects, which is useful for getMore() and one day multi-statement transactions.
         */
        void swapTransactionStack(shared_ptr<TransactionStack> &other) {
            _transactions.swap(other);
        }

        /**
         * After you have saved a TransactionStack somewhere, you can use this class to temporarily return it to cc() and then save it back out again.
         */
        class WithTxnStack : boost::noncopyable {
            shared_ptr<Client::TransactionStack> &_stack;
            bool _released;
          public:
            WithTxnStack(shared_ptr<Client::TransactionStack> &stack);
            ~WithTxnStack();
            void release() {
                _released = true;
            }
        };

        /**
         * Info about the load currently in progress for this client, if one exists.
         */
        class LoadInfo : boost::noncopyable {
            Client::Transaction _txn;
            string _bulkLoadNS;
          public:
            LoadInfo(const StringData &ns) : _txn(DB_SERIALIZABLE), _bulkLoadNS(ns.toString()) {}
            void commitTxn() { _txn.commit(); }
            const string &bulkLoadNS() const { return _bulkLoadNS; }
        };

        /** Enter load mode for a particular namespace, given indexes and options. */
        void beginClientLoad(const StringData &ns, const vector<BSONObj> &indexes,
                             const BSONObj &options);

        /** Commit the client load. uasserts if none is in progress. */
        void commitClientLoad();

        /** Abort the client load. uasserts if none is in progress. */
        void abortClientLoad();

        /** @return true if a load is in progress. */
        bool loadInProgress() const;

        // HACK we need this until upserts go through the NamespaceDetails class
        //      and can prevent writes on a bulk loaded collection automatically.
        string bulkLoadNS() const { return _loadInfo ? _loadInfo->bulkLoadNS() : ""; }

    private:
        Client(const char *desc, AbstractMessagingPort *p = 0);
        friend class CurOp;
        ConnectionId _connectionId; // > 0 for things "conn", 0 otherwise
        string _threadId; // "" on non support systems
        CurOp * _curOp;
        Context * _context;
        shared_ptr<TransactionStack> _transactions;
        shared_ptr<LoadInfo> _loadInfo; // the txn and ns currently under-going bulk load by this client
        bool _shutdown; // to track if Client::shutdown() gets called
        std::string _desc;
        bool _god;
        AuthenticationInfo _ai;
        GTID _lastGTID;
        BSONObj _handshake;
        BSONObj _remoteId;
        AbstractMessagingPort * const _mp;
        OpSettings _opSettings;

        // for CmdCopyDb and CmdCopyDbGetNonce
        shared_ptr< DBClientConnection > _authConn;

        LockState _ls;
        
    public:

        /* set _god=true temporarily, safely */
        class GodScope {
            bool _prev;
        public:
            GodScope();
            ~GodScope();
        };

        /* Set database we want to use, then, restores when we finish (are out of scope)
           Note this is also helpful if an exception happens as the state if fixed up.
        */
        class Context : boost::noncopyable {
        public:
            /** this is probably what you want */
            Context(const StringData &ns, const StringData &path=dbpath, bool doauth=true, bool doVersion=true );

            /** note: this does not call finishInit -- i.e., does not call 
                      shardVersionOk() for example. 
                see also: reset().
            */
            Context( const StringData &ns , Database * db, bool doauth=true );

            // used by ReadContext
            Context(const StringData &path, const StringData &ns, Database *db, bool doauth);

            ~Context();
            Client* getClient() const { return _client; }
            Database* db() const { return _db; }
            const char * ns() const { return _ns.c_str(); }
            bool equals( const StringData &ns , const StringData &path=dbpath ) const { return _ns == ns && _path == path; }

            /** @return true iff the current Context is using db/path */
            bool inDB( const StringData& db , const StringData& path=dbpath ) const;

            void _clear() { // this is sort of an "early destruct" indication, _ns can never be uncleared
                const_cast<string&>(_ns).clear();
                _db = 0;
            }

            /** call before unlocking, so clear any non-thread safe state
             *  _db gets restored on the relock
             */
            void unlocked() { _db = 0; }

            /** call after going back into the lock, will re-establish non-thread safe stuff */
            void relocked() { _finishInit(); }

        private:
            friend class CurOp;
            void _finishInit( bool doauth=true);
            void _auth( int lockState );
            void checkNotStale() const;
            void checkNsAccess( bool doauth );
            void checkNsAccess( bool doauth, int lockState );
            Client * const _client;
            Context * const _oldContext;
            const string _path;
            bool _doVersion;
            const string _ns;
            Database * _db;
            
            Timer _timer;
        }; // class Client::Context

        /** "read lock, and set my context, all in one operation" 
         */
        class ReadContext : boost::noncopyable { 
        public:
            ReadContext(const StringData &ns, const StringData &path=dbpath, bool doauth=true);
            Context& ctx() { return _c; }
        private:
            Lock::DBRead _lk;
            Client::Context _c;
        };

        class WriteContext : boost::noncopyable {
        public:
            WriteContext(const StringData &ns, const StringData &path=dbpath, bool doauth=true );
            Context& ctx() { return _c; }
        private:
            Lock::DBWrite _lk;
            Context _c;
        };

    }; // class Client

    /** get the Client object for this thread. */
    inline Client& cc() {
        Client * c = currentClient.get();
        verify( c );
        return *c;
    }

    inline Client::WithTxnStack::WithTxnStack(shared_ptr<Client::TransactionStack> &stack) : _stack(stack), _released(false) {
        cc().swapTransactionStack(_stack);
    }
    inline Client::WithTxnStack::~WithTxnStack() {
        if (!_released) {
            cc().swapTransactionStack(_stack);
        }
    }

    inline Client::GodScope::GodScope() {
        _prev = cc()._god;
        cc()._god = true;
    }

    inline Client::GodScope::~GodScope() { cc()._god = _prev; }

    inline bool haveClient() { return currentClient.get() > 0; }

};
