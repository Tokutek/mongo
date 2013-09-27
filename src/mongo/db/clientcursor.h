/* clientcursor.h */

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

/* Cursor -- and its derived classes -- are our internal cursors.

   ClientCursor is a wrapper that represents a cursorid from our database
   application's perspective.
*/

#pragma once

#include "mongo/pch.h"

#include <boost/thread/recursive_mutex.hpp>

#include "mongo/db/cursor.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/matcher.h"
#include "mongo/db/projection.h"
#include "mongo/db/keypattern.h"
#include "mongo/s/d_chunk_manager.h"
#include "mongo/util/net/message.h"
#include "mongo/util/net/listen.h"
#include "mongo/util/background.h"
#include "mongo/util/elapsed_tracker.h"

namespace mongo {

    typedef boost::recursive_mutex::scoped_lock recursive_scoped_lock;
    typedef long long CursorId; /* passed to the client so it can send back on getMore */
    static const CursorId INVALID_CURSOR_ID = -1; // But see SERVER-5726.
    class Cursor; /* internal server cursor base class */
    class ClientCursor;
    class ParsedQuery;

    /* todo: make this map be per connection.  this will prevent cursor hijacking security attacks perhaps.
     *       ERH: 9/2010 this may not work since some drivers send getMore over a different connection
    */
    typedef map<CursorId, ClientCursor*> CCById;

    extern BSONObj id_obj;
    
    bool opForSlaveTooOld(uint64_t ts);

    class ClientCursor : private boost::noncopyable {
        friend class CmdCursorInfo;
    public:
        static void assertNoCursors();

        /* use this to assure we don't in the background time out cursor while it is under use.
           if you are using noTimeout() already, there is no risk anyway.
           Further, this mechanism guards against two getMore requests on the same cursor executing
           at the same time - which might be bad.  That should never happen, but if a client driver
           had a bug, it could (or perhaps some sort of attack situation).
        */
        class Pin : boost::noncopyable {
        public:
            Pin( long long cursorid ) :
                _cursorid( INVALID_CURSOR_ID ) {
                recursive_scoped_lock lock( ccmutex );
                ClientCursor *cursor = ClientCursor::find_inlock( cursorid, true );
                if ( cursor ) {
                    uassert( 12051, "clientcursor already in use? driver problem?",
                            cursor->_pinValue < 100 );
                    cursor->_pinValue += 100;
                    _cursorid = cursorid;
                }
            }
            void release() {
                if ( _cursorid == INVALID_CURSOR_ID ) {
                    return;
                }
                ClientCursor *cursor = c();
                _cursorid = INVALID_CURSOR_ID;
                if ( cursor ) {
                    verify( cursor->_pinValue >= 100 );
                    cursor->_pinValue -= 100;
                }
            }
            ~Pin() { DESTRUCTOR_GUARD( release(); ) }
            ClientCursor *c() const { return ClientCursor::find( _cursorid ); }
        private:
            CursorId _cursorid;
        };

        /** Assures safe and reliable cleanup of a ClientCursor. */
        class Holder : boost::noncopyable {
        public:
            Holder( ClientCursor *c = 0 ) :
                _c( 0 ),
                _id( INVALID_CURSOR_ID ) {
                reset( c );
            }
            void reset( ClientCursor *c = 0 ) {
                if ( c == _c )
                    return;
                if ( _c ) {
                    // be careful in case cursor was deleted by someone else
                    ClientCursor::erase( _id );
                }
                if ( c ) {
                    _c = c;
                    _id = c->_cursorid;
                }
                else {
                    _c = 0;
                    _id = INVALID_CURSOR_ID;
                }
            }
            ~Holder() {
                DESTRUCTOR_GUARD ( reset(); );
            }
            ClientCursor* get() { return _c; }
            operator bool() { return _c; }
            ClientCursor * operator-> () { return _c; }
            const ClientCursor * operator-> () const { return _c; }
            /** Release ownership of the ClientCursor. */
            void release() {
                _c = 0;
                _id = INVALID_CURSOR_ID;
            }
        private:
            ClientCursor *_c;
            CursorId _id;
        };

        /**
         * Iterates through all ClientCursors, under its own ccmutex lock.
         * Also supports deletion on the fly.
         */
        class LockedIterator : boost::noncopyable {
        public:
            LockedIterator() : _lock( ccmutex ), _i( clientCursorsById.begin() ) {}
            bool ok() const { return _i != clientCursorsById.end(); }
            ClientCursor *current() const { return _i->second; }
            void advance() { ++_i; }
            /**
             * Delete 'current' and advance. Properly handles cascading deletions that may occur
             * when one ClientCursor is directly deleted.
             */
            void deleteAndAdvance();
        private:
            recursive_scoped_lock _lock;
            CCById::const_iterator _i;
        };
        
        ClientCursor(int queryOptions, const shared_ptr<Cursor>& c, const string& ns,
                     BSONObj query = BSONObj(), const bool inMultiStatementTxn = false );

        ~ClientCursor();

        // ***************  basic accessors *******************

        CursorId cursorid() const { return _cursorid; }
        string ns() const { return _ns; }
        Database * db() const { return _db; }
        const BSONObj& query() const { return _query; }
        int queryOptions() const { return _queryOptions; }

        /* Get rid of cursors for namespaces 'ns'. When dropping a db, ns is "dbname."
           Used by drop, dropIndexes, dropDatabase.
        */
        static void invalidate(const StringData &ns);
        static void invalidateAllCursors();

        // --- some pass through helpers for Cursor ---

        Cursor* c() const { return _c.get(); }
        int pos() const { return _pos; }

        void incPos( int n ) { _pos += n; } // TODO: this is bad
        void setPos( int n ) { _pos = n; } // TODO : this is bad too

        BSONObj indexKeyPattern() { return _c->indexKeyPattern();  }
        bool modifiedKeys() const { return _c->modifiedKeys(); }
        bool isMultiKey() const { return _c->isMultiKey(); }

        bool ok() { return _c->ok(); }
        bool advance() { return _c->advance(); }
        BSONObj current() { return _c->current(); }
        BSONObj currPK() { return _c->currPK(); }
        BSONObj currKey() const { return _c->currKey(); }

        /**
         * same as BSONObj::getFieldsDotted
         * if it can be retrieved from key, it is
         * @param holder keeps the currKey in scope by keeping a reference to it here. generally you'll want 
         *        holder and ret to destruct about the same time.
         * @return if this was retrieved from key
         */
        bool getFieldsDotted( const string& name, BSONElementSet &ret, BSONObj& holder );

        /**
         * same as BSONObj::getFieldDotted
         * if it can be retrieved from key, it is
         * @return if this was retrieved from key
         */
        BSONElement getFieldDotted( const string& name , BSONObj& holder , bool * fromKey = 0 ) ;
        
        /** extract items from object which match a pattern object.
         * e.g., if pattern is { x : 1, y : 1 }, builds an object with
         * x and y elements of this object, if they are present.
         * returns elements with original field names
         * NOTE: copied from BSONObj::extractFields
        */
        BSONObj extractFields(const BSONObj &pattern , bool fillWithNull = false) ;

        /** Extract elements from the object this cursor currently points to, using the expression
         *  specified in KeyPattern. Will use a covered index if the one in this cursor is usable.
         *  TODO: there are some cases where a covered index could be used but is not, for instance
         *  if both this index and the keyPattern are {a : "hashed"}
         */
        BSONObj extractKey( const KeyPattern& usingKeyPattern ) const;

        void fillQueryResultFromObj( BufBuilder &b, const MatchDetails* details = NULL ) const;

        bool currentIsDup() {
            return _c->getsetdup( _c->currPK() );
        }

        bool currentMatches() {
            if ( ! _c->matcher() )
                return true;
            return _c->matcher()->matchesCurrent( _c.get() );
        }

        void setChunkManager( ShardChunkManagerPtr manager ){ _chunkManager = manager; }
        ShardChunkManagerPtr getChunkManager(){ return _chunkManager; }

    private:
        static ClientCursor* find_inlock(CursorId id, bool warn = true) {
            CCById::iterator it = clientCursorsById.find(id);
            if ( it == clientCursorsById.end() ) {
                if ( warn )
                    OCCASIONALLY out() << "ClientCursor::find(): cursor not found in map " << id << " (ok after a drop)\n";
                return 0;
            }
            return it->second;
        }

    public:
        static ClientCursor* find(CursorId id, bool warn = true) {
            recursive_scoped_lock lock(ccmutex);
            ClientCursor *c = find_inlock(id, warn);
            // if this asserts, your code was not thread safe - you either need to set no timeout
            // for the cursor or keep a ClientCursor::Pointer in scope for it.
            massert( 12521, "internal error: use of an unlocked ClientCursor", c == 0 || c->_pinValue );
            return c;
        }

        /**
         * Deletes the cursor with the provided @param 'id' if one exists.
         * @throw if the cursor with the provided id is pinned.
         * This does not do any auth checking and should be used only when erasing cursors as part
         * of cleaning up internal operations.
         */
        static bool erase(CursorId id);
        // Same as erase but checks to make sure this thread has read permission on the cursor's
        // namespace.  This should be called when receiving killCursors from a client.  This should
        // not be called when ccmutex is held.
        static bool eraseIfAuthorized(CursorId id);

        /**
         * @return number of cursors found
         */
        static int erase(int n, long long* ids);
        static int eraseIfAuthorized(int n, long long* ids);

        /**
         * @param millis amount of idle passed time since last call
         */
        bool shouldTimeout( unsigned millis );

        void storeOpForSlave( BSONObj curr );
        void updateSlaveLocation( CurOp& curop );
        bool lastOpForSlaveTooOld();

        unsigned idleTime() const { return _idleAgeMillis; }
        
    public: // static methods

        static void idleTimeReport(unsigned millis);

        static void appendStats( BSONObjBuilder& result );
        static unsigned numCursors() { return clientCursorsById.size(); }
        static void find( const string& ns , set<CursorId>& all );

    public:

        // The Cursor destructor closes its ydb cursor, which reqquires that the
        // transaction that created it be live. Becuse of this, the transaction
        // stack pointer needs to stay in scope around the cursor, and so we
        // declare it first.
        shared_ptr<Client::TransactionStack> transactions; // the transaction this cursor is under,
                                                           // only set to support getMore() 
        
        // True if the above transaction stack is multi-statement. If it is,
        // then this caller must be the owner of that multi-statement txn,
        // otherwise uassert.
        //
        // If the above transaction stack is not multi-statement, return false.
        // It will be necessary to use WithTxnStack(transactions) in order to
        // use this cursor again.
        bool checkMultiStatementTxn();

    private: // methods

        // cursors normally timeout after an inactivity period to prevent excess memory use
        // setting this prevents timeout of the cursor in question.
        void noTimeout() { _pinValue++; }

        static bool _erase_inlock(ClientCursor* cursor);

        CursorId _cursorid;

        const string _ns;
        Database * _db;

        const shared_ptr<Cursor> _c;
        map<string,int> _indexedFields;  // map from indexed field to offset in key object
        int _pos;                        // # objects into the cursor so far

        const BSONObj _query;            // used for logging diags only; optional in constructor
        int _queryOptions;        // see enum QueryOptions dbclient.h

        // if this ClientCursor belongs to a secondary that is pulling
        // data for replication, this will hold the point that the slave has
        // read up to. Used for write concern
        GTID _slaveReadTill;
        uint64_t _slaveReadTillTS;

        unsigned _idleAgeMillis;                 // how long has the cursor been around, relative to server idle time

        /* 0 = normal
           1 = no timeout allowed
           100 = in use (pinned) -- see Pointer class
        */
        unsigned _pinValue;

        ShardChunkManagerPtr _chunkManager;
        bool _partOfMultiStatementTxn;

    public:
        shared_ptr<ParsedQuery> pq;
        shared_ptr<Projection> fields; // which fields query wants returned

    private: // static members

        static CCById clientCursorsById;
        static long long numberTimedOut;
        static boost::recursive_mutex& ccmutex;   // must use this for all statics above!
        static CursorId allocCursorId_inlock();

    };

    class ClientCursorMonitor : public BackgroundJob {
    public:
        string name() const { return "ClientCursorMonitor"; }
        void run();
    };

} // namespace mongo
