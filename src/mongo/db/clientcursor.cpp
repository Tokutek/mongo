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

/* clientcursor.cpp

   ClientCursor is a wrapper that represents a cursorid from our database
   application's perspective.

   Cursor -- and its derived classes -- are our internal cursors.
*/

#include "mongo/pch.h"

#include "mongo/db/clientcursor.h"

#include <string>
#include <time.h>
#include <vector>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/commands.h"
#include "mongo/db/database.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/db/introspect.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/kill_current_op.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/repl_block.h"
#include "mongo/db/parsed_query.h"
#include "mongo/db/scanandorder.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/repl/rs.h"
#include "mongo/platform/random.h"
#include "mongo/util/debug_util.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/timer.h"

namespace mongo {

    CCById ClientCursor::clientCursorsById;
    boost::recursive_mutex& ClientCursor::ccmutex( *(new boost::recursive_mutex()) );
    long long ClientCursor::numberTimedOut = 0;

    void ClientCursor::invalidateAllCursors() {
        verify(Lock::isW());
        for( LockedIterator i; i.ok(); ) {
            i.deleteAndAdvance();
        }
    }

    /* ------------------------------------------- */

    // ns is either a full namespace or "dbname." when invalidating for a whole db
    void ClientCursor::invalidate(const StringData &ns) {
        Lock::assertWriteLocked(ns);
        size_t dotpos = ns.find('.');
        verify(dotpos != string::npos);
        bool isDB = (dotpos + 1) == ns.size(); // first (and only) dot is the last char

        {
            //cout << "\nTEMP invalidate " << ns << endl;
            Database *db = cc().database();
            verify(db);
            verify( ns.startsWith(db->name()) );

            for( LockedIterator i; i.ok(); ) {
                ClientCursor *cc = i.current();

                bool shouldDelete = false;
                if (cc->c()->shouldDestroyOnNSDeletion() && cc->_db == db) {
                    if (isDB) {
                        // already checked that db matched above
                        dassert( StringData(cc->_ns).startsWith(ns) );
                        shouldDelete = true;
                    }
                    else {
                        if ( ns == cc->_ns )
                            shouldDelete = true;
                    }
                }

                if ( shouldDelete ) {
                    i.deleteAndAdvance();
                }
                else {
                    i.advance();
                }
            }
        }
    }

    int ClientCursor::idleAgeTimeoutMillis = 600000;
    /**
     * CursorTimeoutParameter is only for use by cursor_timeout.js right now.
     * Someday it might be worth exporting as a feature, but that doesn't seem too useful right now.
     */
    class CursorTimeoutParameter : public ExportedServerParameter<int> {
      public:
        CursorTimeoutParameter()
                : ExportedServerParameter<int>( ServerParameterSet::getGlobal(), "cursorTimeout",
                                                &ClientCursor::idleAgeTimeoutMillis, false, true ) {}
      protected:
        virtual Status validate(const int& potentialNewValue) {
            if (potentialNewValue <= 0) {
                return Status(ErrorCodes::BadValue, "cursorTimeout must be > 0");
            }
            return Status::OK();
        }
    };
    MONGO_INITIALIZER(RegisterCursorTimeoutParameter)(InitializerContext* context) {
        if (Command::testCommandsEnabled) {
            // Leaked intentionally: a ServerParameter registers itself when constructed.
            new CursorTimeoutParameter();
        }
        return Status::OK();
    }

    /* note called outside of locks (other than ccmutex) so care must be exercised */
    bool ClientCursor::shouldTimeout( unsigned millis ) {
        _idleAgeMillis += millis;
        dassert(idleAgeTimeoutMillis > 0);
        return _idleAgeMillis > static_cast<unsigned>(idleAgeTimeoutMillis) && _pinValue == 0;
    }

    void ClientCursor::resetIdleAge() {
        _idleAgeMillis = 0;
    }

    /* called every 4 seconds.  millis is amount of idle time passed since the last call -- could be zero */
    void ClientCursor::idleTimeReport(unsigned millis) {
        LockedIterator i;
        unsigned sz = clientCursorsById.size();
        if (sz >= 100000) { 
            RATELIMITED(300000) log() << "warning number of open cursors is very large: " << sz << endl;
        }
        while (i.ok()) {
            ClientCursor *cc = i.current();
            if (cc->shouldTimeout(millis)) {
                LOG(1) << "killing old cursor " << cc->_cursorid << ' ' << cc->_ns
                       << " idle:" << cc->idleTime() << "ms" << endl;
                i.deleteAndAdvance();
            } else {
                i.advance();
            }
        }
    }

    void ClientCursor::LockedIterator::deleteAndAdvance() {
        ClientCursor *cc = current();
        CursorId id = cc->cursorid();
        delete cc;
        _i = clientCursorsById.upper_bound( id );
    }

    void ClientCursor::initCursorID() {
        {
            recursive_scoped_lock lock(ccmutex);
            _cursorid = allocCursorId_inlock();
            clientCursorsById.insert( make_pair(_cursorid, this) );
        }
        
        if (_partOfMultiStatementTxn) {
            transactions = cc().txnStack();
            // This cursor is now part of a multi-statement transaction and must be
            // closed before that txn commits or aborts. Note it in the rollback.
            ClientCursorRollback &rollback = cc().txn().clientCursorRollback();
            rollback.noteClientCursor(_cursorid);
        }
    }

    
    ClientCursor::ClientCursor(int queryOptions, const shared_ptr<Cursor>& c, const string& ns,
                               BSONObj query, const bool inMultiStatementTxn, bool createCursorID ) :
        _cursorid(INVALID_CURSOR_ID), _ns(ns), _db( cc().database() ),
        _c(c), _pos(0),
        _query(query),  _queryOptions(queryOptions),
        _idleAgeMillis(0), _pinValue(0),
        _partOfMultiStatementTxn(inMultiStatementTxn) {

        Lock::assertAtLeastReadLocked(ns);

        verify( _db );
        verify( str::startsWith(_ns, _db->name()) );
        if( queryOptions & QueryOption_NoCursorTimeout )
            noTimeout();
        
        if ( ! _c->modifiedKeys() ) {
            // store index information so we can decide if we can
            // get something out of the index key rather than full object

            int x = 0;
            BSONObjIterator i( _c->indexKeyPattern() );
            while ( i.more() ) {
                BSONElement e = i.next();
                if ( e.isNumber() ) {
                    // only want basic index fields, not "2d" etc
                    _indexedFields[e.fieldName()] = x;
                }
                x++;
            }
        }
        if (createCursorID) {
            initCursorID();
        }
    }

    ClientCursor::~ClientCursor() {
        if( _pos == -2 ) {
            // defensive: destructor called twice
            wassert(false);
            return;
        }

        if (_cursorid != INVALID_CURSOR_ID) {
            recursive_scoped_lock lock(ccmutex);

            clientCursorsById.erase(_cursorid);

            // defensive:
            _cursorid = INVALID_CURSOR_ID;
            _pos = -2;
            _pinValue = 0;
        }
    }

    bool ClientCursor::getFieldsDotted( const string& name, BSONElementSet &ret, BSONObj& holder ) {

        map<string,int>::const_iterator i = _indexedFields.find( name );
        if ( i == _indexedFields.end() ) {
            current().getFieldsDotted( name , ret );
            return false;
        }

        int x = i->second;

        holder = currKey();
        BSONObjIterator it( holder );
        while ( x && it.more() ) {
            it.next();
            x--;
        }
        verify( x == 0 );
        ret.insert( it.next() );
        return true;
    }

    BSONElement ClientCursor::getFieldDotted( const string& name , BSONObj& holder , bool * fromKey ) {

        map<string,int>::const_iterator i = _indexedFields.find( name );
        if ( i == _indexedFields.end() ) {
            if ( fromKey )
                *fromKey = false;
            holder = current();
            return holder.getFieldDotted( name );
        }
        
        int x = i->second;

        holder = currKey();
        BSONObjIterator it( holder );
        while ( x && it.more() ) {
            it.next();
            x--;
        }
        verify( x == 0 );

        if ( fromKey )
            *fromKey = true;
        return it.next();
    }

    BSONObj ClientCursor::extractFields(const BSONObj &pattern , bool fillWithNull ) {
        BSONObjBuilder b( pattern.objsize() * 2 );

        BSONObj holder;
     
        BSONObjIterator i( pattern ); 
        while ( i.more() ) {
            BSONElement key = i.next();
            BSONElement value = getFieldDotted( key.fieldName() , holder );

            if ( value.type() ) {
                b.appendAs( value , key.fieldName() );
                continue;
            }

            if ( fillWithNull ) 
                b.appendNull( key.fieldName() );
            
        }

        return b.obj();
    }
    
    BSONObj ClientCursor::extractKey( const KeyPattern& usingKeyPattern ) const {
        KeyPattern currentIndex( _c->indexKeyPattern() );
        if ( usingKeyPattern.isCoveredBy( currentIndex ) && ! currentIndex.isSpecial() ){
            BSONObj currKey = _c->currKey();
            BSONObj prettyKey = currKey.replaceFieldNames( currentIndex.toBSON() );
            return usingKeyPattern.extractSingleKey( prettyKey );
        }
        return usingKeyPattern.extractSingleKey( _c->current() );
    }

    void ClientCursor::fillQueryResultFromObj( BufBuilder &b, const MatchDetails* details ) const {
        const Projection::KeyOnly *keyFieldsOnly = c()->keyFieldsOnly();
        if ( keyFieldsOnly ) {
            mongo::fillQueryResultFromObj( b, 0, keyFieldsOnly->hydrate( c()->currKey(), c()->currPK() ), details );
        }
        else {
            mongo::fillQueryResultFromObj( b, fields.get(), c()->current(), details );
        }
    }

    namespace {
        // so we don't have to do find() which is a little slow very often.
        long long cursorGenTSLast = 0;
        PseudoRandom* cursorGenRandom = NULL;
    }

    long long ClientCursor::allocCursorId_inlock() {
        // It is important that cursor IDs not be reused within a short period of time.

        if ( ! cursorGenRandom ) {
            scoped_ptr<SecureRandom> sr( SecureRandom::create() );
            cursorGenRandom = new PseudoRandom( sr->nextInt64() );
        }

        const long long ts = Listener::getElapsedTimeMillis();

        long long x;

        while ( 1 ) {
            x = ts << 32;
            x |= cursorGenRandom->nextInt32();

            if ( x == 0 )
                continue;

            if ( x < 0 )
                x *= -1;

            if ( ts != cursorGenTSLast || ClientCursor::find_inlock(x, false) == 0 )
                break;
        }

        cursorGenTSLast = ts;

        return x;
    }

    void ClientCursor::storeOpForSlave( BSONObj curr ) {
        if ( ! ( _queryOptions & QueryOption_OplogReplay ))
            return;

        BSONElement e = curr["_id"];
        if ( e.type() == BinData ) {
            _slaveReadTill = getGTIDFromBSON("_id", curr);
        }
    }

    void ClientCursor::updateSlaveLocation( CurOp& curop ) {
        if ( _slaveReadTill.isInitial() )
            return;
        mongo::updateSlaveLocation( curop , _ns.c_str() , _slaveReadTill );
    }

    void ClientCursor::appendStats( BSONObjBuilder& result ) {
        recursive_scoped_lock lock(ccmutex);
        result.appendNumber("totalOpen", clientCursorsById.size() );
        result.appendNumber("clientCursors_size", (int) numCursors());
        result.appendNumber("timedOut" , numberTimedOut);
        unsigned pinned = 0;
        unsigned notimeout = 0;
        for ( CCById::iterator i = clientCursorsById.begin(); i != clientCursorsById.end(); i++ ) {
            unsigned p = i->second->_pinValue;
            if( p >= 100 )
                pinned++;
            else if( p > 0 )
                notimeout++;
        }
        if( pinned ) 
            result.append("pinned", pinned);
        if( notimeout )
            result.append("totalNoTimeout", notimeout);
    }

    // QUESTION: Restrict to the namespace from which this command was issued?
    // Alternatively, make this command admin-only?
    class CmdCursorInfo : public WebInformationCommand {
    public:
        CmdCursorInfo() : WebInformationCommand("cursorInfo") {}
        virtual void help( stringstream& help ) const {
            help << " example: { cursorInfo : 1 }";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::cursorInfo);
            out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
        }
        bool run(const string& dbname, BSONObj& jsobj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            ClientCursor::appendStats( result );
            return true;
        }
    } cmdCursorInfo;

    class CursorServerStats : public ServerStatusSection {
    public:

        CursorServerStats() : ServerStatusSection( "cursors" ){}
        virtual bool includeByDefault() const { return true; }

        BSONObj generateSection(const BSONElement& configElement) const {
            BSONObjBuilder b;
            ClientCursor::appendStats( b );
            return b.obj();
        }

    } cursorServerStats;

    /** thread for timing out old cursors */
    void ClientCursorMonitor::run() {
        Client::initThread("clientcursormon");
        Client& client = cc();
        Timer t;
        const int Secs = 4;
        while ( ! inShutdown() ) {
            ClientCursor::idleTimeReport( t.millisReset() );
            sleepsecs(Secs);
        }
        client.shutdown();
    }

    void ClientCursor::find( const string& ns , set<CursorId>& all ) {
        recursive_scoped_lock lock(ccmutex);

        for ( CCById::iterator i=clientCursorsById.begin(); i!=clientCursorsById.end(); ++i ) {
            if ( i->second->_ns == ns )
                all.insert( i->first );
        }
    }

    bool ClientCursor::_erase_inlock(ClientCursor* cursor) {
        // Must not have an active ClientCursor::Pin.
        massert( 16089,
                str::stream() << "Cannot kill active cursor " << cursor->cursorid(),
                cursor->_pinValue < 100 );

        delete cursor;
        return true;
    }

    bool ClientCursor::erase(CursorId id) {
        recursive_scoped_lock lock(ccmutex);
        ClientCursor* cursor = find_inlock(id);
        if (!cursor) {
            return false;
        }

        return _erase_inlock(cursor);
    }

    bool ClientCursor::eraseIfAuthorized(CursorId id) {
        NamespaceString ns;
        {
            recursive_scoped_lock lock(ccmutex);
            ClientCursor* cursor = find_inlock(id);
            if (!cursor) {
                audit::logKillCursorsAuthzCheck(
                        &cc(),
                        NamespaceString(),
                        id,
                        ErrorCodes::CursorNotFound);
                return false;
            }
            ns = NamespaceString(cursor->ns());
        }

        // Can't be in a lock when checking authorization
        const bool isAuthorized = cc().getAuthorizationSession()->isAuthorizedForActionsOnNamespace(
                ns, ActionType::killCursors);
        audit::logKillCursorsAuthzCheck(
                &cc(),
                ns,
                id,
                isAuthorized ? ErrorCodes::OK : ErrorCodes::Unauthorized);
        if (!isAuthorized) {
            return false;
        }

        // It is safe to lookup the cursor again after temporarily releasing the mutex because
        // of 2 invariants: that the cursor ID won't be re-used in a short period of time, and that
        // the namespace associated with a cursor cannot change.
        recursive_scoped_lock lock(ccmutex);
        ClientCursor* cursor = find_inlock(id);
        if (!cursor) {
            // Cursor was deleted in another thread since we found it earlier in this function.
            return false;
        }
        if (ns != cursor->ns()) {
            warning() << "Cursor namespace changed. Previous ns: " << ns << ", current ns: "
                    << cursor->ns() << endl;
            return false;
        }

        return _erase_inlock(cursor);
    }

    int ClientCursor::erase(int n, long long *ids) {
        int found = 0;
        for ( int i = 0; i < n; i++ ) {
            if ( erase(ids[i]))
                found++;

            if ( inShutdown() )
                break;
        }
        return found;
    }

    int ClientCursor::eraseIfAuthorized(int n, long long *ids) {
        int found = 0;
        for ( int i = 0; i < n; i++ ) {
            if ( eraseIfAuthorized(ids[i]))
                found++;

            if ( inShutdown() )
                break;
        }
        return found;
    }

    bool ClientCursor::checkMultiStatementTxn() {
        verify(transactions.get() != NULL);
        if (_partOfMultiStatementTxn) {
            uassert(16811, "Cannot use a client cursor belonging to a different multi-statement transaction",
                       cc().txnStack() == transactions);
        }
        return _partOfMultiStatementTxn;
    }

    ClientCursorMonitor clientCursorMonitor;

} // namespace mongo
