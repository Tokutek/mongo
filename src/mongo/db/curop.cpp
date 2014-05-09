/**
*    Copyright (C) 2009 10gen Inc.
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
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#include "mongo/pch.h"

#include "mongo/base/counter.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/db/curop.h"
#include "mongo/db/database.h"

namespace mongo {

    // todo : move more here

    CurOp::CurOp( Client * client , CurOp * wrapped ) : 
        _client(client), 
        _wrapped(wrapped) 
    {
        if ( _wrapped )
            _client->_curOp = this;
        _start = 0;
        _active = false;
        _reset();
        _op = 0;
        _ns.clear();
    }

    void CurOp::_reset() {
        _suppressFromCurop = false;
        _command = false;
        _dbprofile = 0;
        _end = 0;
        _message = "";
        _progressMeter.finished();
        _killed = false;
        _expectedLatencyMs = 0;
        _lockStat.reset();
    }

    void CurOp::reset() {
        _reset();
        _start = 0;
        _opNum = _nextOpNum++;
        _ns.clear();
        _debug.reset();
        _query.reset();
        _active = true; // this should be last for ui clarity
    }

    void CurOp::reset( const HostAndPort& remote, int op ) {
        reset();
        if( _remote != remote ) {
            // todo : _remote is not thread safe yet is used as such!
            _remote = remote;
        }
        _op = op;
    }
        
    ProgressMeter& CurOp::setMessage(const char *msg,
                                     const std::string &name,
                                     unsigned long long progressMeterTotal,
                                     int secondsBetween) {
        if ( progressMeterTotal ) {
            if ( _progressMeter.isActive() ) {
                cout << "about to assert, old _message: " << _message << " new message:" << msg << endl;
                verify( ! _progressMeter.isActive() );
            }
            _progressMeter.reset( progressMeterTotal , secondsBetween );
            _progressMeter.setName(name);
        }
        else {
            _progressMeter.finished();
        }
        _message = msg;
        return _progressMeter;
    }

    CurOp::~CurOp() {
        if ( _wrapped ) {
            scoped_lock bl(Client::clientsMutex);
            _client->_curOp = _wrapped;
        }
        _client = 0;
    }

    void CurOp::ensureStarted() {
        if ( _start == 0 )
            _start = curTimeMicros64();
    }

    void CurOp::enter( Client::Context * context ) {
        ensureStarted();
        _ns = context->ns();
        _dbprofile = std::max( context->_db ? context->_db->profile() : 0 , _dbprofile );
    }
    
    void CurOp::leave( Client::Context * context ) {
    }

    void CurOp::recordGlobalTime( long long micros ) const {
        if ( _client ) {
            const LockState& ls = _client->lockState();
            verify( ls.threadState() );
            Top::global.record( _ns , _op , ls.hasAnyWriteLock() ? 1 : -1 , micros , _command );
        }
    }

    BSONObj CurOp::info() {
        BSONObjBuilder b;
        b.append("opid", _opNum);
        bool a = _active && _start;
        b.append("active", a);

        if( a ) {
            b.append("secs_running", elapsedSeconds() );
        }

        b.append( "op" , opToString( _op ) );

        b.append("ns", _ns);

        if (_op == dbInsert) {
            _query.append(b, "insert");
        }
        else {
            _query.append( b , "query" );
        }

        if( !_remote.empty() ) {
            b.append("client", _remote.toString());
        }

        if ( _client ) {
            b.append( "desc" , _client->desc() );
            if ( _client->_threadId.size() ) 
                b.append( "threadId" , _client->_threadId );
            if ( _client->_connectionId )
                b.appendNumber( "connectionId" , _client->_connectionId );
            b.appendNumber( "rootTxnid" , _client->rootTransactionId() );
            _client->_ls.reportState(b);
        }
        
        if ( ! _message.empty() ) {
            if ( _progressMeter.isActive() ) {
                StringBuilder buf;
                buf << _message.toString() << " " << _progressMeter.toString();
                b.append( "msg" , buf.str() );
                BSONObjBuilder sub( b.subobjStart( "progress" ) );
                sub.appendNumber( "done" , (long long)_progressMeter.done() );
                sub.appendNumber( "total" , (long long)_progressMeter.total() );
                sub.done();
            }
            else {
                b.append( "msg" , _message.toString() );
            }
        }

        if( killed() ) 
            b.append("killed", true);
        
        b.append( "lockStats" , _lockStat.report() );

        return b.obj();
    }

    AtomicUInt CurOp::_nextOpNum;

    static Counter64 returnedCounter;
    static Counter64 insertedCounter;
    static Counter64 updatedCounter;
    static Counter64 deletedCounter;
    static Counter64 scannedCounter;

    static ServerStatusMetricField<Counter64> displayReturned( "document.returned", &returnedCounter );
    static ServerStatusMetricField<Counter64> displayUpdated( "document.updated", &updatedCounter );
    static ServerStatusMetricField<Counter64> displayInserted( "document.inserted", &insertedCounter );
    static ServerStatusMetricField<Counter64> displayDeleted( "document.deleted", &deletedCounter );
    static ServerStatusMetricField<Counter64> displayScanned( "queryExecutor.scanned", &scannedCounter );

    static Counter64 idhackCounter;
    static Counter64 scanAndOrderCounter;
    static Counter64 fastmodCounter;

    static ServerStatusMetricField<Counter64> displayIdhack( "operation.idhack", &idhackCounter );
    static ServerStatusMetricField<Counter64> displayScanAndOrder( "operation.scanAndOrder", &scanAndOrderCounter );

    void OpDebug::recordStats() {
        if ( nreturned > 0 )
            returnedCounter.increment( nreturned );
        if ( ninserted > 0 )
            insertedCounter.increment( ninserted );
        if ( nupdated > 0 )
            updatedCounter.increment( nupdated );
        if ( ndeleted > 0 )
            deletedCounter.increment( ndeleted );
        if ( nscanned > 0 )
            scannedCounter.increment( nscanned );

        if ( idhack )
            idhackCounter.increment();
        if ( scanAndOrder )
            scanAndOrderCounter.increment();
    }
}
