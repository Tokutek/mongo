// lockstate.cpp

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

#include "mongo/db/lockstate.h"

#include "mongo/db/d_concurrency.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/client.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    LockState::LockState() 
        : _recursive(0),
          _threadState(0),
          _adminLockCount(0),
          _localLockCount(0),
          _otherCount(0), 
          _otherLock(NULL),
          _scopedLk(NULL),
          _lockPending(false),
          _lockPendingParallelWriter(false),
          _context(NULL) {
    }

    bool LockState::isRW() const { 
        return _threadState == 'R' || _threadState == 'W'; 
    }

    bool LockState::isW() const { 
        return _threadState == 'W'; 
    }

    bool LockState::hasAnyReadLock() const { 
        return _threadState == 'r' || _threadState == 'R';
    }

    bool LockState::hasAnyWriteLock() const { 
        return _threadState == 'w' || _threadState == 'W';
    }

    bool LockState::isLocked( const StringData& ns ) {
        char db[MaxDatabaseNameLen];
        nsToDatabase(ns, db);
        
        DEV verify( _otherName.find( '.' ) == string::npos ); // XXX this shouldn't be here, but somewhere
        if ( _otherCount && db == _otherName )
            return true;

        if ( _localLockCount && mongoutils::str::equals( db , "local" ) )
            return true;
        if ( _adminLockCount && mongoutils::str::equals( db , "admin" ) )
            return true;

        return false;
    }

    void LockState::lockedStart( char newState ) {
        _threadState = newState;
    }
    void LockState::unlocked() {
        _threadState = 0;
    }

    void LockState::changeLockState( char newState ) {
        fassert( 16169 , _threadState != 0 );
        _threadState = newState;
    }

    static string kind(int n) { 
        if( n > 0 )
            return "W";
        if( n < 0 ) 
            return "R";
        return "?";
    }

    BSONObj LockState::reportState() {
        BSONObjBuilder b;
        reportState( b );
        return b.obj();
    }
    
    /** Note: this is is called by the currentOp command, which is a different 
              thread. So be careful about thread safety here. For example reading 
              this->otherName would not be safe as-is!
    */
    void LockState::reportState(BSONObjBuilder& res) {
        BSONObjBuilder b;
        if( _threadState ) {
            char buf[2];
            buf[0] = _threadState; 
            buf[1] = 0;
            b.append("^", buf);
        }
        if (_adminLockCount) {
            b.append("^admin", kind(_adminLockCount));
        }
        if (_localLockCount) {
            b.append("^local", kind(_localLockCount));
        }
        if( _otherCount ) { 
            WrapperForRWLock *k = _otherLock;
            if( k ) {
                string s = "^";
                s += k->name();
                b.append(s, kind(_otherCount));
            }
        }
        BSONObj o = b.obj();
        if( !o.isEmpty() ) 
            res.append("locks", o);
        // this may be a racy read, but that is ok
        // we use a local parameter so we don't
        // need to worry about _context becoming NULL
        // in between the if check and the append
        const string* c = _context;
        if (c) {
            res.append("context", *c);
        }
        res.append("waitingForLock", _lockPending);
    }

    void LockState::Dump() {
        cc().lockState().dump();
    }
    void LockState::dump() {
        char s = _threadState;
        stringstream ss;
        ss << "lock status: ";
        if( s == 0 ){
            ss << "unlocked"; 
        }
        else {
            ss << s;
            if( _recursive ) { 
                ss << " recursive:" << _recursive;
            }
            ss << " otherCount:" << _otherCount;
            if( _otherCount ) {
                ss << " otherdb:" << _otherName;
            }
            if (_adminLockCount) {
                ss << " adminLockCount:" << _adminLockCount;
            }
            if (_localLockCount) {
                ss << " localLockCount:" << _localLockCount;
            }
        }
        log() << ss.str() << endl;
    }

    void LockState::enterScopedLock( Lock::ScopedLock* lock ) {
        _recursive++;
        if ( _recursive == 1 ) {
            fassert(16115, _scopedLk == 0);
            _scopedLk = lock;
        }
    }

    Lock::ScopedLock* LockState::leaveScopedLock() {
        _recursive--;
        dassert( _recursive < 10000 );
        Lock::ScopedLock* temp = _scopedLk;

        if ( _recursive > 0 ) {
            return NULL;
        }
        
        _scopedLk = NULL;
        return temp;
    }

    void LockState::lockedAdmin(int type, const string &context) {
        _adminLockCount += type;
        _context = &context;
    }

    void LockState::lockedLocal(int type, const string &context) {
        _localLockCount += type;
        _context = &context;
    }

    void LockState::unlockedAdmin() {
        _adminLockCount = 0;
        _context = NULL;
    }

    void LockState::unlockedLocal() {
        _localLockCount = 0;
        _context = NULL;
    }

    void LockState::lockedOther( int type, const string &context ) {
        fassert( 16231 , _otherCount == 0 );
        _otherCount = type;
        _context = &context;
    }

    void LockState::lockedOther( const StringData& other , int type , WrapperForRWLock* lock, const string &context  ) {
        fassert( 16170 , _otherCount == 0 );
        _otherName = other.toString();
        _otherCount = type;
        _otherLock = lock;
        _context = &context;
    }

    void LockState::unlockedOther() {
        // we leave _otherName and _otherLock set as
        // _otherLock exists to cache a pointer
        _otherCount = 0;
        _context = NULL;
    }

    LockStat* LockState::getRelevantLockStat() {
        // this requires further review. In mongodb
        // one can never have both admin and local locked
        // whereas with TokuMX we can. If both are locked,
        // not sure which should be returned.
        // going with local for now
        if (_localLockCount) {
            return Lock::nestableLockStat(Lock::local);
        }
        if (_adminLockCount) {
            return Lock::nestableLockStat(Lock::admin);
        }
        if (  _otherCount && _otherLock  )
            return &_otherLock->stats;
        
        if ( isRW() ) 
            return Lock::globalLockStat();

        return 0;
    }


    Acquiring::Acquiring( Lock::ScopedLock* lock,  LockState& ls )
        : _lock( lock ), _ls( ls ){
        _ls._lockPending = true;
    }

    Acquiring::~Acquiring() {
        _ls._lockPending = false;
        LockStat* stat = _ls.getRelevantLockStat();
        if ( stat && _lock ) {
            // increment the global stats for this counter
            stat->recordAcquireTimeMicros( _ls.threadState(), _lock->acquireFinished( stat ) );
        }
    }
    
    AcquiringParallelWriter::AcquiringParallelWriter( LockState& ls )
        : _ls( ls ) {
        _ls._lockPendingParallelWriter = true;
    }
    
    AcquiringParallelWriter::~AcquiringParallelWriter() {
        _ls._lockPendingParallelWriter = false;
    }

}
