/**
*    Copyright (C) 2009 10gen Inc.
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

#include "mongo/db/kill_current_op.h"

#include <set>

#include "mongo/bson/util/atomic_int.h"
#include "mongo/db/client.h"
#include "mongo/db/curop.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/lockstate.h"
#include "mongo/db/relock.h"
#include "mongo/scripting/engine.h"
#include "mongo/util/concurrency/qlock.h"

namespace mongo {

    void KillCurrentOp::interruptJs( AtomicUInt *op ) {
        if ( !globalScriptEngine )
            return;
        if ( !op ) {
            globalScriptEngine->interruptAll();
        }
        else {
            globalScriptEngine->interrupt( *op );
        }
    }

    void KillCurrentOp::killAll() {
        _globalKill = true;
        interruptJs( 0 );
    }

    void KillCurrentOp::kill(AtomicUInt i) {
        bool found = false;
        {
            scoped_lock l( Client::clientsMutex );
            for( set< Client* >::const_iterator j = Client::clients.begin(); !found && j != Client::clients.end(); ++j ) {
                for( CurOp *k = ( *j )->curop(); !found && k; k = k->parent() ) {
                    if ( k->opNum() == i ) {
                        k->kill();
                        for( CurOp *l = ( *j )->curop(); l != k; l = l->parent() ) {
                            l->kill();
                        }
                        found = true;
                    }
                }
            }
        }
        if ( found ) {
            interruptJs( &i );
        }
    }

    void KillCurrentOp::checkForInterrupt() {
        return _checkForInterrupt( cc() );
    }

    void KillCurrentOp::checkForInterrupt( Client &c ) {
        return _checkForInterrupt( c );
    }

    static bool shouldYieldForWriteLock(Client &c) {
        LockState &ls = c.lockState();
        if (ls.threadState() == 'R' || ls.threadState() == 'r') {
            if (Lock::globalWriteLockWaiters() > 0) {
                return true;
            }
        }
        if (ls.threadState() == 'r') {
            if (ls.adminLocked()) {
                if (Lock::nestableWriteLockWaiters(Lock::admin) > 0) {
                    return true;
                }
            }
            if (ls.localLocked()) {
                if (Lock::nestableWriteLockWaiters(Lock::local) > 0) {
                    return true;
                }
            }
            if (ls.otherLock() != NULL) {
                if (ls.otherLock()->writeLockWaiters() > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    void KillCurrentOp::_checkForInterrupt( Client &c ) {
        if (!c.globallyUninterruptible()) {
            if (_killForTransition > 0) {
                uasserted(16809, "interrupted due to state transition");
            }
            if( _globalKill ) {
                uasserted(11600,"interrupted at shutdown");
            }
        }
        if( c.curop()->killed() ) {
            uasserted(11601,"operation was interrupted");
        }
        if (c.isYieldingToWriteLock() && shouldYieldForWriteLock(c)) {
            uasserted(17352, "interrupted by write lock, try this operation again");
        }
    }
    
    const char * KillCurrentOp::checkForInterruptNoAssert() {
        Client& c = cc();
        return checkForInterruptNoAssert(c);
    }

    const char * KillCurrentOp::checkForInterruptNoAssert(Client &c) {
        if (_killForTransition > 0) {
            return "interrupted due to state transition";
        }
        if( _globalKill )
            return "interrupted at shutdown";
        if( c.curop()->killed() )
            return "interrupted";

        if (c.isYieldingToWriteLock() && shouldYieldForWriteLock(c)) {
            return "interrupted by write lock";
        }
        return "";
    }

    void KillCurrentOp::reset() {
        _globalKill = false;
    }
}
