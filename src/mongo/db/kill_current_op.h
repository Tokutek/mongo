
/*
 *    Copyright (C) 2010 10gen Inc.
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


#pragma once

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include "mongo/bson/util/atomic_int.h"
#include "mongo/db/client.h"

namespace mongo {

    /* _globalKill: we are shutting down
       otherwise kill attribute set on specified CurOp
       this class does not handle races between interruptJs and the checkForInterrupt functions - those must be
       handled by the client of this class
    */
    extern class KillCurrentOp {
    public:
        void killAll();
        void kill(AtomicUInt i);

        /** @return true if global interrupt and should terminate the operation */
        bool globalInterruptCheck() const { return _globalKill; }

        void checkForInterrupt();
        void checkForInterrupt( Client &c );

        /** @return "" if not interrupted.  otherwise, you should stop. */
        const char *checkForInterruptNoAssert();
        const char *checkForInterruptNoAssert(Client &c);

        // increments _killForTransition, thereby making
        // checkForInterrupt uassert and kill operations
        void killForTransition() {
            boost::unique_lock<boost::mutex> lock(_transitionLock);
            dassert(_killForTransition >= 0);
            _killForTransition++;
        }
        // decrements _killForTransition, thereby reallowing
        // operations to complete successfully
        void transitionComplete() {
            boost::unique_lock<boost::mutex> lock(_transitionLock);
            dassert(_killForTransition > 0);
            _killForTransition--;
        }

        /** Reset the object to its initial state.  Only for testing. */
        void reset();

    private:
        void interruptJs( AtomicUInt *op );
        void _checkForInterrupt( Client &c );
        volatile bool _globalKill;
        // number of threads that want operations killed
        // because there will be a state transition
        volatile uint32_t _killForTransition;
        // protects _killForTransition variabl
        boost::mutex _transitionLock;
    } killCurrentOp;

    class NoteStateTransition : boost::noncopyable {
        bool _noted;
      public:
        NoteStateTransition() {
            killCurrentOp.killForTransition();
            _noted = true;
        }
        ~NoteStateTransition() {
            if (_noted) {
                killCurrentOp.transitionComplete();
            }
            _noted = false;
        }
        void noteTransitionComplete() {
            verify(_noted);
            killCurrentOp.transitionComplete();
            _noted = false;
        }
    };

}
