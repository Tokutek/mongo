// lockstate.h

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


#pragma once

#include "mongo/db/d_concurrency.h"
#include "mongo/platform/atomic_word.h"

namespace mongo {

    class Acquiring;

    // per thread
    class LockState {
    public:
        LockState();

        void dump();
        static void Dump(); 
        BSONObj reportState();
        void reportState(BSONObjBuilder& b);
        
        unsigned recursiveCount() const { return _recursive; }

        /**
         * @return 0 rwRW
         */
        char threadState() const { return _threadState; }
        
        bool isRW() const; // RW
        bool isW() const; // W
        bool hasAnyReadLock() const; // explicitly rR
        bool hasAnyWriteLock() const; // wWX
        
        bool isLocked( const StringData& ns ); // rwRW

        /** pending means we are currently trying to get a lock */
        bool hasLockPending() const { return _lockPending || _lockPendingParallelWriter; }

        // ----


        void lockedStart( char newState ); // RWrw
        void unlocked(); // _threadState = 0
        
        /**
         * you have to be locked already to call this
         * this is mostly for W_to_R or R_to_W
         */
        void changeLockState( char newstate );

        bool adminLocked() {
            return _adminLockCount != 0;
        }
        bool localLocked() {
            return _localLockCount != 0;
        }
        int otherCount() const { return _otherCount; }
        const string& otherName() const { return _otherName; }
        WrapperForRWLock* otherLock() const { return _otherLock; }
        
        void enterScopedLock( Lock::ScopedLock* lock );
        Lock::ScopedLock* leaveScopedLock();

        void lockedAdmin(int type, const string &context);
        void lockedLocal(int type, const string &context);
        void unlockedAdmin();
        void unlockedLocal();
        void lockedOther( const StringData& db , int type , WrapperForRWLock* lock, const string &context );
        void lockedOther( int type, const string &context );  // "same lock as last time" case
        void unlockedOther();

        LockStat* getRelevantLockStat();
        void recordLockTime() { _scopedLk->recordTime(); }
        void resetLockTime() { _scopedLk->resetTime(); }

        const string &context() const {
            dassert(_context != NULL);
            return *_context;
        }
        
    private:
        unsigned _recursive;           // we allow recursively asking for a lock; we track that here

        // global lock related
        char _threadState;             // 0, 'r', 'w', 'R', 'W'

        int _adminLockCount; // if non-zero, that means admin is locked
        int _localLockCount; // if non-zero, that means local is locked
        
        int _otherCount;               //   >0 means write lock, <0 read lock - XXX change name
        string _otherName;             // which database are we locking and working with (besides local/admin) 
        WrapperForRWLock* _otherLock;  // so we don't have to check the map too often (the map has a mutex)

        // for the nonrecursive case. otherwise there would be many
        // the first lock goes here
        Lock::ScopedLock* _scopedLk;   
        
        bool _lockPending;
        bool _lockPendingParallelWriter;

        const string *_context;

        friend class Acquiring;
        friend class AcquiringParallelWriter;
    };

    class WrapperForRWLock : boost::noncopyable { 
        SimpleRWLock r;
        AtomicInt64 _writeLockWaiters;
    public:
        string name() const { return r.name; }
        LockStat stats;
        WrapperForRWLock(const StringData& name) : r(name), _writeLockWaiters(0) { }
        void lock() {
            class WaiterManager : boost::noncopyable {
                AtomicInt64 &_val;
              public:
                WaiterManager(AtomicInt64 &val) : _val(val) {
                    _val.addAndFetch(1);
                }
                ~WaiterManager() {
                    _val.subtractAndFetch(1);
                }
            } wm(_writeLockWaiters);
            r.lock();
        }
        void lock_shared()   { r.lock_shared(); }
        void unlock()        { r.unlock(); }
        void unlock_shared() { r.unlock_shared(); }
        long long writeLockWaiters() const {
            return _writeLockWaiters.loadRelaxed();
        }
    };

    class ScopedLock;

    class Acquiring {
    public:
        Acquiring( Lock::ScopedLock* lock, LockState& ls );
        ~Acquiring();
    private:
        Lock::ScopedLock* _lock;
        LockState& _ls;
    };
        
    class AcquiringParallelWriter {
    public:
        AcquiringParallelWriter( LockState& ls );
        ~AcquiringParallelWriter();

    private:
        LockState& _ls;
    };


}
