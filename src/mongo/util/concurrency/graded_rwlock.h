// @file graded_rwlock.h a graded rwlock

/*
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

/**
 * With fair rwlocks, there is a problem where even if all write operations are cheap, if a read
 * operation is expensive and a write comes after it, all other readers block behind the writer in
 * the queue.  This effectively turns the expensive read operation into an expensive write operation
 * during which no other clients can make progress.
 *
 * The graded rwlock solves this problem by allowing an expensive read operation, if it knows it
 * will take a long time, to put itself in a different class from the cheap readers.  Thus a write
 * operation first waits for the expensive lock (and blocks other expensive readers), but during
 * this period other cheap readers can still make progress.  Only once the writer has the expensive
 * lock does it try for the cheap lock, which it should get quickly because the other readers are
 * cheap and the lock is fair.
 */

#include "mongo/pch.h"

#include <boost/static_assert.hpp>

#include "mongo/util/assert_util.h"
#include "mongo/util/concurrency/rwlock.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    /**
     * A GradedRWLock<N> supports N levels of cost for readers to declare.
     *
     * Readers declare at what cost level they claim to be when they take the lock.  The cheapest
     * level is 1, the most expensive level is N.  For example, a medium cost reader might take
     * the lock like this:
     *
     *     typedef GradedRWLock<3> TrioLock;
     *     void foo(TrioLock &lock) {
     *         TrioLock::shared_lock<2> lk(lock);
     *         ...;
     *     }
     *
     * A writer always takes all the locks, starting with the most expensive lock and working down.
     */
    template <int N>
    class GradedRWLock : private RWLock {
        typedef GradedRWLock<N> lock_type;
        typedef GradedRWLock<N-1> cheaper_lock_type;

        cheaper_lock_type _parent;

      public:
        GradedRWLock(const char *name) : RWLock(((string)(mongoutils::str::stream() << name << "_" << N)).c_str()), _parent(name) {}

        inline void lock_shared(int level) {
            dassert(level > 0);
            dassert(level <= N);
            if (level == N) {
                RWLock::lock_shared();
            }
            else {
                _parent.lock_shared(level);
            }
        }
        inline void unlock_shared(int level) {
            dassert(level > 0);
            dassert(level <= N);
            if (level == N) {
                RWLock::unlock_shared();
            }
            else {
                _parent.unlock_shared(level);
            }
        }

        // We want lock level 2 to be for more expensive things than lock level 1.  So when we try
        // to lock GradedRWLock<2> at both levels, we have to take the expensive one (2) before the
        // cheaper one (1).  _parent is the cheaper lock, so we lock ourselves, then our parent.
        inline void lock() {
            RWLock::lock();
            _parent.lock();
        }
        inline void unlock() {
            _parent.unlock();
            RWLock::unlock();
        }

        // We may want to, while we have the exclusive lock held, temporarily release some of the
        // cheaper locks (those with level <= maxLevel).  We unlock the cheapest ones first, then
        // the more expensive ones, and when we want to relock them, we lock the more expensive ones
        // before the cheaper ones.  See exclusive_lock::release_cheap_locks.
        inline void unlock_cheap(int maxLevel) {
            dassert(maxLevel > 0);
            dassert(maxLevel <= N);
            _parent.unlock_cheap(maxLevel);
            if (maxLevel >= N) {
                RWLock::unlock();
            }
        }
        inline void lock_cheap(int maxLevel) {
            dassert(maxLevel > 0);
            dassert(maxLevel <= N);
            if (maxLevel >= N) {
                RWLock::lock();
            }
            _parent.lock_cheap(maxLevel);
        }

        template<int level>
        class shared_lock : boost::noncopyable {
            lock_type &_r;
          public:
            shared_lock(lock_type &rwlock) : _r(rwlock) { _r.lock_shared(level); }
            ~shared_lock() { _r.unlock_shared(level); }
        };

        class exclusive_lock : boost::noncopyable {
            lock_type &_r;
          public:
            exclusive_lock(lock_type &rwlock) : _r(rwlock) { _r.lock(); }
            ~exclusive_lock() { _r.unlock(); }

            /**
             * Once we have the exclusive lock, we might want to temporarily let cheaper operations
             * through for a while, without releasing the more expensive locks just yet.  This lets
             * us do that:
             *
             *     typedef GradedRWLock<3> TrioLock;
             *     {
             *         TrioLock::exclusive_lock lk(tl);
             *         // Do some things here
             *         {
             *             TrioLock::exclusive_lock::release_cheap_locks<2> rel(lk);
             *             // do some things that can allow cheap operations through
             *         }
             *         // Now we have the exclusive lock again, and we didn't have to wait for the
             *         // most expensive lock again
             *     }
             */
            template<int level>
            class release_cheap_locks : boost::noncopyable {
                lock_type &_r;
              public:
                release_cheap_locks(exclusive_lock &lk) : _r(lk._r) { _r.unlock_cheap(level); }
                ~release_cheap_locks() { _r.lock_cheap(level); }
            };
        };
    };

    template<>
    class GradedRWLock<1> : private RWLock {
      public:
        GradedRWLock(const char *name) : RWLock(((string)(mongoutils::str::stream() << name << "_" << 1)).c_str()) {}
        inline void lock_shared(int level) {
            dassert(level == 1);
            RWLock::lock_shared();
        }
        inline void unlock_shared(int level) {
            dassert(level == 1);
            RWLock::unlock_shared();
        }
        inline void unlock_cheap(int maxLevel) {
            if (maxLevel >= 1) {
                unlock();
            }
        }
        inline void lock_cheap(int maxLevel) {
            if (maxLevel >= 1) {
                lock();
            }
        }
        using RWLock::lock;
        using RWLock::unlock;
    };

} // namespace mongo
