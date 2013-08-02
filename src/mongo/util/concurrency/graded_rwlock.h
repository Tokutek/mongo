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
        using RWLock::lock;
        using RWLock::unlock;
    };

} // namespace mongo
