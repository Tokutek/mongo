// @file partitioned_counter.h

/*    Copyright (C) 2013 Tokutek Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#pragma once

#include "mongo/pch.h"

#include <limits>
#include <list>
#include <boost/thread/tss.hpp>

#include "mongo/util/assert_util.h"
#include "mongo/util/concurrency/mutex.h"

namespace mongo {

    using boost::thread_specific_ptr;
    using std::list;

    /**
     * PartitionedCounter is a number that can be incremented, decremented, and read.
     *
     * It is assumed that increments and decrements are frequent and concurrent, whereas reads are
     * infrequent.  Therefore, the key is to do increments and decrements without involving a memory
     * location shared between threads.
     *
     * The original implementation is in ft-index under util/partitioned_counter.{h,cc}.  This is a
     * C++ implementation that should be friendlier to mongo code.  In addition, it has the
     * following differences:
     *
     *   - Signed or unsigned types are supported, because it's templated.
     *   - Decrement is supported, and if Value is an unsigned type, decrements check for underflow.
     *   - There is no global cleanup like partitioned_counters_destroy, if there are global objects
     *     they get destructed just like everything else.  This is possible because there is no
     *     global state.
     *   - This implementation is expected to be slower.  boost::thread_specific_ptr is known to be
     *     slow, but in the original implementation we rolled our own pthread keys, so maybe this
     *     isn't so bad.  The linked list removal on thread destruction is slower because the
     *     element doesn't know where in the list it is.  This could be made better if it is shown
     *     to stall other threads.
     */
    template<typename Value>
    class PartitionedCounter : boost::noncopyable {
      public:
        PartitionedCounter(Value init=0);
        ~PartitionedCounter();

        PartitionedCounter& inc(Value x=1);
        PartitionedCounter& dec(Value x=1);

        Value get() const;

        // convenience API

        operator Value() const { return get(); }

        // prefix
        PartitionedCounter& operator++() { return inc(1); }
        PartitionedCounter& operator--() { return dec(1); }

        // maybe TODO: postfix (can't do because we'd need to copy the partitioned counter)
        //Value operator++(Value) { Value x = get(); inc(1); return x; }
        //Value operator--(Value) { Value x = get(); dec(1); return x; }

        PartitionedCounter& operator+=(Value x) { return inc(x); }
        PartitionedCounter& operator-=(Value x) { return dec(x); }

      private:
        class ThreadStateData : boost::noncopyable {
            PartitionedCounter *_pc;
            Value _sum;
          public:
            ThreadStateData(PartitionedCounter *);
            ~ThreadStateData();
            friend class PartitionedCounter;
        };
        class ThreadState : public ThreadStateData {
            char _pad[64 - sizeof(ThreadStateData)];
          public:
            ThreadState(PartitionedCounter *pc) : ThreadStateData(pc) {}
        };
        friend class ThreadStateData;

        ThreadState& ts();

        Value _sumOfDead;
        mutable SimpleMutex _mutex;
        thread_specific_ptr<ThreadState> _ts;
        mutable list<ThreadState *> _threadStates;
        typedef typename list<ThreadState *>::iterator states_iterator;
    };

    template<typename Value>
    PartitionedCounter<Value>::ThreadStateData::ThreadStateData(PartitionedCounter *pc) : _pc(pc), _sum(0) {}

    template<typename Value>
    PartitionedCounter<Value>::ThreadStateData::~ThreadStateData() {
        if (_pc != NULL) {
            SimpleMutex::scoped_lock lk(_pc->_mutex);
            _pc->_sumOfDead += _sum;
            for (states_iterator it = _pc->_threadStates.begin(); it != _pc->_threadStates.end(); ++it) {
                if (*it == this) {
                    _pc->_threadStates.erase(it);
                    break;
                }
            }
        }
    }

    template<typename Value>
    PartitionedCounter<Value>::PartitionedCounter(Value init) : _sumOfDead(init), _mutex("PartitionedCounter") {}

    template<typename Value>
    PartitionedCounter<Value>::~PartitionedCounter() {
        // Prevent recursive lock and modification of _threadStates when we destroy all the _ts
        // objects, we don't care about incrementing _sumOfDead because this pc is dying anyway.
        //
        // We need this lock to protect against the race between a terminating thread in
        // ~ThreadState, and us.
        SimpleMutex::scoped_lock lk(_mutex);
        for (states_iterator it = _threadStates.begin(); it != _threadStates.end(); ++it) {
            (*it)->_pc = NULL;
        }
    }

    template<typename Value>
    Value PartitionedCounter<Value>::get() const {
        SimpleMutex::scoped_lock lk(_mutex);
        Value sum = _sumOfDead;
        for (states_iterator it = _threadStates.begin(); it != _threadStates.end(); ++it) {
            sum += (*it)->_sum;
        }
        return sum;
    }

    template<typename Value>
    typename PartitionedCounter<Value>::ThreadState& PartitionedCounter<Value>::ts() {
        if (_ts.get() == NULL) {
            _ts.reset(new ThreadState(this));
            SimpleMutex::scoped_lock lk(_mutex);
            _threadStates.push_back(_ts.get());
        }
        return *_ts;
    }

    template<typename Value>
    PartitionedCounter<Value>& PartitionedCounter<Value>::inc(Value x) {
        ts()._sum += x;
        return *this;
    }

    template<typename Value>
    PartitionedCounter<Value>& PartitionedCounter<Value>::dec(Value x) {
        if (!std::numeric_limits<Value>::is_signed) {
            massert(17019, "cannot decrement partitioned counter below zero", ts()._sum > x);
        }
        ts()._sum -= x;
        return *this;
    }

} // namespace mongo
