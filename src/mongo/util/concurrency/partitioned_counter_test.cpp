// @file partitioned_counter_test.cpp

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

#include "mongo/unittest/unittest.h"

#include <boost/ref.hpp>
#include <boost/thread/thread.hpp>

#include "mongo/platform/atomic_word.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/concurrency/partitioned_counter.h"
#include "mongo/util/log.h"
#include "mongo/util/timer.h"

namespace {

    using namespace mongo;

    TEST(PartitionedCounterTest, Create) {
        PartitionedCounter<int> pc1;
        ASSERT_EQUALS(pc1, 0);
        PartitionedCounter<unsigned> pc2;
        ASSERT_EQUALS(pc2.get(), 0U);
    }

    TEST(PartitionedCounterTest, CreateWithArg) {
        PartitionedCounter<int> pc3(4);
        ASSERT_EQUALS(pc3.get(), 4);
    }

    TEST(PartitionedCounterTest, Increment) {
        PartitionedCounter<int> pc;
        int i = 0;
        while (i < 10) {
            ASSERT_EQUALS(pc, i);
            ASSERT_EQUALS(++pc, ++i);
        }
        while (i < 20) {
            ASSERT_EQUALS(pc, i);
            ASSERT_EQUALS(pc += 1, ++i);
        }
        while (i < 30) {
            ASSERT_EQUALS(pc, i);
            ASSERT_EQUALS(pc.inc(), ++i);
        }
    }

    TEST(PartitionedCounterTest, IncrementWithArg) {
        PartitionedCounter<int> pc;
        int i = 0;
        while (i < 30) {
            ASSERT_EQUALS(pc, i);
            ASSERT_EQUALS(pc.inc(3), i += 3);
        }
        while (i < 60) {
            ASSERT_EQUALS(pc, i);
            ASSERT_EQUALS(pc += 3, i += 3);
        }
    }

    TEST(PartitionedCounterTest, Decrement) {
        PartitionedCounter<int> pc(100);
        int i = 100;
        while (i > 90) {
            ASSERT_EQUALS(pc, i);
            ASSERT_EQUALS(--pc, --i);
        }
        while (i > 80) {
            ASSERT_EQUALS(pc, i);
            ASSERT_EQUALS(pc -= 1, --i);
        }
        while (i > 70) {
            ASSERT_EQUALS(pc, i);
            ASSERT_EQUALS(pc.dec(), --i);
        }
    }

    TEST(PartitionedCounterTest, DecrementWithArg) {
        PartitionedCounter<int> pc;
        int i = 0;
        while (i > 70) {
            ASSERT_EQUALS(pc, i);
            ASSERT_EQUALS(pc.dec(3), i -= 3);
        }
        while (i > 40) {
            ASSERT_EQUALS(pc, i);
            ASSERT_EQUALS(pc -= 3, i -= 3);
        }
    }

    TEST(PartitionedCounterTest, DecrementUnsignedUnderflow) {
        PartitionedCounter<unsigned> pc(3);
        ASSERT_THROWS(pc -= 4, MsgAssertionException);
    }

    static void incThread(int n, PartitionedCounter<int>& pc, volatile bool& running) {
        for (int i = 0; i < n; ++i) {
            ++pc;
        }
        while (running) {
            sleepmillis(1);
        }
    }

    TEST(PartitionedCounterTest, Threading) {
        static const int NTHREADS = 10;
        static const int NINCS = 100000;
        boost::thread_group group;
        PartitionedCounter<int> pc;
        volatile bool running = true;
        for (int i = 0; i < 10; ++i) {
            group.add_thread(new boost::thread(incThread, NINCS, boost::ref(pc), boost::ref(running)));
        }
        while (pc != NTHREADS * NINCS) {
            sleepmillis(1);
        }
        running = false;
        group.join_all();
        ASSERT_EQUALS(pc, NTHREADS * NINCS);
        ++pc;
        ASSERT_EQUALS(pc, NTHREADS * NINCS + 1);
    }

    // just adds the api that counterThread will use on top of AtomicWord
    template<typename WordType>
    class AtomicWordCounter : public AtomicWord<WordType> {
      public:
        AtomicWordCounter() : AtomicWord<WordType>() {}
        AtomicWordCounter& operator++() { AtomicWord<WordType>::addAndFetch(1); return *this; }
        operator WordType() const { return AtomicWord<WordType>::load(); }
    };

    template<class Counter>
    static void counterThread(size_t n, Counter& c) {
        for (size_t i = 0; i < n; ++i) {
            ++c;
        }
    }

    template<class Counter>
    static unsigned long long timeit(int nthreads) {
        static const size_t NINCS = 2ULL<<24;
        const size_t incsPerThread = NINCS / nthreads;
        boost::thread_group group;
        Counter c;
        Timer t;
        for (int i = 0; i < nthreads; ++i) {
            group.add_thread(new boost::thread(counterThread<Counter>, incsPerThread, boost::ref(c)));
        }
        group.join_all();
        ASSERT_EQUALS(c, incsPerThread * nthreads);
        return t.micros();
    }

    TEST(PartitionedCounterTest, Timing) {
        for (int nthreads = 1; nthreads <= 1<<10; nthreads <<= 1) {
            unsigned long long atomic = timeit<AtomicWordCounter<uint64_t> >(nthreads);
            unsigned long long partitioned = timeit<PartitionedCounter<uint64_t> >(nthreads);
            LOG(0) << nthreads << " threads" << endl
                   << "  atomic:      " << atomic << endl
                   << "  partitioned: " << partitioned << endl;
#if !_DEBUG
            // 4 threads seems to be the tipping point for most processors
            // maybe TODO figure out how to make it faster (don't use boost tsp?)
            if (nthreads > 2) {
                ASSERT_LESS_THAN(partitioned, atomic);
            }
#endif
        }
    }

} // namespace
