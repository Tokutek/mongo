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

#include "mongo/pch.h"

#include <vector>
#include <boost/thread/thread.hpp>

#include "mongo/unittest/unittest.h"
#include "mongo/util/concurrency/graded_rwlock.h"
#include "mongo/util/concurrency/synchronization.h"
#include "mongo/util/debug_util.h"
#include "mongo/util/log.h"

namespace mongo {
namespace {

    class GradedRWLockTest {
        typedef GradedRWLock<2> TwoLevelLock;
        typedef TwoLevelLock::shared_lock<1> cheap_rdlock;
        typedef TwoLevelLock::shared_lock<2> expensive_rdlock;

        TwoLevelLock _tllock;
        Notification events[3];
        const char *states[3];

        void logState() {
            DEV LOG(0) << states[0] << " " << states[1] << " " << states[2] << std::endl;
            sleepmillis(100);
        }

      public:
        GradedRWLockTest() : _tllock("test") {
            states[0] = "none";
            states[1] = "none";
            states[2] = "none";
        }

        void thread0() {
            // 1. take the cheap read lock
            {
                cheap_rdlock lk(_tllock);
                states[0] = "cheap_rdlock_1";
                logState();
                events[1].notifyOne();
                events[0].waitToBeNotified();
            }
            // 4. drop the cheap lock, grab it again while thread2 is waiting for the write lock
            states[0] = "none";
            logState();
            {
                cheap_rdlock lk(_tllock);
                states[0] = "cheap_rdlock_2";
                logState();
                events[1].notifyOne();
                events[0].waitToBeNotified();
            }
            // 6. drop the cheap lock again and show we can still re-grab it
            states[0] = "none";
            logState();
            {
                cheap_rdlock lk(_tllock);
                states[0] = "cheap_rdlock_3";
                logState();
                events[1].notifyOne();
                events[0].waitToBeNotified();
            }
            // 8. drop the cheap lock, this should let the write lock actually get control
            //    we won't get the next cheap lock until the write lock releases the cheap one
            states[0] = "none";
            logState();
            {
                cheap_rdlock lk(_tllock);
                // 11. get the cheap read lock again, then drop it
                states[0] = "cheap_rdlock_4";
                logState();
                events[2].notifyOne();
                states[0] = "cheap_rdlock_4_again";
                logState();
            }
            states[0] = "none";
            logState();
            events[0].waitToBeNotified();
            // 13. make sure we can get the cheap lock one more time
            {
                cheap_rdlock lk(_tllock);
                states[0] = "cheap_rdlock_5";
                logState();
            }
        }

        void thread1() {
            events[1].waitToBeNotified();
            // 2. take the expensive read lock
            {
                expensive_rdlock lk(_tllock);
                states[1] = "expensive_rdlock_1";
                logState();
                events[2].notifyOne();
                events[1].waitToBeNotified();
                // 5. announce we still have the expensive lock
                states[1] = "expensive_rdlock_1_again";
                logState();
                events[0].notifyOne();
                events[1].waitToBeNotified();
                // 7. drop the expensive lock, and try to grab it again, the write lock should get in now
                states[1] = "expensive_rdlock_1_last";
                logState();
            }
            states[1] = "expensive_rdlock_2_wait";
            logState();
            events[0].notifyOne();
            {
                expensive_rdlock lk(_tllock);
                // 12. announce that we got the expensive read lock one more time, now we're done
                states[1] = "expensive_rdlock_2";
                logState();
                events[0].notifyOne();
            }
            states[1] = "none";
            logState();
        }

        void thread2() {
            events[2].waitToBeNotified();
            // 3. wait for the exclusive lock
            states[2] = "exclusive_lock_wait";
            logState();
            events[0].notifyOne();
            {
                TwoLevelLock::exclusive_lock lk(_tllock);
                // 9. announce that we got the write lock, now we're done
                states[2] = "exclusive_lock";
                logState();
                {
                    TwoLevelLock::exclusive_lock::release_cheap_locks<1> rel(lk);
                    // 10. allow the cheap reader through again one more time
                    states[2] = "exclusive_lock_release<1>";
                    logState();
                    events[2].waitToBeNotified();
                }
                // 12. re-grab the cheap lock, then exit
                states[2] = "exclusive_lock_again";
                logState();
            }
            states[2] = "none";
            logState();
        }
    };

    TEST(GradedRWLockTest, Basic) {
        GradedRWLockTest test;
        vector<shared_ptr<boost::thread> > threads;
        threads.push_back(boost::make_shared<boost::thread>(boost::bind(&GradedRWLockTest::thread0, &test)));
        threads.push_back(boost::make_shared<boost::thread>(boost::bind(&GradedRWLockTest::thread1, &test)));
        threads.push_back(boost::make_shared<boost::thread>(boost::bind(&GradedRWLockTest::thread2, &test)));
        for (vector<shared_ptr<boost::thread> >::iterator it = threads.begin(); it != threads.end(); ++it) {
            boost::thread &t = *it->get();
            t.join();
        }
    }

}
} // namespace mongo
