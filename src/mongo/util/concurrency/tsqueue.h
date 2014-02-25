/*
 *    Copyright (C) 2014 Tokutek Inc.
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

#include "mongo/pch.h"

#include <exception>
#include <queue>

#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>

namespace mongo {

    /**
     * TSQueue is a thread-safe producer/consumer queue, with blocking push and pop, as well as a flush
     * mechanism.  After flush() is invoked, push() should not be called again, and pop() will throw
     * Empty once the queue has been cleared.
     */
    template<typename T>
    class TSQueue : boost::noncopyable {
        const size_t _maxSize;
        std::queue<T> _queue;
        mutable boost::mutex _mutex;
        boost::condition_variable _pushCond, _popCond;
        size_t _pushWaiters;
        bool _flushing;

      public:
        class Empty : public std::exception {
          public:
            const char *what() const throw() { return "empty queue"; }
        };

        TSQueue(size_t maxSize) : _maxSize(maxSize), _pushWaiters(0), _flushing(false) {}

        void flush() {
            boost::unique_lock<boost::mutex> lk(_mutex);
            _flushing = true;
            while (!_queue.empty() || _pushWaiters > 0) {
                _popCond.notify_all();
                _pushCond.wait(lk);
            }
        }

        void reset() {
            boost::unique_lock<boost::mutex> lk(_mutex);
            _flushing = false;
        }

        bool empty() const {
            boost::unique_lock<boost::mutex> lk(_mutex);
            return _queue.empty();
        }

        size_t size() const {
            boost::unique_lock<boost::mutex> lk(_mutex);
            return _queue.size();
        }

        void push(const T& elt) {
            boost::unique_lock<boost::mutex> lk(_mutex);
            while (_queue.size() >= _maxSize) {
                _pushWaiters++;
                _pushCond.wait(lk);
                _pushWaiters--;
            }
            _queue.push(elt);
            _popCond.notify_one();
        }

        T pop() {
            boost::unique_lock<boost::mutex> lk(_mutex);
            while (_queue.empty() && !_flushing) {
                _popCond.wait(lk);
            }
            if (_queue.empty()) {
                _pushCond.notify_one();
                throw Empty();
            }
            T copy = _queue.front();
            _queue.pop();
            _pushCond.notify_one();
            return copy;
        }
    };

} // namespace mongo
