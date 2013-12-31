/**
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

#include "mongo/pch.h"

#include <cmath>
#include <limits>
#include <numeric>
#include <boost/tuple/tuple.hpp>

#include "mongo/platform/unordered_set.h"

namespace mongo {

    typedef std::pair<double, double> QuantileBase;
    class Quantile : public QuantileBase {
      public:
        Quantile(double _phi, double _epsilon) : QuantileBase(_phi, _epsilon) {}
        double f(double ri, unsigned long long n) const {
            if (ri >= first * n) {
                return 2 * second * ri / first;
            } else {
                return 2 * second * (n - ri) / (1 - first);
            }
        }
    };
    typedef set<Quantile> QuantileSetBase;
    class QuantileSet : public QuantileSetBase {
      public:
        QuantileSet() {}
        QuantileSet& add(double phi, double epsilon) {
            insert(Quantile(phi, epsilon));
            return *this;
        }
        double f(double ri, unsigned long long n) const {
            double minF = std::numeric_limits<double>::max();
            for (const_iterator it = begin(); it != end(); ++it) {
                double curF = it->f(ri, n);
                if (curF < minF) {
                    minF = curF;
                }
            }
            return minF;
        }
    };

    /**
     * QuantileCounter receives a stream of values and can report the phi-th quantile of the stream.
     * It has good accuracy near the requests made in the QuantileSet.  This is an implementation of
     * http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.91.853&rep=rep1&type=pdf (targeted
     * quantiles problem).
     */
    template<typename T>
    class QuantileCounter : public boost::noncopyable {
        typedef boost::tuple<T, unsigned long long, unsigned long long> ElementBase;
        class Element : public ElementBase {
          public:
            Element(T v) : ElementBase(v) {}
            Element(T v, unsigned long long g, unsigned long long d) : ElementBase(v, g, d) {}
            T v() const { return boost::get<0>(*this); }
            unsigned long long& g() { return boost::get<1>(*this); }
            unsigned long long& d() { return boost::get<2>(*this); }
            unsigned long long g() const { return boost::get<1>(*this); }
            unsigned long long d() const { return boost::get<2>(*this); }
            struct LessThan {
                bool operator()(const Element &a, const Element &b) const {
                    return a.v() < b.v();
                }
            };
        };
        typedef multiset<Element, typename Element::LessThan> State;

        const QuantileSet _requests;
        size_t _n;
        State _state;
        T _min, _max;

        static unsigned long long accumulateElementValue(unsigned long long acc, const Element &e) {
            return acc + e.v();
        }

        unsigned long long r(typename State::iterator at) const {
            return std::accumulate(_state.begin(), at, 0ULL, accumulateElementValue);
        }

        void compress(typename State::iterator from, typename State::iterator to) {
            unsigned long long ri = r(from);
            typename State::iterator last(to);
            last--;
            for (typename State::iterator cur(from); cur < last; ) {
                typename State::iterator next(cur);
                next++;
                if (cur->g() + next->g() + next->d() < _requests.f(ri, _n)) {
                    Element e(next->v(), cur->g() + next->g(), next->d());
                    _state.erase(cur);
                    _state.erase(next);
                    cur = _state.insert(e);
                } else {
                    cur++;
                }
            }
        }

      public:
        QuantileCounter(const QuantileSet &requests) : _requests(requests), _state(typename Element::LessThan()) {}

        QuantileCounter& operator<<(T v) {
            _n++;
            typename State::iterator pos = _state.insert(Element(v));
            Element& e = const_cast<Element &>(*pos);
            typename State::iterator last(_state.end());
            last--;
            if (pos == _state.begin() ||
                pos == last) {
                e.g() = 1;
                e.d() = 0;
            } else {
                unsigned long long ri = r(pos);
                e.g() = 1;
                e.d() = std::floor(_requests.f(ri, _n)) - 1;
            }
            return *this;
        }

        void compress() {
            compress(_state.begin(), _state.end());
        }

        T quantile(double phi) const {
            double target = phi * _n;
            unsigned long long ri = 0;
            for (typename State::iterator it = _state.begin(); it != _state.end(); ++it) {
                if (ri + it->g() + it->d() > target + _requests.f(target, _n) / 2) {
                    return it->v();
                }
                ri += it->g();
            }
            typename State::iterator last(_state.end());
            last--;
            return last->v();
        }
    };

} // namespace mongo
