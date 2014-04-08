// percentage_progress_meter.h

/*    Copyright 2014 Tokutek Inc.
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

#include <limits>
#include <string>

#include <boost/noncopyable.hpp>

#include "mongo/util/timer.h"

namespace mongo {

    class PercentageProgressMeter : boost::noncopyable {
      public:
        PercentageProgressMeter(const std::string &prefix, float minDelta = 0.001, long long minMillis = 1000)
                : _lastReported(-std::numeric_limits<float>::max()),
                  _prefix(prefix),
                  _minDelta(minDelta),
                  _minMillis(minMillis) {}

        std::string toString() const;
        bool report(float progress);

      private:
        float _lastReported;
        Timer _t;
        const std::string _prefix;
        const float _minDelta;
        const long long _minMillis;
    };

}
