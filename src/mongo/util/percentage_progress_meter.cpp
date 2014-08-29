// percentage_progress_meter.cpp

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

#include "mongo/util/percentage_progress_meter.h"

#include "mongo/pch.h"

#include <iomanip>

#include "mongo/util/log.h"

namespace mongo {

    std::string PercentageProgressMeter::toString() const {
        std::stringstream ss;
        ss << _prefix << ": " << std::fixed << std::setprecision(1) << (100 * _lastReported) << "%";
        return ss.str();
    }

    bool PercentageProgressMeter::report(float progress) {
        if (_t.millis() < _minMillis || progress < _lastReported + _minDelta) {
            return false;
        }

        _t.reset();
        _lastReported = progress;
        LOG(0) << "\t\t" << toString() << endl;

        return true;
    }

}
