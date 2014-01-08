/* -*- mode: C++; c-file-style: "Google"; c-basic-offset: 4 -*- */

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

#include "mongo/pch.h"

// Not even ashamed about how non-portable this is.
#include <unistd.h>

namespace mongo {

    // Process-wide alarm that explodes after a given number of milliseconds.
    // Two alarms _will_ interfere with each other.
    //
    // Good for catching stalls in a debugger.
    class Alarm {
    public:
        static void handleAlarm(int sig) {
            abort();
        }

        Alarm(const int ms) {
            struct sigaction sact;
            sigemptyset(&sact.sa_mask);
            sact.sa_flags = 0;
            sact.sa_handler = handleAlarm;
            sigaction(SIGALRM, &sact, 0);
            ualarm(ms * 1000, 0);
        }

        ~Alarm() {
            ualarm(0, 0);
        }
    };

} // namespace mongo
