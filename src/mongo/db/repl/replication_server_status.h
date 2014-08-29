/**
*    Copyright (C) 2008 10gen Inc.
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

#include <set>
#include <string>

#include "mongo/db/jsobj.h"
#include "mongo/util/concurrency/mutex.h"


namespace mongo {

    bool anyReplEnabled();

    class ReplSettings {
    public:
        bool fastsync;
        bool startInRecovery;

        bool autoresync;

        int slavedelay;

        int expireOplogDays;
        int expireOplogHours;

        std::string replSet;       // --replSet[/<seedlist>]
        std::string ourSetName() const {
            std::string setname;
            size_t sl = replSet.find('/');
            if( sl == std::string::npos )
                return replSet;
            return replSet.substr(0, sl);
        }
        bool usingReplSets() const { return !replSet.empty(); }

        std::set<std::string> discoveredSeeds;
        mutex discoveredSeeds_mx;

        BSONObj reconfig;

        ReplSettings()
            : fastsync(),
            startInRecovery(false),
            autoresync(false),
            slavedelay(),
            expireOplogDays(14),
            expireOplogHours(0),
            discoveredSeeds(),
            discoveredSeeds_mx("ReplSettings::discoveredSeeds") {
        }

    };

    extern ReplSettings replSettings;
}
