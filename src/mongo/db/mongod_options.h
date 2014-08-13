/*
 *    Copyright (C) 2013 10gen Inc.
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

#include "mongo/base/status.h"
#include "mongo/db/server_options.h"
#include "mongo/db/storage_options.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/option_section.h"

namespace mongo {

    namespace optionenvironment {
        class OptionSection;
        class Environment;
    } // namespace optionenvironment

    namespace moe = mongo::optionenvironment;

    struct MongodGlobalParams {
        bool upgrade;
        bool repair;
        bool scriptingEnabled; // --noscripting

        MongodGlobalParams() :
            upgrade(0),
            repair(0),
            scriptingEnabled(true)
        { }
    };

    extern MongodGlobalParams mongodGlobalParams;

    Status addMongodOptions(moe::OptionSection* options);

    void printMongodHelp(const moe::OptionSection& options);

    bool handlePreValidationMongodOptions(const moe::Environment& params,
                                            const std::vector<std::string>& args);

    Status storeMongodOptions(const moe::Environment& params, const std::vector<std::string>& args);
}
