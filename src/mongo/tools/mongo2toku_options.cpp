/*
 *    Copyright (C) 2010 10gen Inc.
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

#include "mongo/tools/mongo2toku_options.h"

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/password.h"

namespace mongo {

    Mongo2TokuGlobalParams mongo2TokuGlobalParams;

    typedef moe::OptionDescription OD;
    typedef moe::PositionalOptionDescription POD;

    Status addMongo2TokuOptions(moe::OptionSection* options) {
        Status ret = addGeneralToolOptions(options);
        if (!ret.isOK()) {
            return ret;
        }

        ret = addRemoteServerToolOptions(options);
        if (!ret.isOK()) {
            return ret;
        }

        ret = addLocalServerToolOptions(options);
        if (!ret.isOK()) {
            return ret;
        }

        ret = options->addOption(OD("ts", "ts", moe::String,
                    "max OpTime already applied (secs:inc)", true));
        if (!ret.isOK()) {
            return ret;
        }

        ret = options->addOption(OD("from", "from", moe::String,
                    "host to pull from", true));
        if (!ret.isOK()) {
            return ret;
        }

        ret = options->addOption(OD("oplogns", "oplogns", moe::String,
                    "ns to pull from", true, moe::Value(std::string("local.oplog.rs"))));
        if (!ret.isOK()) {
            return ret;
        }

        ret = options->addOption(OD("reportingPeriod", "reportingPeriod", moe::Int,
                    "seconds between progress reports", true));
        if (!ret.isOK()) {
            return ret;
        }


        ret = options->addOption(OD("ruser", "ruser", moe::String, "username on source host", true));
        if(!ret.isOK()) {
            return ret;
        }
        // We ask a user for a password if they pass in an empty string or pass --password with no
        // argument.  This must be handled when the password value is checked.
        //
        // Desired behavior:
        // --username test --password test // Continue with username "test" and password "test"
        // --username test // Continue with username "test" and no password
        // --username test --password // Continue with username "test" and prompt for password
        // --username test --password "" // Continue with username "test" and prompt for password
        //
        // To do this we pass moe::Value(std::string("")) as the "implicit value" of this option
        ret = options->addOption(OD("rpass", "rpass", moe::String,
                    "password on source host", true, moe::Value(), moe::Value(std::string(""))));
        if(!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("rauthenticationDatabase", "rauthenticationDatabase", moe::String,
                    "user source on source host (defaults to admin)", true,
                    moe::Value(std::string("admin"))));
        if(!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("rauthenticationMechanism", "rauthenticationMechanism",
                    moe::String,
                    "authentication mechanism on remote host", true,
                    moe::Value(std::string("MONGODB-CR"))));
        if(!ret.isOK()) {
            return ret;
        }

        return Status::OK();
    }

    void printMongo2TokuHelp(const moe::OptionSection options, std::ostream* out) {
        *out << "Pull and replay a remote MongoDB oplog.\n" << std::endl;
        *out << options.helpString();
        *out << std::flush;
    }

    Status handlePreValidationMongo2TokuOptions(const moe::Environment& params) {
        if (toolsParsedOptions.count("help")) {
            printMongo2TokuHelp(toolsOptions, &std::cout);
            ::_exit(0);
        }

        return Status::OK();
    }

    Status storeMongo2TokuOptions(const moe::Environment& params,
                                  const std::vector<std::string>& args) {
        Status ret = storeGeneralToolOptions(params, args);
        if (!ret.isOK()) {
            return ret;
        }

        if (!hasParam("from")) {
            log() << "need to specify --from" << std::endl;
            ::_exit(-1);
        }
        else {
            mongo2TokuGlobalParams.from = getParam("from");
        }

        if (toolsParsedOptions.count("ts")) {
            mongo2TokuGlobalParams.ts = toolsParsedOptions["ts"].as<string>();
        }
        if (toolsParsedOptions.count("oplogns")) {
            mongo2TokuGlobalParams.oplogns = toolsParsedOptions["oplogns"].as<string>();
        }
        if (toolsParsedOptions.count("reportingPeriod")) {
            mongo2TokuGlobalParams.reportingPeriod = toolsParsedOptions["reportingPeriod"].as<int>();
        }
        if (toolsParsedOptions.count("ruser")) {
            mongo2TokuGlobalParams.ruser = toolsParsedOptions["ruser"].as<string>();
            if (!toolsParsedOptions.count("rpass")) {
                log() << "if doing auth on source, must specify both --ruser and --rpass" << endl;
                ::_exit(-1);
            }
        }
        if (toolsParsedOptions.count("rpass")) {
            mongo2TokuGlobalParams.rpass = toolsParsedOptions["rpass"].as<string>();
            if (mongo2TokuGlobalParams.rpass.empty()) {
                mongo2TokuGlobalParams.rpass = askPassword();
            }
        }
        if (toolsParsedOptions.count("rauthenticationDatabase")) {
            mongo2TokuGlobalParams.rauthenticationDatabase = toolsParsedOptions["rauthenticationDatabase"].as<string>();
        }
        if (toolsParsedOptions.count("rauthenticationMechanism")) {
            mongo2TokuGlobalParams.rauthenticationMechanism = toolsParsedOptions["rauthenticationMechanism"].as<string>();
        }

        return Status::OK();
    }

    MONGO_INITIALIZER_GENERAL(ParseStartupConfiguration,
            MONGO_NO_PREREQUISITES,
            ("default"))(InitializerContext* context) {

        toolsOptions = moe::OptionSection( "options" );
        moe::OptionsParser parser;

        Status retStatus = addMongo2TokuOptions(&toolsOptions);
        if (!retStatus.isOK()) {
            return retStatus;
        }

        retStatus = parser.run(toolsOptions, context->args(), context->env(), &toolsParsedOptions);
        if (!retStatus.isOK()) {
            std::cerr << retStatus.reason() << std::endl;
            std::cerr << "try '" << context->args()[0]
                      << " --help' for more information" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }

        retStatus = handlePreValidationMongo2TokuOptions(toolsParsedOptions);
        if (!retStatus.isOK()) {
            return retStatus;
        }

        retStatus = toolsParsedOptions.validate();
        if (!retStatus.isOK()) {
            return retStatus;
        }

        retStatus = storeMongo2TokuOptions(toolsParsedOptions, context->args());
        if (!retStatus.isOK()) {
            return retStatus;
        }

        return Status::OK();
    }
}
