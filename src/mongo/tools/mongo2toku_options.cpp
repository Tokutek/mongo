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

#include "mongo/base/status.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/password.h"

namespace mongo {

    Mongo2TokuGlobalParams mongo2TokuGlobalParams;

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

        options->addOptionChaining("ts", "ts", moe::String, "max OpTime already applied (secs:inc)");

        options->addOptionChaining("from", "from", moe::String, "host to pull from");

        options->addOptionChaining("oplogns", "oplogns", moe::String, "ns to pull from")
                                  .setDefault(moe::Value(std::string("local.oplog.rs")));

        options->addOptionChaining("reportingPeriod", "reportingPeriod", moe::Int,
                "seconds between progress reports");


        options->addOptionChaining("ruser", "ruser", moe::String, "username on source host");

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
        options->addOptionChaining("rpass", "rpass", moe::String, "password on source host")
                                  .setImplicit(moe::Value(std::string("")));

        options->addOptionChaining("rauthenticationDatabase", "rauthenticationDatabase", moe::String,
                "user source on source host (defaults to admin)")
                                  .setDefault(moe::Value(std::string("admin")));

        options->addOptionChaining("rauthenticationMechanism", "rauthenticationMechanism", moe::String,
                "authentication mechanism on remote host")
                                  .setDefault(moe::Value(std::string("MONGODB-CR")));

        return Status::OK();
    }

    void printMongo2TokuHelp(std::ostream* out) {
        *out << "Pull and replay a remote MongoDB oplog.\n" << std::endl;
        *out << moe::startupOptions.helpString();
        *out << std::flush;
    }

    bool handlePreValidationMongo2TokuOptions(const moe::Environment& params) {
        if (params.count("help")) {
            printMongo2TokuHelp(&std::cout);
            return false;
        }

        return true;
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

        if (params.count("ts")) {
            mongo2TokuGlobalParams.ts = params["ts"].as<string>();
        }
        if (params.count("oplogns")) {
            mongo2TokuGlobalParams.oplogns = params["oplogns"].as<string>();
        }
        if (params.count("reportingPeriod")) {
            mongo2TokuGlobalParams.reportingPeriod = params["reportingPeriod"].as<int>();
        }
        if (params.count("ruser")) {
            mongo2TokuGlobalParams.ruser = params["ruser"].as<string>();
            if (!params.count("rpass")) {
                log() << "if doing auth on source, must specify both --ruser and --rpass" << endl;
                ::_exit(-1);
            }
        }
        if (params.count("rpass")) {
            mongo2TokuGlobalParams.rpass = params["rpass"].as<string>();
            if (mongo2TokuGlobalParams.rpass.empty()) {
                mongo2TokuGlobalParams.rpass = askPassword();
            }
        }
        if (params.count("rauthenticationDatabase")) {
            mongo2TokuGlobalParams.rauthenticationDatabase = params["rauthenticationDatabase"].as<string>();
        }
        if (params.count("rauthenticationMechanism")) {
            mongo2TokuGlobalParams.rauthenticationMechanism = params["rauthenticationMechanism"].as<string>();
        }

        return Status::OK();
    }

}
