/* Copyright 2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "mongo/util/net/ssl_options.h"

#include <boost/filesystem/operations.hpp>

#include "mongo/base/status.h"
#include "mongo/db/server_options.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {

    typedef moe::OptionDescription OD;
    typedef moe::PositionalOptionDescription POD;

    Status addSSLServerOptions(moe::OptionSection* options) {
        options->addOptionChaining("ssl.sslOnNormalPorts", "sslOnNormalPorts", moe::Switch,
                "use ssl on configured ports");

        options->addOptionChaining("ssl.PEMKeyFile", "sslPEMKeyFile", moe::String,
                "PEM file for ssl");

        options->addOptionChaining("ssl.PEMKeyPassword", "sslPEMKeyPassword", moe::String,
                "PEM file password")
                                  .setImplicit(moe::Value(std::string("")));

        options->addOptionChaining("ssl.clusterFile", "sslClusterFile", moe::String,
                "Key file for internal SSL authentication");

        options->addOptionChaining("ssl.clusterPassword", "sslClusterPassword", moe::String,
                "Internal authentication key file password")
                                  .setImplicit(moe::Value(std::string("")));

        options->addOptionChaining("ssl.CAFile", "sslCAFile", moe::String,
                "Certificate Authority file for SSL");

        options->addOptionChaining("ssl.CRLFile", "sslCRLFile", moe::String,
                "Certificate Revocation List file for SSL");

        options->addOptionChaining("ssl.weakCertificateValidation", "sslWeakCertificateValidation",
                moe::Switch, "allow client to connect without presenting a certificate");

        options->addOptionChaining("ssl.FIPSMode", "sslFIPSMode", moe::Switch,
                "activate FIPS 140-2 mode at startup");


        return Status::OK();
    }

    Status addSSLClientOptions(moe::OptionSection* options) {
        options->addOptionChaining("ssl", "ssl", moe::Switch, "use SSL for all connections");

        options->addOptionChaining("ssl.CAFile", "sslCAFile", moe::String,
                "Certificate Authority file for SSL");

        options->addOptionChaining("ssl.PEMKeyFile", "sslPEMKeyFile", moe::String,
                "PEM certificate/key file for SSL");

        options->addOptionChaining("ssl.PEMKeyPassword", "sslPEMKeyPassword", moe::String,
                "password for key in PEM file for SSL");

        options->addOptionChaining("ssl.CRLFile", "sslCRLFile", moe::String,
                "Certificate Revocation List file for SSL");

        options->addOptionChaining("ssl.FIPSMode", "sslFIPSMode", moe::Switch,
                "activate FIPS 140-2 mode at startup");


        return Status::OK();
    }

    Status storeSSLServerOptions(const moe::Environment& params) {

        if (params.count("ssl.PEMKeyFile")) {
            sslGlobalParams.sslPEMKeyFile = boost::filesystem::absolute(
                                        params["ssl.PEMKeyFile"].as<string>()).generic_string();
        }

        if (params.count("ssl.PEMKeyPassword")) {
            sslGlobalParams.sslPEMKeyPassword = params["ssl.PEMKeyPassword"].as<string>();
        }

        if (params.count("ssl.clusterFile")) {
            sslGlobalParams.sslClusterFile = boost::filesystem::absolute(
                                         params["ssl.clusterFile"].as<string>()).generic_string();
        }

        if (params.count("ssl.clusterPassword")) {
            sslGlobalParams.sslClusterPassword = params["ssl.clusterPassword"].as<string>();
        }

        if (params.count("ssl.CAFile")) {
            sslGlobalParams.sslCAFile = boost::filesystem::absolute(
                                         params["ssl.CAFile"].as<std::string>()).generic_string();
        }

        if (params.count("ssl.CRLFile")) {
            sslGlobalParams.sslCRLFile = boost::filesystem::absolute(
                                         params["ssl.CRLFile"].as<std::string>()).generic_string();
        }

        if (params.count("ssl.weakCertificateValidation")) {
            sslGlobalParams.sslWeakCertificateValidation = true;
        }
        if (params.count("ssl.sslOnNormalPorts")) {
            sslGlobalParams.sslOnNormalPorts = true;
            if (sslGlobalParams.sslPEMKeyFile.size() == 0) {
                return Status(ErrorCodes::BadValue,
                              "need sslPEMKeyFile with sslOnNormalPorts");
            }
            if (sslGlobalParams.sslWeakCertificateValidation &&
                sslGlobalParams.sslCAFile.empty()) {
                return Status(ErrorCodes::BadValue,
                              "need sslCAFile with sslWeakCertificateValidation");
            }
            if (!sslGlobalParams.sslCRLFile.empty() &&
                sslGlobalParams.sslCAFile.empty()) {
                return Status(ErrorCodes::BadValue, "need sslCAFile with sslCRLFile");
            }
            if (params.count("ssl.FIPSMode")) {
                sslGlobalParams.sslFIPSMode = true;
            }
        }
        else if (sslGlobalParams.sslPEMKeyFile.size() ||
                 sslGlobalParams.sslPEMKeyPassword.size() ||
                 sslGlobalParams.sslClusterFile.size() ||
                 sslGlobalParams.sslClusterPassword.size() ||
                 sslGlobalParams.sslCAFile.size() ||
                 sslGlobalParams.sslCRLFile.size() ||
                 sslGlobalParams.sslWeakCertificateValidation ||
                 sslGlobalParams.sslFIPSMode) {
            return Status(ErrorCodes::BadValue, "need to enable sslOnNormalPorts");
        }
        if (serverGlobalParams.clusterAuthMode == "sendKeyfile" ||
            serverGlobalParams.clusterAuthMode == "sendX509" ||
            serverGlobalParams.clusterAuthMode == "x509") {
            if (!sslGlobalParams.sslOnNormalPorts){
                return Status(ErrorCodes::BadValue, "need to enable sslOnNormalPorts");
            }
        }
        else if (params.count("clusterAuthMode") &&
                 serverGlobalParams.clusterAuthMode != "keyfile") {
            StringBuilder sb;
            sb << "unsupported value for clusterAuthMode " << serverGlobalParams.clusterAuthMode;
            return Status(ErrorCodes::BadValue, sb.str());
        }

        return Status::OK();
    }

    Status storeSSLClientOptions(const moe::Environment& params) {
        if (params.count("ssl")) {
            sslGlobalParams.sslOnNormalPorts = true;
        }
        if (params.count("ssl.PEMKeyFile")) {
            sslGlobalParams.sslPEMKeyFile = params["ssl.PEMKeyFile"].as<std::string>();
        }
        if (params.count("ssl.PEMKeyPassword")) {
            sslGlobalParams.sslPEMKeyPassword = params["ssl.PEMKeyPassword"].as<std::string>();
        }
        if (params.count("ssl.CAFile")) {
            sslGlobalParams.sslCAFile = params["ssl.CAFile"].as<std::string>();
        }
        if (params.count("ssl.CRLFile")) {
            sslGlobalParams.sslCRLFile = params["ssl.CRLFile"].as<std::string>();
        }
        if (params.count("ssl.FIPSMode")) {
            sslGlobalParams.sslFIPSMode = true;
        }
        return Status::OK();
    }

} // namespace mongo
