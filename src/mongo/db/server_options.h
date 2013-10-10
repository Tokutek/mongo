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
#include "mongo/platform/process_id.h"
#include "mongo/util/net/listen.h" // For DEFAULT_MAX_CONN
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/option_section.h"

namespace mongo {

    namespace optionenvironment {
        class OptionSection;
        class Environment;
    } // namespace optionenvironment

    namespace moe = mongo::optionenvironment;

    struct ServerGlobalParams {

        ServerGlobalParams() :
            port(DefaultDBPort), rest(false), jsonp(false), indexBuildRetry(true), quiet(false),
            configsvr(false), cpu(false), objcheck(true), defaultProfile(0),
            slowMS(100), defaultLocalThresholdMillis(15), moveParanoia(true),
            noUnixSocket(false), doFork(0), socket("/tmp"), maxConns(DEFAULT_MAX_CONN),
            logAppend(false), logWithSyslog(false), isHttpInterfaceEnabled(false)
        {
            started = time(0);
        }

        std::string binaryName;     // mongod or mongos
        std::string cwd;            // cwd of when process started

        int port;              // --port
        enum {
            DefaultDBPort = 27017,
            ConfigServerPort = 27019,
            ShardServerPort = 27018
        };
        bool isDefaultPort() const { return port == DefaultDBPort; }

        std::string bind_ip;        // --bind_ip
        bool rest;             // --rest
        bool jsonp;            // --jsonp

        bool indexBuildRetry;  // --noIndexBuildRetry

        bool quiet;            // --quiet

        bool configsvr;        // --configsvr

        bool cpu;              // --cpu show cpu time periodically

        bool objcheck;         // --objcheck

        int defaultProfile;    // --profile
        int slowMS;            // --time in ms that is "slow"
        int defaultLocalThresholdMillis;    // --localThreshold in ms to consider a node local
        bool moveParanoia;     // for move chunk paranoia

        bool noUnixSocket;     // --nounixsocket
        bool doFork;           // --fork
        std::string socket;    // UNIX domain socket directory

        int maxConns;          // Maximum number of simultaneous open connections.

        std::string keyFile;   // Path to keyfile, or empty if none.
        std::string pidFile;   // Path to pid file, or empty if none.

        std::string logpath;   // Path to log file, if logging to a file; otherwise, empty.
        bool logAppend;        // True if logging to a file in append mode.
        bool logWithSyslog;    // True if logging to syslog; must not be set if logpath is set.
        std::string clusterAuthMode; // Cluster authentication mode

        bool isHttpInterfaceEnabled; // True if the dbwebserver should be enabled.

#ifndef _WIN32
        ProcessId parentProc;      // --fork pid of initial process
        ProcessId leaderProc;      // --fork pid of leader process
#endif

        bool gdb;
        std::string gdbPath;

        std::vector<std::string> plugins;
        std::string pluginsDir;

        time_t started;

        BSONArray argvArray;
        BSONObj parsedOpts;
    };

    extern ServerGlobalParams serverGlobalParams;

    Status addGeneralServerOptions(moe::OptionSection* options);

    Status addWindowsServerOptions(moe::OptionSection* options);

    Status addSSLServerOptions(moe::OptionSection* options);

    Status storeServerOptions(const moe::Environment& params,
                              const std::vector<std::string>& args);

    void printCommandLineOpts();

    // This function should eventually go away, but needs to be here now because we have a lot of
    // code that is shared between mongod and mongos that must know at runtime which binary it is in
    bool isMongos();
}
