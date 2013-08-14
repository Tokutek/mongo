/**
*    Copyright (C) 2008 10gen Inc.
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

#include <string>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/base/units.h"
#include "mongo/db/jsobj.h"
#include "mongo/platform/process_id.h"
#include "mongo/util/net/listen.h"

namespace mongo {

    namespace optionenvironment {
        class OptionSection;
        class Environment;
    } // namespace optionenvironment

    /* command line options
    */
    /* concurrency: OK/READ */
    struct CmdLine {

        CmdLine();

        std::string binaryName;     // mongod or mongos
        std::string cwd;            // cwd of when process started

        // this is suboptimal as someone could rename a binary.  todo...
        bool isMongos() const { return binaryName == "mongos"; }

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

        std::string _replSet;       // --replSet[/<seedlist>]
        std::string ourSetName() const {
            std::string setname;
            size_t sl = _replSet.find('/');
            if( sl == std::string::npos )
                return _replSet;
            return _replSet.substr(0, sl);
        }
        bool usingReplSets() const { return !_replSet.empty(); }

        // for master/slave replication
        std::string source;    // --source
        std::string only;      // --only

        bool quiet;            // --quiet
        bool noTableScan;      // --notablescan no table scans allowed

        bool configsvr;        // --configsvr

        bool quota;            // --quota
        int quotaFiles;        // --quotaFiles
        bool cpu;              // --cpu show cpu time periodically

        uint32_t logFlushPeriod; // group/batch commit interval ms
        uint32_t expireOplogDays;  // number of days before an oplog entry is eligible for removal
        uint32_t expireOplogHours; // number of hours, in addition to days above.


        bool objcheck;         // --objcheck

        int defaultProfile;    // --profile
        int slowMS;            // --time in ms that is "slow"
        int defaultLocalThresholdMillis;    // --localThreshold in ms to consider a node local
        bool moveParanoia;     // for move chunk paranoia
        double syncdelay;      // seconds between fsyncs

        bool noUnixSocket;     // --nounixsocket
        bool doFork;           // --fork
        std::string socket;    // UNIX domain socket directory

        int maxConns;          // Maximum number of simultaneous open connections.

        std::string keyFile;   // Path to keyfile, or empty if none.
        std::string pidFile;   // Path to pid file, or empty if none.

        std::string logpath;   // Path to log file, if logging to a file; otherwise, empty.
        bool logAppend;        // True if logging to a file in append mode.
        bool logWithSyslog;    // True if logging to syslog; must not be set if logpath is set.

        bool isHttpInterfaceEnabled; // True if the dbwebserver should be enabled.

#ifndef _WIN32
        ProcessId parentProc;      // --fork pid of initial process
        ProcessId leaderProc;      // --fork pid of leader process
#endif

#ifdef MONGO_SSL
        bool sslOnNormalPorts;      // --sslOnNormalPorts
        std::string sslPEMKeyFile;       // --sslPEMKeyFile
        std::string sslPEMKeyPassword;   // --sslPEMKeyPassword
        std::string sslClusterFile;       // --sslInternalKeyFile
        std::string sslClusterPassword;   // --sslInternalKeyPassword
        std::string sslCAFile;      // --sslCAFile
        std::string sslCRLFile;     // --sslCRLFile
        bool sslWeakCertificateValidation;
        bool sslFIPSMode;
#endif
        
        // TokuMX variables
        bool directio;
        bool gdb;
        BytesQuantity<uint64_t> cacheSize;
        BytesQuantity<uint64_t> locktreeMaxMemory;
        BytesQuantity<uint64_t> loaderMaxMemory;
        bool loaderCompressTmp;
        uint32_t checkpointPeriod;
        uint32_t cleanerPeriod;
        uint32_t cleanerIterations;
        uint64_t lockTimeout;
        int fsRedzone;
        string logDir;
        string tmpDir;
        string gdbPath;
        BytesQuantity<uint64_t> txnMemLimit;
        bool fastupdates;
        bool fastupdatesIgnoreErrors;

        string pluginsDir;
        vector<string> plugins;

        static void launchOk();

        /**
         * @return true if should run program, false if should exit
         */
        static Status store( const std::vector<std::string>& argv,
                             optionenvironment::OptionSection& options,
                             optionenvironment::Environment& output );

        /**
         * Blot out sensitive fields in the argv array.
         */
        static void censor(int argc, char** argv);
        static void censor(std::vector<std::string>* args);

        static BSONArray getArgvArray();
        static BSONObj getParsedOpts();

        static Status setupBinaryName(const std::vector<std::string>& argv);
        static Status setupCwd();
        static Status setArgvArray(const std::vector<std::string>& argv);
        static Status setParsedOpts(optionenvironment::Environment& params);

        time_t started;
    };

    // todo move to cmdline.cpp?
    inline CmdLine::CmdLine() :
        port(DefaultDBPort), rest(false), jsonp(false), quiet(false),
        noTableScan(false),
        configsvr(false), quota(false), quotaFiles(8), cpu(false),
        logFlushPeriod(100), // 0 means fsync every transaction, 100 means fsync log once every 100 ms
        expireOplogDays(14), expireOplogHours(0), // default of 14, two weeks
        objcheck(true), defaultProfile(0),
        slowMS(100), defaultLocalThresholdMillis(15), moveParanoia( false ),
        syncdelay(60), noUnixSocket(false), doFork(0), socket("/tmp"), maxConns(DEFAULT_MAX_CONN),
        logAppend(false), logWithSyslog(false), isHttpInterfaceEnabled(false),
        directio(false), gdb(false), cacheSize(0), locktreeMaxMemory(0), loaderMaxMemory(0), loaderCompressTmp(true), checkpointPeriod(60), cleanerPeriod(2),
        cleanerIterations(5), lockTimeout(4000), fsRedzone(5), logDir(""), tmpDir(""), gdbPath(""),
        txnMemLimit(1ULL<<20), fastupdates(false), fastupdatesIgnoreErrors(false), pluginsDir(), plugins()
    {
        started = time(0);

#ifdef MONGO_SSL
        sslOnNormalPorts = false;
#endif
    }

    extern CmdLine cmdLine;

    void printCommandLineOpts();
}

