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

#include "mongo/db/mongod_options.h"

#include <string>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/instance.h"
#include "mongo/db/module.h"
#include "mongo/db/repl/replication_server_status.h"
#include "mongo/db/server_options.h"
#include "mongo/db/server_options_helpers.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/version.h"

namespace mongo {

    MongodGlobalParams mongodGlobalParams;

    extern DiagLog _diaglog;

    Status addMongodOptions(moe::OptionSection* options) {

        moe::OptionSection general_options("General options");

        Status ret = addGeneralServerOptions(&general_options);
        if (!ret.isOK()) {
            return ret;
        }

#if defined(_WIN32)
        moe::OptionSection windows_scm_options("Windows Service Control Manager options");

        ret = addWindowsServerOptions(&windows_scm_options);
        if (!ret.isOK()) {
            return ret;
        }
#endif

#ifdef MONGO_SSL
        moe::OptionSection ssl_options("SSL options");

        ret = addSSLServerOptions(&ssl_options);
        if (!ret.isOK()) {
            return ret;
        }
#endif

        moe::OptionSection ms_options("Master/slave options (old; use replica sets instead)");
        moe::OptionSection rs_options("Replica set options");
        moe::OptionSection replication_options("Replication options");
        moe::OptionSection sharding_options("Sharding options");

        general_options.addOptionChaining("auth", "auth", moe::Switch, "run with security");

        general_options.addOptionChaining("cacheSize", "cacheSize", moe::UnsignedLongLong,
                "tokumx cache size (in bytes) for data and indexes");

        general_options.addOptionChaining("checkpointPeriod", "checkpointPeriod", moe::Unsigned,
                "tokumx time between checkpoints, 0 means never checkpoint");

        general_options.addOptionChaining("cleanerIterations", "cleanerIterations", moe::Unsigned,
                "tokumx number of iterations per cleaner thread operation, 0 means never run");

        general_options.addOptionChaining("cleanerPeriod", "cleanerPeriod", moe::Unsigned,
                "tokumx time between cleaner thread operations, 0 means never run");

        general_options.addOptionChaining("cpu", "cpu", moe::Switch,
                "periodically show cpu and iowait utilization");

#ifdef _WIN32
        general_options.addOptionChaining("dbpath", "dbpath", moe::String,
                "directory for datafiles - defaults to \\data\\db\\")
                                         .setDefault(moe::Value(std::string("\\data\\db\\")));

#else
        general_options.addOptionChaining("dbpath", "dbpath", moe::String,
                "directory for datafiles - defaults to /data/db/")
                                         .setDefault(moe::Value(std::string("/data/db")));

#endif
        general_options.addOptionChaining("diaglog", "diaglog", moe::Int,
                "0=off 1=W 2=R 3=both 7=W+some reads");

        general_options.addOptionChaining("directio", "directio", moe::Switch,
                "use direct I/O in tokumx");

        general_options.addOptionChaining("fsRedzone", "fsRedzone", moe::Int,
                "percentage of free-space left on device before the system goes read-only");

        general_options.addOptionChaining("logDir", "logDir", moe::String,
                "directory to store transaction log files (default is --dbpath)");

        general_options.addOptionChaining("tmpDir", "tmpDir", moe::String,
                "directory to store temporary bulk loader files (default is --dbpath)");

        general_options.addOptionChaining("directoryperdb", "directoryperdb", moe::Switch,
                "each database will be stored in a separate directory")
                                         .hidden();

        general_options.addOptionChaining("ipv6", "ipv6", moe::Switch,
                "enable IPv6 support (disabled by default)");

        general_options.addOptionChaining("journal", "journal", moe::Switch, "enable journaling")
                                         .hidden();

        general_options.addOptionChaining("journalCommitInterval", "journalCommitInterval",
                moe::Unsigned, "how often to group/batch commit (ms)")
                                         .hidden();

        general_options.addOptionChaining("journalOptions", "journalOptions", moe::Int,
                "journal diagnostic options")
                                         .hidden();

        general_options.addOptionChaining("logFlushPeriod", "logFlushPeriod",
                moe::Unsigned, "how often to fsync recovery log (ms)");

        general_options.addOptionChaining("jsonp", "jsonp", moe::Switch,
                "allow JSONP access via http (has security implications)");

        general_options.addOptionChaining("lockTimeout", "lockTimeout", moe::UnsignedLongLong,
                "tokumx row lock wait timeout (in ms), 0 means wait as long as necessary");

        general_options.addOptionChaining("locktreeMaxMemory", "locktreeMaxMemory", moe::UnsignedLongLong,
                "tokumx memory limit (in bytes) for storing transactions' row locks");

        general_options.addOptionChaining("loaderMaxMemory", "loaderMaxMemory", moe::UnsignedLongLong,
                "tokumx memory limit (in bytes) for a single bulk loader to use. the bulk loader is used to build foreground indexes and is also utilized by mongorestore/import");

        general_options.addOptionChaining("loaderCompressTmp", "loaderCompressTmp", moe::Switch,
                "the bulk loader (used for mongoimport/mongorestore and non-background index builds) will compress intermediate files (see tmpDir) when writing them to disk")
                                         .setDefault(moe::Value(true));

        general_options.addOptionChaining("noauth", "noauth", moe::Switch, "run without security");

        general_options.addOptionChaining("noIndexBuildRetry", "noIndexBuildRetry", moe::Switch,
                "don't retry any index builds that were interrupted by shutdown")
                                         .hidden();

        general_options.addOptionChaining("nojournal", "nojournal", moe::Switch,
                "disable journaling (journaling is on by default for 64 bit)")
                                         .hidden();

        general_options.addOptionChaining("noprealloc", "noprealloc", moe::Switch,
                "disable data file preallocation - will often hurt performance")
                                         .hidden();

        general_options.addOptionChaining("noscripting", "noscripting", moe::Switch,
                "disable scripting engine");

        general_options.addOptionChaining("notablescan", "notablescan", moe::Switch,
                "do not allow table scans");

        general_options.addOptionChaining("nssize", "nssize", moe::Int,
                ".ns file size (in MB) for new databases")
                                         .hidden();

        general_options.addOptionChaining("profile", "profile", moe::Int, "0=off 1=slow, 2=all");

        general_options.addOptionChaining("quota", "quota", moe::Switch,
                "limits each database to a certain number of files (8 default)");

        general_options.addOptionChaining("quotaFiles", "quotaFiles", moe::Int,
                "number of files allowed per db, requires --quota");

        general_options.addOptionChaining("repair", "repair", moe::Switch, "run repair on all dbs")
                                         .hidden();

        general_options.addOptionChaining("repairpath", "repairpath", moe::String,
                "root directory for repair files - defaults to dbpath")
                                         .hidden();

        general_options.addOptionChaining("rest", "rest", moe::Switch, "turn on simple rest api");

#if defined(__linux__)
        general_options.addOptionChaining("shutdown", "shutdown", moe::Switch,
                "kill a running server (for init scripts)");

#endif
        general_options.addOptionChaining("slowms", "slowms", moe::Int,
                "value of slow for profile and console log")
                                         .setDefault(moe::Value(100));

        general_options.addOptionChaining("smallfiles", "smallfiles", moe::Switch,
                "use a smaller default file size")
                                         .hidden();

        general_options.addOptionChaining("syncdelay", "syncdelay", moe::Double,
                "seconds between disk syncs (0=never, but not recommended)")
                                         .setDefault(moe::Value(60.0))
                                         .hidden();

        general_options.addOptionChaining("sysinfo", "sysinfo", moe::Switch,
                "print some diagnostic system information");

        general_options.addOptionChaining("upgrade", "upgrade", moe::Switch,
                "upgrade db if needed")
                                         .hidden();


        replication_options.addOptionChaining("oplogSize", "oplogSize", moe::Int,
                "size to use (in MB) for replication op log. default is 5% of disk space "
                "(i.e. large is good)")
                                         .hidden();

        replication_options.addOptionChaining("expireOplogDays", "expireOplogDays",
                moe::Unsigned, "how many days of oplog data to keep");

        replication_options.addOptionChaining("expireOplogHours", "expireOplogHours",
                moe::Unsigned, "how many hours, in addition to expireOplogDays, of oplog data to keep");

        replication_options.addOptionChaining("txnMemLimit", "txnMemLimit", moe::UnsignedLongLong,
                "limit of the size of a transaction's operation");


        ms_options.addOptionChaining("master", "master", moe::Switch, "master mode")
                                         .hidden();

        ms_options.addOptionChaining("slave", "slave", moe::Switch, "slave mode")
                                         .hidden();

        ms_options.addOptionChaining("source", "source", moe::String,
                "when slave: specify master as <server:port>")
                                         .hidden();

        ms_options.addOptionChaining("only", "only", moe::String,
                "when slave: specify a single database to replicate")
                                         .hidden();

        ms_options.addOptionChaining("slavedelay", "slavedelay", moe::Int,
                "specify delay (in seconds) to be used when applying master ops to slave");

        ms_options.addOptionChaining("autoresync", "autoresync", moe::Switch,
                "automatically resync if slave data is stale")
                                    .hidden();


        rs_options.addOptionChaining("replSet", "replSet", moe::String,
                "arg is <setname>[/<optionalseedhostlist>]");

        rs_options.addOptionChaining("replIndexPrefetch", "replIndexPrefetch", moe::String,
                "specify index prefetching behavior (if secondary) [none|_id_only|all]")
                                         .hidden();


        sharding_options.addOptionChaining("configsvr", "configsvr", moe::Switch,
                "declare this is a config db of a cluster; default port 27019; "
                "default dir /data/configdb");

        sharding_options.addOptionChaining("shardsvr", "shardsvr", moe::Switch,
                "declare this is a shard db of a cluster; default port 27018");


        sharding_options.addOptionChaining("noMoveParanoia", "noMoveParanoia", moe::Switch,
                "turn off paranoid saving of data for the moveChunk command; default")
                                          .hidden();

        sharding_options.addOptionChaining("moveParanoia", "moveParanoia", moe::Switch,
                "turn on paranoid saving of data during the moveChunk command "
                "(used for internal system diagnostics)")
                                          .hidden();

        options->addSection(general_options);
#if defined(_WIN32)
        options->addSection(windows_scm_options);
#endif
        options->addSection(replication_options);
        options->addSection(ms_options);
        options->addSection(rs_options);
        options->addSection(sharding_options);
#ifdef MONGO_SSL
        options->addSection(ssl_options);
#endif

        options->addOptionChaining("fastsync", "fastsync", moe::Switch,
                "indicate that this instance is starting from a dbpath snapshot of the repl peer")
                                  .hidden();

        options->addOptionChaining("pretouch", "pretouch", moe::Int,
                "n pretouch threads for applying master/slave operations")
                                  .hidden();

        // This is a deprecated option that we are supporting for backwards compatibility
        // The first value for this option can be either 'dbpath' or 'run'.
        // If it is 'dbpath', mongod prints the dbpath and exits.  Any extra values are ignored.
        // If it is 'run', mongod runs normally.  Providing extra values is an error.
        options->addOptionChaining("command", "command", moe::StringVector, "command")
                                  .hidden()
                                  .positional(1, 3);

        options->addOptionChaining("nodur", "nodur", moe::Switch, "disable journaling")
                                  .hidden();

        // things we don't want people to use
        options->addOptionChaining("nohints", "nohints", moe::Switch, "ignore query hints")
                                  .hidden();

        options->addOptionChaining("nopreallocj", "nopreallocj", moe::Switch,
                "don't preallocate journal files")
                                  .hidden();

        options->addOptionChaining("dur", "dur", moe::Switch, "enable journaling")
                                  .hidden();

        options->addOptionChaining("durOptions", "durOptions", moe::Int,
                "durability diagnostic options")
                                  .hidden();

        // deprecated pairing command line options
        options->addOptionChaining("pairwith", "pairwith", moe::Switch, "DEPRECATED")
                                  .hidden();

        options->addOptionChaining("arbiter", "arbiter", moe::Switch, "DEPRECATED")
                                  .hidden();

        options->addOptionChaining("opIdMem", "opIdMem", moe::Switch, "DEPRECATED")
                                  .hidden();


        // TokuMX unreleased options
        options->addOptionChaining("fastupdates", "fastupdates", moe::Switch,
                "internal only")
                                  .hidden();

        options->addOptionChaining("fastupdatesIgnoreErrors", "fastupdatesIgnoreErrors", moe::Switch,
                "silently ignore all fastupdate errors. NOT RECOMMENDED FOR PRODUCTION, unless failed updates are expected and/or acceptable.")
                                  .hidden();


        return Status::OK();
    }

    void printMongodHelp(const moe::OptionSection& options) {
        std::cout << options.helpString() << std::endl;
    };

    namespace {
        void sysRuntimeInfo() {
            out() << "sysinfo:" << endl;
#if defined(_SC_PAGE_SIZE)
            out() << "  page size: " << (int) sysconf(_SC_PAGE_SIZE) << endl;
#endif
#if defined(_SC_PHYS_PAGES)
            out() << "  _SC_PHYS_PAGES: " << sysconf(_SC_PHYS_PAGES) << endl;
#endif
#if defined(_SC_AVPHYS_PAGES)
            out() << "  _SC_AVPHYS_PAGES: " << sysconf(_SC_AVPHYS_PAGES) << endl;
#endif
        }
    } // namespace

    bool handlePreValidationMongodOptions(const moe::Environment& params,
                                            const std::vector<std::string>& args) {
        if (params.count("help")) {
            printMongodHelp(moe::startupOptions);
            return true;
        }
        if (params.count("version")) {
            cout << mongodVersion() << endl;
            printGitVersion();
            printOpenSSLVersion();
            return true;
        }
        if (params.count("sysinfo")) {
            sysRuntimeInfo();
            return true;
        }

        return false;
    }

    Status storeMongodOptions(const moe::Environment& params,
                              const std::vector<std::string>& args) {

        Status ret = storeServerOptions(params, args);
        if (!ret.isOK()) {
            return ret;
        }

        if (params.count("dbpath")) {
            storageGlobalParams.dbpath = params["dbpath"].as<string>();
            if (params.count("fork") && storageGlobalParams.dbpath[0] != '/') {
                // we need to change dbpath if we fork since we change
                // cwd to "/"
                // fork only exists on *nix
                // so '/' is safe
                storageGlobalParams.dbpath = serverGlobalParams.cwd + "/" +
                                                 storageGlobalParams.dbpath;
            }
        }
#ifdef _WIN32
        if (storageGlobalParams.dbpath.size() > 1 &&
            storageGlobalParams.dbpath[storageGlobalParams.dbpath.size()-1] == '/') {
            // size() check is for the unlikely possibility of --dbpath "/"
            storageGlobalParams.dbpath =
                storageGlobalParams.dbpath.erase(storageGlobalParams.dbpath.size()-1);
        }
#endif
        if ( params.count("slowms")) {
            serverGlobalParams.slowMS = params["slowms"].as<int>();
        }

        if ( params.count("syncdelay")) {
            storageGlobalParams.syncdelay = params["syncdelay"].as<double>();
        }

        if (params.count("directoryperdb")) {
            return Status(ErrorCodes::BadValue,
                          "directoryperdb is deprecated in TokuMX");
        }
        if (params.count("cpu")) {
            serverGlobalParams.cpu = true;
        }
        if (params.count("noauth")) {
            AuthorizationManager::setAuthEnabled(false);
        }
        if (params.count("auth")) {
            AuthorizationManager::setAuthEnabled(true);
        }
        if (params.count("quota")) {
            storageGlobalParams.quota = true;
        }
        if (params.count("quotaFiles")) {
            storageGlobalParams.quota = true;
            storageGlobalParams.quotaFiles = params["quotaFiles"].as<int>() - 1;
        }
        if ((params.count("nodur") || params.count("nojournal")) &&
            (params.count("dur") || params.count("journal"))) {
            return Status(ErrorCodes::BadValue,
                          "Can't specify both --journal and --nojournal options.");
        }

        if (params.count("nodur") || params.count("nojournal")) {
            return Status(ErrorCodes::BadValue,
                          "nodur and nojournal are deprecated in TokuMX");
        }

        if (params.count("dur") || params.count("journal")) {
            return Status(ErrorCodes::BadValue,
                          "dur and journal are deprecated in TokuMX");
        }

        if (params.count("durOptions")) {
            return Status(ErrorCodes::BadValue,
                          "durOptions is deprecated in TokuMX");
        }
        if (params.count("journalOptions")) {
            return Status(ErrorCodes::BadValue,
                          "journalOptions is deprecated in TokuMX");
        }
        if (params.count("nohints")) {
            storageGlobalParams.useHints = false;
        }
        if (params.count("nopreallocj")) {
            return Status(ErrorCodes::BadValue,
                          "nopreallocj is deprecated in TokuMX");
        }
        if (params.count("httpinterface")) {
            if (params.count("nohttpinterface")) {
                return Status(ErrorCodes::BadValue,
                              "can't have both --httpinterface and --nohttpinterface");
            }
            serverGlobalParams.isHttpInterfaceEnabled = true;
        }
        // SERVER-10019 Enabling rest/jsonp without --httpinterface should break in the future
        if (params.count("rest")) {
            if (params.count("nohttpinterface")) {
                log() << "** WARNING: Should not specify both --rest and --nohttpinterface" <<
                    startupWarningsLog;
            }
            else if (!params.count("httpinterface")) {
                log() << "** WARNING: --rest is specified without --httpinterface," <<
                    startupWarningsLog;
                log() << "**          enabling http interface" << startupWarningsLog;
                serverGlobalParams.isHttpInterfaceEnabled = true;
            }
            serverGlobalParams.rest = true;
        }
        if (params.count("jsonp")) {
            if (params.count("nohttpinterface")) {
                log() << "** WARNING: Should not specify both --jsonp and --nohttpinterface" <<
                    startupWarningsLog;
            }
            else if (!params.count("httpinterface")) {
                log() << "** WARNING --jsonp is specified without --httpinterface," <<
                    startupWarningsLog;
                log() << "**         enabling http interface" << startupWarningsLog;
                serverGlobalParams.isHttpInterfaceEnabled = true;
            }
            serverGlobalParams.jsonp = true;
        }
        if (params.count("noscripting")) {
            mongodGlobalParams.scriptingEnabled = false;
        }
        if (params.count("noprealloc")) {
            return Status(ErrorCodes::BadValue,
                          "noprealloc is deprecated in TokuMX");
        }
        if (params.count("smallfiles")) {
            return Status(ErrorCodes::BadValue,
                          "smallfiles is deprecated in TokuMX");
        }
        if (params.count("diaglog")) {
            int x = params["diaglog"].as<int>();
            if ( x < 0 || x > 7 ) {
                return Status(ErrorCodes::BadValue, "can't interpret --diaglog setting");
            }
            _diaglog.setLevel(x);
        }

        if ((params.count("dur") || params.count("journal")) && params.count("repair")) {
            return Status(ErrorCodes::BadValue,
                          "Can't specify both --journal and --repair options.");
        }

        if (params.count("repair")) {
            return Status(ErrorCodes::BadValue,
                          "repair is deprecated in TokuMX");
        }
        if (params.count("upgrade")) {
            return Status(ErrorCodes::BadValue,
                          "upgrade is deprecated in TokuMX");
        }
        if (params.count("notablescan")) {
            storageGlobalParams.noTableScan = true;
        }
        if (params.count("master")) {
            return Status(ErrorCodes::BadValue,
                          "master is deprecated in TokuMX");
        }
        if (params.count("slave")) {
            return Status(ErrorCodes::BadValue,
                          "slave is deprecated in TokuMX");
        }
        if (params.count("slavedelay")) {
            replSettings.slavedelay = params["slavedelay"].as<int>();
        }
        if (params.count("fastsync")) {
            replSettings.fastsync = true;
        }
        if (params.count("rs_maintenance")) {
            replSettings.startInRecovery = true;
        }
        if (params.count("autoresync")) {
            replSettings.autoresync = true;
            if( params.count("replSet") ) {
                return Status(ErrorCodes::BadValue,
                              "--autoresync is not used with --replSet\nsee "
                              "http://dochub.mongodb.org/core/resyncingaverystalereplicasetmember");
            }
        }
        if (params.count("source")) {
            return Status(ErrorCodes::BadValue,
                          "source is deprecated in TokuMX");
        }
        if( params.count("pretouch") ) {
            return Status(ErrorCodes::BadValue,
                          "pretouch is deprecated in TokuMX");
        }
        if (params.count("replSet")) {
            if (params.count("slavedelay")) {
                return Status(ErrorCodes::BadValue, "--slavedelay cannot be used with --replSet");
            }
            else if (params.count("only")) {
                return Status(ErrorCodes::BadValue, "--only cannot be used with --replSet");
            }
            /* seed list of hosts for the repl set */
            replSettings.replSet = params["replSet"].as<string>().c_str();
        }
        if (params.count("replIndexPrefetch")) {
            return Status(ErrorCodes::BadValue,
                          "replIndexPrefetch is deprecated in TokuMX");
        }
        if (params.count("noIndexBuildRetry")) {
            return Status(ErrorCodes::BadValue,
                          "noIndexBuildRetry is deprecated in TokuMX");
        }
        if (params.count("only")) {
            return Status(ErrorCodes::BadValue,
                          "only is deprecated in TokuMX");
        }
        if( params.count("nssize") ) {
            return Status(ErrorCodes::BadValue,
                          "nssize is deprecated in TokuMX");
        }
        if (params.count("oplogSize")) {
            return Status(ErrorCodes::BadValue,
                          "oplogSize is deprecated in TokuMX");
        }
        if (params.count("cacheSize")) {
            long x = params["cacheSize"].as<long>();
            if (x <= 0) {
                return Status(ErrorCodes::BadValue, "bad --cacheSize arg");
            }
            return Status(ErrorCodes::BadValue, "--cacheSize option not currently supported");
        }
        if (!params.count("port")) {
            if( params.count("configsvr") ) {
                serverGlobalParams.port = ServerGlobalParams::ConfigServerPort;
            }
            if( params.count("shardsvr") ) {
                if( params.count("configsvr") ) {
                    return Status(ErrorCodes::BadValue,
                                  "can't do --shardsvr and --configsvr at the same time");
                }
                serverGlobalParams.port = ServerGlobalParams::ShardServerPort;
            }
        }
        else {
            if (serverGlobalParams.port <= 0 || serverGlobalParams.port > 65535) {
                return Status(ErrorCodes::BadValue, "bad --port number");
            }
        }
        if ( params.count("configsvr" ) ) {
            serverGlobalParams.configsvr = true;
            if (replSettings.usingReplSets()) {
                return Status(ErrorCodes::BadValue,
                              "replication should not be enabled on a config server");
            }
            if (!params.count("dbpath"))
                storageGlobalParams.dbpath = "/data/configdb";
        }
        if ( params.count( "profile" ) ) {
            serverGlobalParams.defaultProfile = params["profile"].as<int>();
        }
        if (params.count("ipv6")) {
            enableIPv6();
        }

        if (params.count("noMoveParanoia") && params.count("moveParanoia")) {
            return Status(ErrorCodes::BadValue,
                          "The moveParanoia and noMoveParanoia flags cannot both be set");
        }

        if (params.count("noMoveParanoia")) {
            return Status(ErrorCodes::BadValue,
                          "noMoveParanoia is deprecated in TokuMX");
        }

        if (params.count("moveParanoia")) {
            return Status(ErrorCodes::BadValue,
                          "moveParanoia is deprecated in TokuMX");
        }

        if (params.count("pairwith") || params.count("arbiter") || params.count("opIdMem")) {
            return Status(ErrorCodes::BadValue,
                          "****\n"
                          "Replica Pairs have been deprecated. Invalid options: "
                              "--pairwith, --arbiter, and/or --opIdMem\n"
                          "<http://dochub.mongodb.org/core/replicapairs>\n"
                          "****");
        }

        if (params.count("journalCommitInterval")) {
            log() << "--journalCommitInterval deprecated, treating as --logFlushPeriod" << startupWarningsLog;
            storageGlobalParams.logFlushPeriod = params["journalCommitInterval"].as<unsigned>();
            if (storageGlobalParams.logFlushPeriod > 300) {
                return Status(ErrorCodes::BadValue,
                              "--logFlushPeriod out of allowed range (0-300ms)");
            }
        }
        if (params.count("logFlushPeriod")) {
            storageGlobalParams.logFlushPeriod = params["logFlushPeriod"].as<unsigned>();
            if (storageGlobalParams.logFlushPeriod > 300) {
                return Status(ErrorCodes::BadValue,
                              "--logFlushPeriod out of allowed range (0-300ms)");
            }
        }
        if (!(params.count("expireOplogHours") || params.count("expireOplogDays")) && params.count("replSet")) {
            log() << "*****************************" << startupWarningsLog;
            log() << "No value set for expireOplogDays, using default of " << replSettings.expireOplogDays << " days." << startupWarningsLog;
            log() << "*****************************" << startupWarningsLog;
        }
        if( params.count("expireOplogHours") ) {
            replSettings.expireOplogHours = params["expireOplogHours"].as<int>();
            // if expireOplogHours is set, we don't want to use the default
            // value of expireOplogDays. We want to use 0. If the user
            // sets the value of expireOplogDays as well, next if-clause
            // below will catch it
            if (!params.count("expireOplogDays")) {
                replSettings.expireOplogDays = 0;
                log() << "*****************************" << startupWarningsLog;
                log() << "No value set for expireOplogDays, only for expireOplogHours. Having at least 1 day set for expireOplogDays is recommended." << startupWarningsLog;
                log() << "*****************************" << startupWarningsLog;
            }
        }
        if( params.count("expireOplogDays") ) {
            replSettings.expireOplogDays = params["expireOplogDays"].as<int>();
        }
        if (params.count("directio")) {
            storageGlobalParams.directio = true;
        }
        if (params.count("fastupdates")) {
            storageGlobalParams.fastupdates = true;
        }
        if (params.count("fastupdatesIgnoreErrors")) {
            storageGlobalParams.fastupdatesIgnoreErrors = true;
        }
        if (params.count("checkpointPeriod")) {
            storageGlobalParams.checkpointPeriod = params["checkpointPeriod"].as<int>();
        }
        if (params.count("cleanerPeriod")) {
            storageGlobalParams.cleanerPeriod = params["cleanerPeriod"].as<int>();
        }
        if (params.count("cleanerIterations")) {
            storageGlobalParams.cleanerIterations = params["cleanerIterations"].as<int>();
        }
        if (params.count("fsRedzone")) {
            storageGlobalParams.fsRedzone = params["fsRedzone"].as<int>();
            if (storageGlobalParams.fsRedzone < 1 || storageGlobalParams.fsRedzone > 99) {
                return Status(ErrorCodes::BadValue,
                              "--fsRedzone must be between 1 and 99.");
            }
        }
        if (params.count("logDir")) {
            storageGlobalParams.logDir = params["logDir"].as<string>();
            if ( storageGlobalParams.logDir[0] != '/' ) {
                // we need to change dbpath if we fork since we change
                // cwd to "/"
                // fork only exists on *nix
                // so '/' is safe
                storageGlobalParams.logDir = serverGlobalParams.cwd + "/" + storageGlobalParams.logDir;
            }
        }
        if (params.count("tmpDir")) {
            storageGlobalParams.tmpDir = params["tmpDir"].as<string>();
            if ( storageGlobalParams.tmpDir[0] != '/' ) {
                // we need to change dbpath if we fork since we change
                // cwd to "/"
                // fork only exists on *nix
                // so '/' is safe
                storageGlobalParams.tmpDir = serverGlobalParams.cwd + "/" + storageGlobalParams.tmpDir;
            }
        }
        if (params.count("txnMemLimit")) {
            unsigned long long limit = params["txnMemLimit"].as<unsigned long long>();
            if (limit > (2ULL << 20)) {
                return Status(ErrorCodes::BadValue,
                              "--txnMemLimit cannot be greater than 2MB");
            }
            storageGlobalParams.txnMemLimit = limit;
        }
        if (params.count("loaderMaxMemory")) {
            unsigned long long x = params["loaderMaxMemory"].as<unsigned long long>();
            if (x < (32ULL << 20)) {
                return Status(ErrorCodes::BadValue,
                              "bad --loaderMaxMemory arg (should never be less than 32mb)");
            }
            storageGlobalParams.loaderMaxMemory = x;
        }
        if (params.count("locktreeMaxMemory")) {
            unsigned long long x = params["locktreeMaxMemory"].as<unsigned long long>();
            if (x < (64ULL << 10)) {
                return Status(ErrorCodes::BadValue,
                              "bad --locktreeMaxMemory arg (should never be less than 64kb)");
            }
            storageGlobalParams.locktreeMaxMemory = x;
        }

        if (sizeof(void*) == 4) {
            // trying to make this stand out more like startup warnings
            log() << endl;
            warning() << "32-bit servers don't have journaling enabled by default. "
                      << "Please use --journal if you want durability." << endl;
            log() << endl;
        }

        return Status::OK();
    }

} // namespace mongo
