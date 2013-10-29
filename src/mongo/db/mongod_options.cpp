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
#include "mongo/util/version_reporting.h"

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

        // Authentication Options

        // Way to enable or disable auth on command line and in Legacy config file
        general_options.addOptionChaining("auth", "auth", moe::Switch, "run with security")
                                         .setSources(moe::SourceAllLegacy)
                                         .incompatibleWith("security.authentication");

        general_options.addOptionChaining("noauth", "noauth", moe::Switch, "run without security")
                                         .setSources(moe::SourceAllLegacy)
                                         .incompatibleWith("security.authentication");

        // Way to enable or disable auth in JSON Config
        general_options.addOptionChaining("security.authentication", "", moe::String,
                "How the database behaves with respect to authentication of clients.  "
                "Options are \"optional\", which means that a client can connect with or without "
                "authentication, and \"required\" which means clients must use authentication")
                                         .setSources(moe::SourceYAMLConfig)
                                         .incompatibleWith("auth")
                                         .incompatibleWith("noauth")
                                         .format("(:?optional)|(:?required)",
                                                 "(optional/required)");

        // setParameter parameters that we want as config file options
        // TODO: Actually read these into our environment.  Currently they have no effect
        general_options.addOptionChaining("security.authSchemaVersion", "", moe::String, "TODO")
                                         .setSources(moe::SourceYAMLConfig);

        general_options.addOptionChaining("security.authenticationMechanisms", "", moe::String,
                "TODO")
                                         .setSources(moe::SourceYAMLConfig);

        general_options.addOptionChaining("security.enableLocalhostAuthBypass", "", moe::String,
                "TODO")
                                         .setSources(moe::SourceYAMLConfig);

        general_options.addOptionChaining("security.supportCompatibilityFormPrivilegeDocuments", "",
                moe::String, "TODO")
                                         .setSources(moe::SourceYAMLConfig);

        // Network Options

        general_options.addOptionChaining("net.ipv6", "ipv6", moe::Switch,
                "enable IPv6 support (disabled by default)");

        general_options.addOptionChaining("net.http.JSONPEnabled", "jsonp", moe::Switch,
                "allow JSONP access via http (has security implications)");

        general_options.addOptionChaining("net.http.RESTInterfaceEnabled", "rest", moe::Switch,
                "turn on simple rest api");

        // Diagnostic Options

        general_options.addOptionChaining("diaglog", "diaglog", moe::Int,
                "DEPRECATED: 0=off 1=W 2=R 3=both 7=W+some reads")
                                         .hidden()
                                         .setSources(moe::SourceAllLegacy);

        general_options.addOptionChaining("operationProfiling.slowOpThresholdMs", "slowms",
                moe::Int, "value of slow for profile and console log")
                                         .setDefault(moe::Value(100));

        general_options.addOptionChaining("profile", "profile", moe::Int,
                "0=off 1=slow, 2=all")
                                         .setSources(moe::SourceAllLegacy);

        general_options.addOptionChaining("operationProfiling.mode", "", moe::Int,
                "(off/slowOp/all)")
                                         .setSources(moe::SourceYAMLConfig);

        general_options.addOptionChaining("cpu", "cpu", moe::Switch,
                "periodically show cpu and iowait utilization")
                                         .setSources(moe::SourceAllLegacy);

        general_options.addOptionChaining("sysinfo", "sysinfo", moe::Switch,
                "print some diagnostic system information")
                                         .setSources(moe::SourceAllLegacy);

        // Storage Options

        general_options.addOptionChaining("storage.cacheSize", "cacheSize", moe::UnsignedLongLong,
                "tokumx cache size (in bytes) for data and indexes");

        general_options.addOptionChaining("storage.checkpointPeriod", "checkpointPeriod", moe::Unsigned,
                "tokumx time between checkpoints, 0 means never checkpoint");

        general_options.addOptionChaining("storage.cleanerIterations", "cleanerIterations", moe::Unsigned,
                "tokumx number of iterations per cleaner thread operation, 0 means never run");

        general_options.addOptionChaining("storage.cleanerPeriod", "cleanerPeriod", moe::Unsigned,
                "tokumx time between cleaner thread operations, 0 means never run");

#ifdef _WIN32
        general_options.addOptionChaining("storage.dbPath", "dbpath", moe::String,
                "directory for datafiles - defaults to \\data\\db\\")
                                         .setDefault(moe::Value(std::string("\\data\\db\\")));

#else
        general_options.addOptionChaining("storage.dbPath", "dbpath", moe::String,
                "directory for datafiles - defaults to /data/db/")
                                         .setDefault(moe::Value(std::string("/data/db")));

#endif
        general_options.addOptionChaining("storage.logDir", "logDir", moe::String,
                "directory to store transaction log files (default is --dbpath)");

        general_options.addOptionChaining("storage.tmpDir", "tmpDir", moe::String,
                "directory to store temporary bulk loader files (default is --dbpath)");

        general_options.addOptionChaining("storage.directoryPerDB", "directoryperdb", moe::Switch,
                "each database will be stored in a separate directory")
                                         .hidden();

        general_options.addOptionChaining("storage.directio", "directio", moe::Switch,
                "use direct I/O in tokumx");

        general_options.addOptionChaining("storage.fsRedzone", "fsRedzone", moe::Int,
                "percentage of free-space left on device before the system goes read-only");

        general_options.addOptionChaining("storage.loaderCompressTmp", "loaderCompressTmp", moe::Switch,
                "the bulk loader (used for mongoimport/mongorestore and non-background index builds) will compress intermediate files (see tmpDir) when writing them to disk")
                                         .setDefault(moe::Value(true));

        general_options.addOptionChaining("storage.loaderMaxMemory", "loaderMaxMemory", moe::UnsignedLongLong,
                "tokumx memory limit (in bytes) for a single bulk loader to use. the bulk loader is used to build foreground indexes and is also utilized by mongorestore/import");
        general_options.addOptionChaining("storage.lockTimeout", "lockTimeout", moe::UnsignedLongLong,
                "tokumx row lock wait timeout (in ms), 0 means wait as long as necessary");

        general_options.addOptionChaining("storage.locktreeMaxMemory", "locktreeMaxMemory", moe::UnsignedLongLong,
                "tokumx memory limit (in bytes) for storing transactions' row locks");

        general_options.addOptionChaining("storage.logFlushPeriod", "logFlushPeriod",
                moe::Unsigned, "how often to fsync recovery log (ms)");

        general_options.addOptionChaining("noIndexBuildRetry", "noIndexBuildRetry", moe::Switch,
                "don't retry any index builds that were interrupted by shutdown")
                                         .hidden()
                                         .setSources(moe::SourceAllLegacy);

        general_options.addOptionChaining("storage.indexBuildRetry", "", moe::Bool,
                "don't retry any index builds that were interrupted by shutdown")
                                         .hidden()
                                         .setSources(moe::SourceYAMLConfig);

        general_options.addOptionChaining("noprealloc", "noprealloc", moe::Switch,
                "disable data file preallocation - will often hurt performance")
                                         .hidden()
                                         .setSources(moe::SourceAllLegacy);

        general_options.addOptionChaining("storage.preallocDataFiles", "", moe::Bool,
                "disable data file preallocation - will often hurt performance")
                                         .hidden()
                                         .setSources(moe::SourceYAMLConfig);

        general_options.addOptionChaining("storage.nsSize", "nssize", moe::Int,
                ".ns file size (in MB) for new databases")
                                         .hidden()
                                         .setDefault(moe::Value(16));

        general_options.addOptionChaining("storage.quota.enforced", "quota", moe::Switch,
                "limits each database to a certain number of files (8 default)")
                                         .setSources(moe::SourceAllLegacy)
                                         .incompatibleWith("keyFile");

        general_options.addOptionChaining("storage.quota.maxFilesPerDB", "quotaFiles", moe::Int,
                "number of files allowed per db, implies --quota");

        general_options.addOptionChaining("storage.smallFiles", "smallfiles", moe::Switch,
                "use a smaller default file size")
                                         .hidden();

        general_options.addOptionChaining("storage.syncPeriodSecs", "syncdelay", moe::Double,
                "seconds between disk syncs (0=never, but not recommended)")
                                         .setDefault(moe::Value(60.0));

        // Upgrade and repair are disallowed in JSON configs since they trigger very heavyweight
        // actions rather than specify configuration data
        general_options.addOptionChaining("upgrade", "upgrade", moe::Switch,
                "upgrade db if needed")
                                         .hidden()
                                         .setSources(moe::SourceAllLegacy);

        general_options.addOptionChaining("repair", "repair", moe::Switch,
                "run repair on all dbs")
                                         .hidden()
                                         .setSources(moe::SourceAllLegacy);

        general_options.addOptionChaining("storage.repairPath", "repairpath", moe::String,
                "root directory for repair files - defaults to dbpath")
                                         .hidden();

        // Javascript Options

        general_options.addOptionChaining("noscripting", "noscripting", moe::Switch,
                "disable scripting engine")
                                         .setSources(moe::SourceAllLegacy);

        // Query Options

        general_options.addOptionChaining("notablescan", "notablescan", moe::Switch,
                "do not allow table scans")
                                         .setSources(moe::SourceAllLegacy);

        // Journaling Options

        // Way to enable or disable journaling on command line and in Legacy config file
        general_options.addOptionChaining("journal", "journal", moe::Switch, "enable journaling")
                                         .hidden()
                                         .setSources(moe::SourceAllLegacy);

        general_options.addOptionChaining("nojournal", "nojournal", moe::Switch,
                "disable journaling (journaling is on by default for 64 bit)")
                                         .hidden()
                                         .setSources(moe::SourceAllLegacy);

        general_options.addOptionChaining("dur", "dur", moe::Switch, "enable journaling")
                                         .hidden()
                                         .setSources(moe::SourceAllLegacy);

        general_options.addOptionChaining("nodur", "nodur", moe::Switch, "disable journaling")
                                         .hidden()
                                         .setSources(moe::SourceAllLegacy);

        // Way to enable or disable journaling in JSON Config
        general_options.addOptionChaining("storage.journal.enabled", "", moe::Bool,
                "enable journaling")
                                         .hidden()
                                         .setSources(moe::SourceYAMLConfig);

        // Two ways to set durability diagnostic options.  durOptions is deprecated
        general_options.addOptionChaining("storage.journal.debugFlags", "journalOptions", moe::Int,
                "journal diagnostic options")
                                         .hidden()
                                         .incompatibleWith("durOptions");

        general_options.addOptionChaining("durOptions", "durOptions", moe::Int,
                "durability diagnostic options")
                                         .hidden()
                                         .setSources(moe::SourceAllLegacy)
                                         .incompatibleWith("storage.journal.debugFlags");

        general_options.addOptionChaining("storage.journal.commitIntervalMs",
                "journalCommitInterval", moe::Unsigned, "how often to group/batch commit (ms)")
                                         .hidden();

        // Deprecated option that we don't want people to use for performance reasons
        options->addOptionChaining("nopreallocj", "nopreallocj", moe::Switch,
                "don't preallocate journal files")
                                  .hidden()
                                  .setSources(moe::SourceAllLegacy);

#if defined(__linux__)
        general_options.addOptionChaining("shutdown", "shutdown", moe::Switch,
                "kill a running server (for init scripts)");

#endif

        // Master Slave Options

        ms_options.addOptionChaining("master", "master", moe::Switch, "master mode")
                                    .setSources(moe::SourceAllLegacy)
                                    .hidden();

        ms_options.addOptionChaining("slave", "slave", moe::Switch, "slave mode")
                                    .setSources(moe::SourceAllLegacy)
                                    .hidden();

        ms_options.addOptionChaining("source", "source", moe::String,
                "when slave: specify master as <server:port>")
                                    .setSources(moe::SourceAllLegacy)
                                    .hidden();

        ms_options.addOptionChaining("only", "only", moe::String,
                "when slave: specify a single database to replicate")
                                    .setSources(moe::SourceAllLegacy)
                                    .hidden();

        ms_options.addOptionChaining("slavedelay", "slavedelay", moe::Int,
                "specify delay (in seconds) to be used when applying master ops to slave")
                                    .setSources(moe::SourceAllLegacy);

        ms_options.addOptionChaining("autoresync", "autoresync", moe::Switch,
                "automatically resync if slave data is stale")
                                    .setSources(moe::SourceAllLegacy)
                                    .hidden();

        // Replication Options

        replication_options.addOptionChaining("replication.expireOplogDays", "expireOplogDays",
                moe::Unsigned, "how many days of oplog data to keep");

        replication_options.addOptionChaining("replication.expireOplogHours", "expireOplogHours",
                moe::Unsigned, "how many hours, in addition to expireOplogDays, of oplog data to keep");

        replication_options.addOptionChaining("replication.txnMemLimit", "txnMemLimit", moe::UnsignedLongLong,
                "limit of the size of a transaction's operation");

        replication_options.addOptionChaining("replication.oplogSizeMB", "oplogSize", moe::Int,
                "size to use (in MB) for replication op log. default is 5% of disk space "
                "(i.e. large is good)")
                                    .hidden();

        rs_options.addOptionChaining("replication.replSet", "replSet", moe::String,
                "arg is <setname>[/<optionalseedhostlist>]")
                                    .setSources(moe::SourceAllLegacy)
                                    .incompatibleWith("replication.replSetName");

        rs_options.addOptionChaining("replication.replSetName", "", moe::String, "arg is <setname>")
                                    .setSources(moe::SourceYAMLConfig)
                                    .format("[^/]", "[replica set name with no \"/\"]")
                                    .incompatibleWith("replication.replSet");

        rs_options.addOptionChaining("replication.secondaryIndexPrefetch", "replIndexPrefetch", moe::String,
                "specify index prefetching behavior (if secondary) [none|_id_only|all]")
                                    .hidden()
                                    .format("(:?none)|(:?_id_only)|(:?all)",
                                            "(none/_id_only/all)");

        // Sharding Options

        sharding_options.addOptionChaining("sharding.configsvr", "configsvr", moe::Switch,
                "declare this is a config db of a cluster; default port 27019; "
                "default dir /data/configdb")
                                          .setSources(moe::SourceAllLegacy)
                                          .incompatibleWith("sharding.clusterRole");

        sharding_options.addOptionChaining("sharding.shardsvr", "shardsvr", moe::Switch,
                "declare this is a shard db of a cluster; default port 27018")
                                          .setSources(moe::SourceAllLegacy)
                                          .incompatibleWith("sharding.clusterRole");

        sharding_options.addOptionChaining("sharding.clusterRole", "", moe::String,
                "Choose what role this mongod has in a sharded cluster.  Possible values are:\n"
                "    \"configsvr\": Start this node as a config server.  Starts on port 27019 by "
                "default."
                "    \"shardsvr\": Start this node as a shard server.  Starts on port 27018 by "
                "default.")
                                          .setSources(moe::SourceYAMLConfig)
                                          .incompatibleWith("sharding.configsvr")
                                          .incompatibleWith("sharding.shardsvr")
                                          .format("(:?configsvr)|(:?shardsvr)",
                                                  "(configsvr/shardsvr)");

        sharding_options.addOptionChaining("sharding.noMoveParanoia", "noMoveParanoia", moe::Switch,
                "turn off paranoid saving of data for the moveChunk command; default")
                                          .hidden()
                                          .setSources(moe::SourceAllLegacy);

        sharding_options.addOptionChaining("sharding.archiveMovedChunks", "moveParanoia",
                moe::Switch, "turn on paranoid saving of data during the moveChunk command "
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

        // The following are legacy options that are disallowed in the JSON config file

        options->addOptionChaining("fastsync", "fastsync", moe::Switch,
                "indicate that this instance is starting from a dbpath snapshot of the repl peer")
                                  .hidden()
                                  .setSources(moe::SourceAllLegacy);

        options->addOptionChaining("pretouch", "pretouch", moe::Int,
                "n pretouch threads for applying master/slave operations")
                                  .hidden()
                                  .setSources(moe::SourceAllLegacy);

        // This is a deprecated option that we are supporting for backwards compatibility
        // The first value for this option can be either 'dbpath' or 'run'.
        // If it is 'dbpath', mongod prints the dbpath and exits.  Any extra values are ignored.
        // If it is 'run', mongod runs normally.  Providing extra values is an error.
        options->addOptionChaining("command", "command", moe::StringVector, "command")
                                  .hidden()
                                  .positional(1, 3)
                                  .setSources(moe::SourceAllLegacy);


        // things we don't want people to use
        options->addOptionChaining("nohints", "nohints", moe::Switch, "ignore query hints")
                                  .hidden()
                                  .setSources(moe::SourceAllLegacy);

        // deprecated pairing command line options
        options->addOptionChaining("pairwith", "pairwith", moe::Switch, "DEPRECATED")
                                  .hidden()
                                  .setSources(moe::SourceAllLegacy);

        options->addOptionChaining("arbiter", "arbiter", moe::Switch, "DEPRECATED")
                                  .hidden()
                                  .setSources(moe::SourceAllLegacy);

        options->addOptionChaining("opIdMem", "opIdMem", moe::Switch, "DEPRECATED")
                                  .hidden()
                                  .setSources(moe::SourceAllLegacy);


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
            return false;
        }
        if (params.count("version")) {
            cout << mongodVersion() << endl;
            printGitVersion();
            printOpenSSLVersion();
            return false;
        }
        if (params.count("sysinfo")) {
            sysRuntimeInfo();
            return false;
        }

        return true;
    }

    Status storeMongodOptions(const moe::Environment& params,
                              const std::vector<std::string>& args) {

        Status ret = storeServerOptions(params, args);
        if (!ret.isOK()) {
            return ret;
        }

        // TODO: Integrate these options with their setParameter counterparts
        if (params.count("security.authSchemaVersion")) {
            return Status(ErrorCodes::BadValue,
                          "security.authSchemaVersion is currently not supported in config files");
        }

        if (params.count("security.authenticationMechanisms")) {
            return Status(ErrorCodes::BadValue,
                          "security.authenticationMechanisms is currently not supported in config "
                          "files");
        }

        if (params.count("security.enableLocalhostAuthBypass")) {
            return Status(ErrorCodes::BadValue,
                          "security.enableLocalhostAuthBypass is currently not supported in config "
                          "files");
        }

        if (params.count("security.supportCompatibilityFormPrivilegeDocuments")) {
            return Status(ErrorCodes::BadValue,
                          "security.supportCompatibilityFormPrivilegeDocuments is currently not "
                          "supported in config files");
        }

        if (params.count("storage.dbPath")) {
            storageGlobalParams.dbpath = params["storage.dbPath"].as<string>();
            if (params.count("processManagement.fork") && storageGlobalParams.dbpath[0] != '/') {
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
        if ( params.count("operationProfiling.slowOpThresholdMs")) {
            serverGlobalParams.slowMS = params["operationProfiling.slowOpThresholdMs"].as<int>();
        }

        if ( params.count("storage.syncPeriodSecs")) {
            storageGlobalParams.syncdelay = params["storage.syncPeriodSecs"].as<double>();
        }

        if (params.count("storage.directoryPerDB")) {
            return Status(ErrorCodes::BadValue,
                          "directoryperdb is deprecated in TokuMX");
        }
        if (params.count("cpu")) {
            serverGlobalParams.cpu = true;
        }
        if (params.count("noauth") ||
            (params.count("security.authentication") &&
             params["security.authentication"].as<std::string>() == "optional")) {
            AuthorizationManager::setAuthEnabled(false);
        }
        if (params.count("auth") ||
            (params.count("security.authentication") &&
             params["security.authentication"].as<std::string>() == "required")) {
            AuthorizationManager::setAuthEnabled(true);
        }
        if (params.count("storage.quota.enforced")) {
            storageGlobalParams.quota = true;
        }
        if (params.count("storage.quota.maxFilesPerDB")) {
            storageGlobalParams.quota = true;
            storageGlobalParams.quotaFiles = params["storage.quota.maxFilesPerDB"].as<int>() - 1;
        }
        if ((params.count("nodur") || params.count("nojournal")) &&
            (params.count("dur") || params.count("journal"))) {
            return Status(ErrorCodes::BadValue,
                          "Can't specify both --journal and --nojournal options.");
        }

        // "storage.journal.enabled" comes from the config file, so check it before we check
        // "journal", "nojournal", "dur", and "nodur", since those come from the command line.
        if (params.count("storage.journal.enabled")) {
            return Status(ErrorCodes::BadValue,
                          "storage.journal.enabled is deprecated in TokuMX");
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
        if (params.count("storage.journal.debugFlags")) {
            return Status(ErrorCodes::BadValue,
                          "storage.journal.debugFlags is deprecated in TokuMX");
        }
        if (params.count("nohints")) {
            storageGlobalParams.useHints = false;
        }
        if (params.count("nopreallocj")) {
            return Status(ErrorCodes::BadValue,
                          "nopreallocj is deprecated in TokuMX");
        }

        // Check "net.http.enabled" before "httpinterface" and "nohttpinterface", since this comes
        // from a config file and those come from the command line
        if (params.count("net.http.enabled")) {
            serverGlobalParams.isHttpInterfaceEnabled = params["net.http.enabled"].as<bool>();
        }

        if (params.count("httpinterface")) {
            if (params.count("nohttpinterface")) {
                return Status(ErrorCodes::BadValue,
                              "can't have both --httpinterface and --nohttpinterface");
            }
            serverGlobalParams.isHttpInterfaceEnabled = true;
        }
        // SERVER-10019 Enabling rest/jsonp without --httpinterface should break in the future
        if (params.count("net.http.RESTInterfaceEnabled")) {

            // If we are explicitly setting httpinterface to false in the config file (the source of
            // "net.http.enabled") and not overriding it on the command line (the source of
            // "httpinterface"), then we can fail with an error message without breaking backwards
            // compatibility.
            if (!params.count("httpinterface") &&
                params.count("net.http.enabled") &&
                params["net.http.enabled"].as<bool>() == false) {
                return Status(ErrorCodes::BadValue,
                              "httpinterface must be enabled to use the rest api");
            }

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
        if (params.count("net.http.JSONPenabled")) {

            // If we are explicitly setting httpinterface to false in the config file (the source of
            // "net.http.enabled") and not overriding it on the command line (the source of
            // "httpinterface"), then we can fail with an error message without breaking backwards
            // compatibility.
            if (!params.count("httpinterface") &&
                params.count("net.http.enabled") &&
                params["net.http.enabled"].as<bool>() == false) {
                return Status(ErrorCodes::BadValue,
                              "httpinterface must be enabled to use jsonp");
            }

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
        if (params.count("noprealloc") ||
            (params.count("storage.preallocDataFiles") &&
             params["storage.preallocDataFiles"].as<bool>() == false)) {
            return Status(ErrorCodes::BadValue,
                          "noprealloc and storage.preallocDataFiles are deprecated in TokuMX");
        }
        if (params.count("storage.smallFiles")) {
            return Status(ErrorCodes::BadValue,
                          "storage.smallFiles is deprecated in TokuMX");
        }
        if (params.count("diaglog")) {
            warning() << "--diaglog is deprecated and will be removed in a future release";
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
            if( params.count("replication.replSet") ) {
                return Status(ErrorCodes::BadValue,
                              "--autoresync is not used with --replSet\nsee "
                              "http://dochub.mongodb.org/core/resyncingaverystalereplicasetmember");
            }
            if( params.count("replication.replSetName") ) {
                return Status(ErrorCodes::BadValue,
                              "--autoresync is not used with replication.replSetName\nsee "
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
        if (params.count("replication.replSetName")) {
            if (params.count("slavedelay")) {
                return Status(ErrorCodes::BadValue,
                              "--slavedelay cannot be used with replication.replSetName");
            }
            else if (params.count("only")) {
                return Status(ErrorCodes::BadValue,
                              "--only cannot be used with replication.replSetName");
            }
            replSettings.replSet = params["replication.replSetName"].as<string>().c_str();
        }
        if (params.count("replication.replSet")) {
            if (params.count("slavedelay")) {
                return Status(ErrorCodes::BadValue, "--slavedelay cannot be used with --replSet");
            }
            else if (params.count("only")) {
                return Status(ErrorCodes::BadValue, "--only cannot be used with --replSet");
            }
            /* seed list of hosts for the repl set */
            replSettings.replSet = params["replication.replSet"].as<string>().c_str();
        }
        if (params.count("replication.secondaryIndexPrefetch")) {
            return Status(ErrorCodes::BadValue,
                          "replication.secondaryIndexPrefetch is deprecated in TokuMX");
        }
        if (params.count("noIndexBuildRetry") ||
            (params.count("storage.indexBuildRetry") &&
             !params["storage.indexBuildRetry"].as<bool>())) {
            return Status(ErrorCodes::BadValue,
                          "noIndexBuildRetry and storage.indexBuildRetry are deprecated in TokuMX");
        }

        if (params.count("only")) {
            return Status(ErrorCodes::BadValue,
                          "only is deprecated in TokuMX");
        }
        if( params.count("storage.nsSize") ) {
            return Status(ErrorCodes::BadValue,
                          "storage.nsSize is deprecated in TokuMX");
        }
        if (params.count("replication.oplogSizeMB")) {
            return Status(ErrorCodes::BadValue,
                          "replication.oplogSizeMB is deprecated in TokuMX");
        }
        if (!params.count("net.port")) {
            if( params.count("sharding.configsvr") ) {
                serverGlobalParams.port = ServerGlobalParams::ConfigServerPort;
            }
            if( params.count("sharding.shardsvr") ) {
                if( params.count("sharding.configsvr") ) {
                    return Status(ErrorCodes::BadValue,
                                  "can't do --shardsvr and --configsvr at the same time");
                }
                serverGlobalParams.port = ServerGlobalParams::ShardServerPort;
            }
            if (params.count("sharding.clusterRole")) {
                std::string clusterRole = params["sharding.clusterRole"].as<std::string>();
                if (clusterRole == "configsvr") {
                    serverGlobalParams.port = ServerGlobalParams::ConfigServerPort;
                }
                else if (clusterRole == "shardsvr") {
                    serverGlobalParams.port = ServerGlobalParams::ShardServerPort;
                }
                else {
                    StringBuilder sb;
                    sb << "Bad value for sharding.clusterRole: " << clusterRole
                       << ".  Supported modes are: (configsvr|shardsvr)";
                    return Status(ErrorCodes::BadValue, sb.str());
                }
            }
        }
        else {
            if (serverGlobalParams.port <= 0 || serverGlobalParams.port > 65535) {
                return Status(ErrorCodes::BadValue, "bad --port number");
            }
        }
        if (params.count("sharding.configsvr") ||
            (params.count("sharding.clusterRole") &&
             params["sharding.clusterRole"].as<std::string>() == "configsvr")) {
            serverGlobalParams.configsvr = true;
            if (replSettings.usingReplSets()) {
                return Status(ErrorCodes::BadValue,
                              "replication should not be enabled on a config server");
            }
            if (!params.count("storage.dbPath"))
                storageGlobalParams.dbpath = "/data/configdb";
        }
        if (params.count("profile")) {
            serverGlobalParams.defaultProfile = params["profile"].as<int>();
        }
        else {
            if (params.count("operationProfiling.mode")) {
                std::string profilingMode = params["operationProfiling.mode"].as<std::string>();
                if (profilingMode == "off") {
                    serverGlobalParams.defaultProfile = 0;
                }
                else if (profilingMode == "slowOp") {
                    serverGlobalParams.defaultProfile = 1;
                }
                else if (profilingMode == "all") {
                    serverGlobalParams.defaultProfile = 2;
                }
                else {
                    StringBuilder sb;
                    sb << "Bad value for operationProfiling.mode: " << profilingMode
                       << ".  Supported modes are: (off|slowOp|all)";
                    return Status(ErrorCodes::BadValue, sb.str());
                }
            }
        }
        if (params.count("net.ipv6")) {
            enableIPv6();
        }

        if (params.count("sharding.noMoveParanoia") && params.count("sharding.archiveMovedChunks")) {
            return Status(ErrorCodes::BadValue,
                          "The moveParanoia and noMoveParanoia flags cannot both be set");
        }

        if (params.count("sharding.noMoveParanoia")) {
            return Status(ErrorCodes::BadValue,
                          "sharding.noMoveParanoia is deprecated in TokuMX");
        }

        if (params.count("sharding.archiveMovedChunks")) {
            return Status(ErrorCodes::BadValue,
                          "sharding.archiveMovedChunks is deprecated in TokuMX");
        }

        if (params.count("pairwith") || params.count("arbiter") || params.count("opIdMem")) {
            return Status(ErrorCodes::BadValue,
                          "****\n"
                          "Replica Pairs have been deprecated. Invalid options: "
                              "--pairwith, --arbiter, and/or --opIdMem\n"
                          "<http://dochub.mongodb.org/core/replicapairs>\n"
                          "****");
        }

        // needs to be after things like --configsvr parsing, thus here.
        if (params.count("storage.repairPath")) {
            return Status(ErrorCodes::BadValue,
                          "storage.repairPath is deprecated in TokuMX");
        }

        if (params.count("storage.journal.commitIntervalMs")) {
            log() << "--journalCommitInterval deprecated, treating as --logFlushPeriod" << startupWarningsLog;
            storageGlobalParams.logFlushPeriod = params["storage.journal.commitIntervalMs"].as<unsigned>();
            if (storageGlobalParams.logFlushPeriod > 300) {
                return Status(ErrorCodes::BadValue,
                              "--logFlushPeriod out of allowed range (0-300ms)");
            }
        }
        if (params.count("storage.logFlushPeriod")) {
            storageGlobalParams.logFlushPeriod = params["storage.logFlushPeriod"].as<unsigned>();
            if (storageGlobalParams.logFlushPeriod > 300) {
                return Status(ErrorCodes::BadValue,
                              "--logFlushPeriod out of allowed range (0-300ms)");
            }
        }
        if (!(params.count("replication.expireOplogHours") || params.count("replication.expireOplogDays")) && !replSettings.replSet.empty()) {
            log() << "*****************************" << startupWarningsLog;
            log() << "No value set for expireOplogDays, using default of " << replSettings.expireOplogDays << " days." << startupWarningsLog;
            log() << "*****************************" << startupWarningsLog;
        }
        if( params.count("replication.expireOplogHours") ) {
            replSettings.expireOplogHours = params["replication.expireOplogHours"].as<int>();
            // if expireOplogHours is set, we don't want to use the default
            // value of expireOplogDays. We want to use 0. If the user
            // sets the value of expireOplogDays as well, next if-clause
            // below will catch it
            if (!params.count("replication.expireOplogDays")) {
                replSettings.expireOplogDays = 0;
                log() << "*****************************" << startupWarningsLog;
                log() << "No value set for expireOplogDays, only for expireOplogHours. Having at least 1 day set for expireOplogDays is recommended." << startupWarningsLog;
                log() << "*****************************" << startupWarningsLog;
            }
        }
        if( params.count("replication.expireOplogDays") ) {
            replSettings.expireOplogDays = params["replication.expireOplogDays"].as<int>();
        }
        if (params.count("storage.directio")) {
            storageGlobalParams.directio = params["storage.directio"].as<bool>();
        }
        if (params.count("storage.fastupdates")) {
            storageGlobalParams.fastupdates = params["storage.fastupdates"].as<bool>();
        }
        if (params.count("storage.fastupdatesIgnoreErrors")) {
            storageGlobalParams.fastupdatesIgnoreErrors = params["storage.fastupdatesIgnoreErrors"].as<bool>();
        }
        if (params.count("storage.cacheSize")) {
            storageGlobalParams.cacheSize = params["storage.cacheSize"].as<unsigned long long>();
        }
        if (params.count("storage.checkpointPeriod")) {
            storageGlobalParams.checkpointPeriod = params["storage.checkpointPeriod"].as<int>();
        }
        if (params.count("storage.checkpointPeriod")) {
            storageGlobalParams.checkpointPeriod = params["storage.checkpointPeriod"].as<int>();
        }
        if (params.count("storage.cleanerPeriod")) {
            storageGlobalParams.cleanerPeriod = params["storage.cleanerPeriod"].as<int>();
        }
        if (params.count("storage.cleanerIterations")) {
            storageGlobalParams.cleanerIterations = params["storage.cleanerIterations"].as<int>();
        }
        if (params.count("storage.fsRedzone")) {
            storageGlobalParams.fsRedzone = params["storage.fsRedzone"].as<int>();
            if (storageGlobalParams.fsRedzone < 1 || storageGlobalParams.fsRedzone > 99) {
                return Status(ErrorCodes::BadValue,
                              "--fsRedzone must be between 1 and 99.");
            }
        }
        if (params.count("storage.logDir")) {
            storageGlobalParams.logDir = params["storage.logDir"].as<string>();
            if ( storageGlobalParams.logDir[0] != '/' ) {
                // we need to change dbpath if we fork since we change
                // cwd to "/"
                // fork only exists on *nix
                // so '/' is safe
                storageGlobalParams.logDir = serverGlobalParams.cwd + "/" + storageGlobalParams.logDir;
            }
        }
        if (params.count("storage.tmpDir")) {
            storageGlobalParams.tmpDir = params["storage.tmpDir"].as<string>();
            if ( storageGlobalParams.tmpDir[0] != '/' ) {
                // we need to change dbpath if we fork since we change
                // cwd to "/"
                // fork only exists on *nix
                // so '/' is safe
                storageGlobalParams.tmpDir = serverGlobalParams.cwd + "/" + storageGlobalParams.tmpDir;
            }
        }
        if (params.count("replication.txnMemLimit")) {
            unsigned long long limit = params["replication.txnMemLimit"].as<unsigned long long>();
            if (limit > (2ULL << 20)) {
                return Status(ErrorCodes::BadValue,
                              "--txnMemLimit cannot be greater than 2MB");
            }
            storageGlobalParams.txnMemLimit = limit;
        }
        if (params.count("storage.loaderMaxMemory")) {
            unsigned long long x = params["storage.loaderMaxMemory"].as<unsigned long long>();
            if (x < (32ULL << 20)) {
                return Status(ErrorCodes::BadValue,
                              "bad --loaderMaxMemory arg (should never be less than 32mb)");
            }
            storageGlobalParams.loaderMaxMemory = x;
        }
        if (params.count("storage.locktreeMaxMemory")) {
            unsigned long long x = params["storage.locktreeMaxMemory"].as<unsigned long long>();
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
