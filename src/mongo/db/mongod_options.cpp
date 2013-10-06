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

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/instance.h"
#include "mongo/db/module.h"
#include "mongo/db/repl/replication_server_status.h"
#include "mongo/db/server_options.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/version.h"

namespace mongo {

    typedef moe::OptionDescription OD;
    typedef moe::PositionalOptionDescription POD;

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

        ret = general_options.addOption(OD("auth", "auth", moe::Switch, "run with security", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("cacheSize", "cacheSize", moe::UnsignedLongLong,
                    "tokumx cache size (in bytes) for data and indexes", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("checkpointPeriod", "checkpointPeriod", moe::Unsigned,
                    "tokumx time between checkpoints, 0 means never checkpoint", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("cleanerIterations", "cleanerIterations", moe::Unsigned,
                    "tokumx number of iterations per cleaner thread operation, 0 means never run", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("cleanerPeriod", "cleanerPeriod", moe::Unsigned,
                    "tokumx time between cleaner thread operations, 0 means never run", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("cpu", "cpu", moe::Switch,
                    "periodically show cpu and iowait utilization", true));
        if (!ret.isOK()) {
            return ret;
        }
#ifdef _WIN32
        ret = general_options.addOption(OD("dbpath", "dbpath", moe::String,
                    "directory for datafiles - defaults to \\data\\db\\",
                    true, moe::Value(std::string("\\data\\db\\"))));
#else
        ret = general_options.addOption(OD("dbpath", "dbpath", moe::String,
                    "directory for datafiles - defaults to /data/db/",
                    true, moe::Value(std::string("/data/db"))));
#endif
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("diaglog", "diaglog", moe::Int,
                    "0=off 1=W 2=R 3=both 7=W+some reads", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("directio", "directio", moe::Switch,
                    "use direct I/O in tokumx", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("fsRedzone", "fsRedzone", moe::Int,
                    "percentage of free-space left on device before the system goes read-only", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("logDir", "logDir", moe::String,
                    "directory to store transaction log files (default is --dbpath)", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("tmpDir", "tmpDir", moe::String,
                    "directory to store temporary bulk loader files (default is --dbpath)", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("ipv6", "ipv6", moe::Switch,
                    "enable IPv6 support (disabled by default)", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("logFlushPeriod", "logFlushPeriod",
                    moe::Unsigned, "how often to fsync recovery log (ms)", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("jsonp", "jsonp", moe::Switch,
                    "allow JSONP access via http (has security implications)", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("lockTimeout", "lockTimeout", moe::UnsignedLongLong,
                    "tokumx row lock wait timeout (in ms), 0 means wait as long as necessary", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("locktreeMaxMemory", "locktreeMaxMemory", moe::UnsignedLongLong,
                    "tokumx memory limit (in bytes) for storing transactions' row locks", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("loaderMaxMemory", "loaderMaxMemory", moe::UnsignedLongLong,
                    "tokumx memory limit (in bytes) for a single bulk loader to use. the bulk loader is used to build foreground indexes and is also utilized by mongorestore/import", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("loaderCompressTmp", "loaderCompressTmp", moe::Switch,
                    "the bulk loader (used for mongoimport/mongorestore and non-background index builds) will compress intermediate files (see tmpDir) when writing them to disk", true, moe::Value(true)));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("noauth", "noauth", moe::Switch, "run without security",
                    true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("noIndexBuildRetry", "noIndexBuildRetry", moe::Switch,
                    "don't retry any index builds that were interrupted by shutdown", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("noscripting", "noscripting", moe::Switch,
                    "disable scripting engine", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("notablescan", "notablescan", moe::Switch,
                    "do not allow table scans", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("profile", "profile", moe::Int, "0=off 1=slow, 2=all",
                    true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("rest", "rest", moe::Switch, "turn on simple rest api",
                    true));
        if (!ret.isOK()) {
            return ret;
        }
#if defined(__linux__)
        ret = general_options.addOption(OD("shutdown", "shutdown", moe::Switch,
                    "kill a running server (for init scripts)", true));
        if (!ret.isOK()) {
            return ret;
        }
#endif
        ret = general_options.addOption(OD("slowms", "slowms", moe::Int,
                    "value of slow for profile and console log" , true, moe::Value(100)));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("sysinfo", "sysinfo", moe::Switch,
                    "print some diagnostic system information", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = general_options.addOption(OD("upgrade", "upgrade", moe::Switch,
                    "upgrade db if needed", true));
        if (!ret.isOK()) {
            return ret;
        }

        ret = replication_options.addOption(OD("expireOplogDays", "expireOplogDays",
                    moe::Unsigned, "how many days of oplog data to keep", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = replication_options.addOption(OD("expireOplogHours", "expireOplogHours",
                    moe::Unsigned, "how many hours, in addition to expireOplogDays, of oplog data to keep", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = replication_options.addOption(OD("txnMemLimit", "txnMemLimit", moe::UnsignedLongLong,
                    "limit of the size of a transaction's operation", true));
        if (!ret.isOK()) {
            return ret;
        }

        // TokuMX deprecates the master/slave options
        ret = ms_options.addOption(OD("master", "master", moe::Switch, "master mode", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = ms_options.addOption(OD("slave", "slave", moe::Switch, "slave mode", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = ms_options.addOption(OD("source", "source", moe::String,
                    "when slave: specify master as <server:port>", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = ms_options.addOption(OD("only", "only", moe::String,
                    "when slave: specify a single database to replicate", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = ms_options.addOption(OD("autoresync", "autoresync", moe::Switch,
                    "automatically resync if slave data is stale", false));
        if (!ret.isOK()) {
            return ret;
        }

        ret = rs_options.addOption(OD("replSet", "replSet", moe::String,
                    "arg is <setname>[/<optionalseedhostlist>]", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = rs_options.addOption(OD("replIndexPrefetch", "replIndexPrefetch", moe::String,
                    "specify index prefetching behavior (if secondary) [none|_id_only|all]", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = rs_options.addOption(OD("slavedelay", "slavedelay", moe::Int,
                    "specify delay (in seconds) to be used when applying master ops to slave",
                    true));
        if (!ret.isOK()) {
            return ret;
        }

        ret = sharding_options.addOption(OD("configsvr", "configsvr", moe::Switch,
                    "declare this is a config db of a cluster; default port 27019; "
                    "default dir /data/configdb", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = sharding_options.addOption(OD("shardsvr", "shardsvr", moe::Switch,
                    "declare this is a shard db of a cluster; default port 27018", true));
        if (!ret.isOK()) {
            return ret;
        }

        ret = sharding_options.addOption(OD("noMoveParanoia", "noMoveParanoia", moe::Switch,
                    "turn off paranoid saving of data for the moveChunk command; default", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = sharding_options.addOption(OD("moveParanoia", "moveParanoia", moe::Switch,
                    "turn on paranoid saving of data during the moveChunk command "
                    "(used for internal system diagnostics)", false));
        if (!ret.isOK()) {
            return ret;
        }
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

        ret = options->addOption(OD("fastsync", "fastsync", moe::Switch,
                    "indicate that this instance is starting from a "
                    "dbpath snapshot of the repl peer", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("pretouch", "pretouch", moe::Int,
                    "n pretouch threads for applying master/slave operations", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("command", "command", moe::StringVector, "command", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("cacheSize", "cacheSize", moe::Long,
                    "cache size (in MB) for rec store", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("nodur", "nodur", moe::Switch, "disable journaling", false));
        if (!ret.isOK()) {
            return ret;
        }
        // things we don't want people to use
        ret = options->addOption(OD("nohints", "nohints", moe::Switch, "ignore query hints",
                    false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("nopreallocj", "nopreallocj", moe::Switch,
                    "don't preallocate journal files", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("dur", "dur", moe::Switch, "enable journaling", false));
        if (!ret.isOK()) {
            return ret;
        } // old name for --journal
        ret = options->addOption(OD("durOptions", "durOptions", moe::Int,
                    "durability diagnostic options", false));
        if (!ret.isOK()) {
            return ret;
        } // deprecated name
        // deprecated pairing command line options
        ret = options->addOption(OD("pairwith", "pairwith", moe::Switch, "DEPRECATED", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("arbiter", "arbiter", moe::Switch, "DEPRECATED", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("opIdMem", "opIdMem", moe::Switch, "DEPRECATED", false));
        if (!ret.isOK()) {
            return ret;
        }

        // TokuMX unreleased options
        ret = options->addOption(OD("fastupdates", "fastupdates", moe::Switch,
                    "internal only", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("fastupdatesIgnoreErrors", "fastupdatesIgnoreErrors", moe::Switch,
                    "silently ignore all fastupdate errors. NOT RECOMMENDED FOR PRODUCTION, unless failed updates are expected and/or acceptable.", false));
        if (!ret.isOK()) {
            return ret;
        }

        // TokuMX deprecated options
        ret = options->addOption(OD("directoryperdb", "directoryperdb", moe::Switch,
                    "each database will be stored in a separate directory", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("journal", "journal", moe::Switch, "enable journaling",
                    false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("journalCommitInterval", "journalCommitInterval",
                    moe::Unsigned, "how often to group/batch commit (ms)", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("journalOptions", "journalOptions", moe::Int,
                    "journal diagnostic options", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("nojournal", "nojournal", moe::Switch,
                    "disable journaling (journaling is on by default for 64 bit)", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("noprealloc", "noprealloc", moe::Switch,
                    "disable data file preallocation - will often hurt performance", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("nssize", "nssize", moe::Int,
                    ".ns file size (in MB) for new databases", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("quota", "quota", moe::Switch,
                    "limits each database to a certain number of files (8 default)", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("quotaFiles", "quotaFiles", moe::Int,
                    "number of files allowed per db, requires --quota", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("repair", "repair", moe::Switch, "run repair on all dbs",
                    false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("repairpath", "repairpath", moe::String,
                    "root directory for repair files - defaults to dbpath" , false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("smallfiles", "smallfiles", moe::Switch,
                    "use a smaller default file size", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("syncdelay", "syncdelay", moe::Double,
                    "seconds between disk syncs (0=never, but not recommended)", false));
        if (!ret.isOK()) {
            return ret;
        }
        ret = options->addOption(OD("oplogSize", "oplogSize", moe::Int,
                    "size to use (in MB) for replication op log. default is 5% of disk space "
                    "(i.e. large is good)", false));
        if (!ret.isOK()) {
            return ret;
        }

        ret = options->addPositionalOption(POD("command", moe::String, 3));
        if (!ret.isOK()) {
            return ret;
        }

        ret = addModuleOptions(options);
        if (!ret.isOK()) {
            return ret;
        }

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

    Status handlePreValidationMongodOptions(const moe::Environment& params,
                                            const std::vector<std::string>& args) {
        if (params.count("help")) {
            printMongodHelp(serverOptions);
            ::_exit(EXIT_SUCCESS);
        }
        if (params.count("version")) {
            cout << mongodVersion() << endl;
            printGitVersion();
            printOpenSSLVersion();
            ::_exit(EXIT_SUCCESS);
        }
        if (params.count("sysinfo")) {
            sysRuntimeInfo();
            ::_exit(EXIT_SUCCESS);
        }

        return Status::OK();
    }

    Status storeMongodOptions(const moe::Environment& params,
                              const std::vector<std::string>& args) {

        Status ret = storeServerOptions(params, args);
        if (!ret.isOK()) {
            std::cerr << "Error storing command line: " << ret.toString() << std::endl;
            ::_exit(EXIT_BADOPTIONS);
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
            std::cerr << "directoryperdb is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
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
            std::cerr << "Can't specify both --journal and --nojournal options." << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }

        if (params.count("nodur") || params.count("nojournal")) {
            std::cerr << "nodur and nojournal are deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }

        if (params.count("dur") || params.count("journal")) {
            std::cerr << "dur and journal are deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }

        if (params.count("durOptions")) {
            std::cerr << "durOptions is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if (params.count("journalOptions")) {
            std::cerr << "journalOptions is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if (params.count("nohints")) {
            storageGlobalParams.useHints = false;
        }
        if (params.count("nopreallocj")) {
            std::cerr << "nopreallocj is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if (params.count("httpinterface")) {
            if (params.count("nohttpinterface")) {
                std::cerr << "can't have both --httpinterface and --nohttpinterface" << std::endl;
                ::_exit(EXIT_BADOPTIONS);
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
            std::cerr << "noprealloc is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if (params.count("smallfiles")) {
            std::cerr << "smallfiles is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if (params.count("diaglog")) {
            int x = params["diaglog"].as<int>();
            if ( x < 0 || x > 7 ) {
                std::cerr << "can't interpret --diaglog setting" << std::endl;
                ::_exit(EXIT_BADOPTIONS);
            }
            _diaglog.setLevel(x);
        }

        if ((params.count("dur") || params.count("journal")) && params.count("repair")) {
            std::cerr << "Can't specify both --journal and --repair options." << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }

        if (params.count("repair")) {
            std::cerr << "repair is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if (params.count("upgrade")) {
            std::cerr << "upgrade is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if (params.count("notablescan")) {
            storageGlobalParams.noTableScan = true;
        }
        if (params.count("master")) {
            std::cerr << "master is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if (params.count("slave")) {
            std::cerr << "slave is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
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
                std::cerr << "--autoresync is not used with --replSet\nsee "
                    << "http://dochub.mongodb.org/core/resyncingaverystalereplicasetmember"
                    << std::endl;
                ::_exit(EXIT_BADOPTIONS);
            }
        }
        if (params.count("source")) {
            std::cerr << "source is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if( params.count("pretouch") ) {
            std::cerr << "pretouch is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if (params.count("replSet")) {
            if (params.count("slavedelay")) {
                std::cerr << "--slavedelay cannot be used with --replSet" << std::endl;
                ::_exit(EXIT_BADOPTIONS);
            }
            else if (params.count("only")) {
                std::cerr << "--only cannot be used with --replSet" << std::endl;
                ::_exit(EXIT_BADOPTIONS);
            }
            /* seed list of hosts for the repl set */
            replSettings.replSet = params["replSet"].as<string>().c_str();
        }
        if (params.count("replIndexPrefetch")) {
            std::cerr << "replIndexPrefetch is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if (params.count("noIndexBuildRetry")) {
            std::cerr << "noIndexBuildRetry is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if (params.count("only")) {
            std::cerr << "only is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if( params.count("nssize") ) {
            std::cerr << "nssize is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if (params.count("oplogSize")) {
            std::cerr << "oplogSize is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if (params.count("cacheSize")) {
            long x = params["cacheSize"].as<long>();
            if (x <= 0) {
                std::cerr << "bad --cacheSize arg" << std::endl;
                ::_exit(EXIT_BADOPTIONS);
            }
            std::cerr << "--cacheSize option not currently supported" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        if (!params.count("port")) {
            if( params.count("configsvr") ) {
                serverGlobalParams.port = ServerGlobalParams::ConfigServerPort;
            }
            if( params.count("shardsvr") ) {
                if( params.count("configsvr") ) {
                    std::cerr << "can't do --shardsvr and --configsvr at the same time"
                              << std::endl;
                    ::_exit(EXIT_BADOPTIONS);
                }
                serverGlobalParams.port = ServerGlobalParams::ShardServerPort;
            }
        }
        else {
            if (serverGlobalParams.port <= 0 || serverGlobalParams.port > 65535) {
                std::cerr << "bad --port number" << std::endl;
                ::_exit(EXIT_BADOPTIONS);
            }
        }
        if ( params.count("configsvr" ) ) {
            serverGlobalParams.configsvr = true;
            if (replSettings.usingReplSets()) {
                std::cerr << "replication should not be enabled on a config server" << std::endl;
                ::_exit(EXIT_BADOPTIONS);
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
            std::cerr << "The moveParanoia and noMoveParanoia flags cannot both be set; "
                << "please use only one of them." << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }

        if (params.count("noMoveParanoia")) {
            std::cerr << "noMoveParanoia is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }

        if (params.count("moveParanoia")) {
            std::cerr << "moveParanoia is deprecated" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }

        if (params.count("pairwith") || params.count("arbiter") || params.count("opIdMem")) {
            std::cerr << "****\n"
                << "Replica Pairs have been deprecated. Invalid options: --pairwith, "
                << "--arbiter, and/or --opIdMem\n"
                << "<http://dochub.mongodb.org/core/replicapairs>\n"
                << "****" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }

        if (params.count("journalCommitInterval")) {
            log() << "--journalCommitInterval deprecated, treating as --logFlushPeriod" << startupWarningsLog;
            storageGlobalParams.logFlushPeriod = params["journalCommitInterval"].as<unsigned>();
            if (storageGlobalParams.logFlushPeriod > 300) {
                std::cerr << "--logFlushPeriod out of allowed range (0-300ms)" << std::endl;
                ::_exit(EXIT_BADOPTIONS);
            }
        }
        if (params.count("logFlushPeriod")) {
            storageGlobalParams.logFlushPeriod = params["logFlushPeriod"].as<unsigned>();
            if (storageGlobalParams.logFlushPeriod > 300) {
                std::cerr << "--logFlushPeriod out of allowed range (0-300ms)" << std::endl;
                ::_exit(EXIT_BADOPTIONS);
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
                std::cerr << "--fsRedzone must be between 1 and 99." << std::endl;
                ::_exit(EXIT_BADOPTIONS);
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
                std::cerr << "--txnMemLimit cannot be greater than 2MB" << std::endl;
                ::_exit(EXIT_BADOPTIONS);
            }
            storageGlobalParams.txnMemLimit = limit;
        }
        if (params.count("loaderMaxMemory")) {
            unsigned long long x = params["loaderMaxMemory"].as<unsigned long long>();
            if (x < (32ULL << 20)) {
                std::cerr << "bad --loaderMaxMemory arg (should never be less than 32mb)" << std::endl;
                ::_exit(EXIT_BADOPTIONS);
            }
            storageGlobalParams.loaderMaxMemory = x;
        }
        if (params.count("locktreeMaxMemory")) {
            unsigned long long x = params["locktreeMaxMemory"].as<unsigned long long>();
            if (x < (64ULL << 10)) {
                std::cerr << "bad --locktreeMaxMemory arg (should never be less than 64kb)" << std::endl;
                ::_exit(EXIT_BADOPTIONS);
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

    MONGO_INITIALIZER_GENERAL(ParseStartupConfiguration,
            ("GlobalLogManager"),
            ("default", "completedStartupConfig"))(InitializerContext* context) {
        serverOptions = moe::OptionSection("Allowed options");
        Status ret = addMongodOptions(&serverOptions);
        if (!ret.isOK()) {
            return ret;
        }

        moe::OptionsParser parser;
        ret = parser.run(serverOptions, context->args(), context->env(), &serverParsedOptions);
        if (!ret.isOK()) {
            std::cerr << ret.reason() << std::endl;
            std::cerr << "try '" << context->args()[0]
                      << " --help' for more information" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }

        ret = handlePreValidationMongodOptions(serverParsedOptions, context->args());
        if (!ret.isOK()) {
            return ret;
        }

        ret = serverParsedOptions.validate();
        if (!ret.isOK()) {
            return ret;
        }

        ret = storeMongodOptions(serverParsedOptions, context->args());
        if (!ret.isOK()) {
            return ret;
        }

        return Status::OK();
    }

    Status addModuleOptions(moe::OptionSection* options) {
        Module::addAllOptions(options);
        return Status::OK();
    }

} // namespace mongo
