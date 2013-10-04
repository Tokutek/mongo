// @file db.cpp : Defines main() for the mongod program.

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

#include "mongo/pch.h"

#include <boost/thread/thread.hpp>
#include <boost/filesystem/operations.hpp>
#include <fstream>

#include "mongo/base/initializer.h"
#include "mongo/db/client.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/cmdline.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/d_globals.h"
#include "mongo/db/dbmessage.h"
#include "mongo/db/dbwebserver.h"
#include "mongo/db/initialize_server_global_state.h"
#include "mongo/db/instance.h"
#include "mongo/db/introspect.h"
#include "mongo/db/json.h"
#include "mongo/db/module.h"
#include "mongo/db/repl.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/restapi.h"
#include "mongo/db/stats/counters.h"
#include "mongo/db/stats/snapshots.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/ttl.h"
#include "mongo/plugins/loader.h"
#include "mongo/s/d_writeback.h"
#include "mongo/scripting/engine.h"
#include "mongo/util/background.h"
#include "mongo/util/concurrency/task.h"
#include "mongo/util/net/message_server.h"
#include "mongo/util/ntservice.h"
#include "mongo/util/ramlog.h"
#include "mongo/util/stacktrace.h"
#include "mongo/util/startup_test.h"
#include "mongo/util/text.h"
#include "mongo/util/version.h"

#if defined(_WIN32)
# include <DbgHelp.h>
#else
# include <sys/file.h>
#endif

namespace mongo {

    /* only off if --nohints */
    extern bool useHints;

    extern int diagLogging;
    extern int lockFile;

    void setupQuittingSignals();
    void setupSignals( bool ignored );
    void startReplication();
    void exitCleanly( ExitCode code );

#ifdef _WIN32
    ntservice::NtServiceDefaultStrings defaultServiceStrings = {
        L"MongoDB",
        L"Mongo DB",
        L"Mongo DB Server"
    };
#endif

    CmdLine cmdLine;
    static bool scriptingEnabled = true;
    static bool noHttpInterface = false;
    bool shouldRepairDatabases = 0;
    Timer startupSrandTimer;

    const char *ourgetns() {
        Client *c = currentClient.get();
        if ( ! c )
            return "";
        Client::Context* cc = c->getContext();
        return cc ? cc->ns() : "";
    }

    struct MyStartupTests {
        MyStartupTests() {
            verify( sizeof(OID) == 12 );
        }
    } mystartupdbcpp;

    QueryResult* emptyMoreResult(long long);


    /* todo: make this a real test.  the stuff in dbtests/ seem to do all dbdirectclient which exhaust doesn't support yet. */
// QueryOption_Exhaust
#define TESTEXHAUST 0
#if( TESTEXHAUST )
    void testExhaust() {
        sleepsecs(1);
        unsigned n = 0;
        auto f = [&n](const BSONObj& o) {
            verify( o.valid() );
            //cout << o << endl;
            n++;
            bool testClosingSocketOnError = false;
            if( testClosingSocketOnError )
                verify(false);
        };
        DBClientConnection db(false);
        db.connect("localhost");
        const char *ns = "local.foo";
        if( db.count(ns) < 10000 )
            for( int i = 0; i < 20000; i++ )
                db.insert(ns, BSON("aaa" << 3 << "b" << "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));

        try {
            db.query(f, ns, Query() );
        }
        catch(...) {
            cout << "hmmm" << endl;
        }

        try {
            db.query(f, ns, Query() );
        }
        catch(...) {
            cout << "caught" << endl;
        }

        cout << n << endl;
    };
#endif

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

    class MyMessageHandler : public MessageHandler {
    public:
        virtual void connected( AbstractMessagingPort* p ) {
            Client::initThread("conn", p);
        }

        virtual void process( Message& m , AbstractMessagingPort* port , LastError * le) {
            while ( true ) {
                if ( inShutdown() ) {
                    log() << "got request after shutdown()" << endl;
                    break;
                }

                lastError.startRequest( m , le );

                DbResponse dbresponse;
                try {
                    assembleResponse( m, dbresponse, port->remote() );
                }
                catch ( const ClockSkewException & ) {
                    log() << "ClockSkewException - shutting down" << endl;
                    exitCleanly( EXIT_CLOCK_SKEW );
                }

                if ( dbresponse.response ) {
                    port->reply(m, *dbresponse.response, dbresponse.responseTo);
                    if( dbresponse.exhaustNS.size() > 0 ) {
                        MsgData *header = dbresponse.response->header();
                        QueryResult *qr = (QueryResult *) header;
                        long long cursorid = qr->cursorId;
                        if( cursorid ) {
                            verify( dbresponse.exhaustNS.size() && dbresponse.exhaustNS[0] );
                            string ns = dbresponse.exhaustNS; // before reset() free's it...
                            m.reset();
                            BufBuilder b(512);
                            b.appendNum((int) 0 /*size set later in appendData()*/);
                            b.appendNum(header->id);
                            b.appendNum(header->responseTo);
                            b.appendNum((int) dbGetMore);
                            b.appendNum((int) 0);
                            b.appendStr(ns);
                            b.appendNum((int) 0); // ntoreturn
                            b.appendNum(cursorid);
                            m.appendData(b.buf(), b.len());
                            b.decouple();
                            DEV log() << "exhaust=true sending more" << endl;
                            continue; // this goes back to top loop
                        }
                    }
                }
                break;
            }
        }

        virtual void disconnected( AbstractMessagingPort* p ) {
            Client * c = currentClient.get();
            if( c ) c->shutdown();
        }

    };

    void logStartup() {
        BSONObjBuilder toLog;
        stringstream id;
        id << getHostNameCached() << "-" << jsTime();
        toLog.append( "_id", id.str() );
        toLog.append( "hostname", getHostNameCached() );

        toLog.appendTimeT( "startTime", time(0) );
        char buf[64];
        curTimeString( buf );
        toLog.append( "startTimeLocal", buf );

        toLog.append( "cmdLine", CmdLine::getParsedOpts() );
        toLog.append( "pid", getpid() );


        BSONObjBuilder buildinfo( toLog.subobjStart("buildinfo"));
        appendBuildInfo(buildinfo);
        buildinfo.doneFast();

        BSONObj o = toLog.obj();

        Lock::GlobalWrite lk;
        Client::GodScope gs;
        DBDirectClient c;
        const char* name = "local.startup_log";
        c.createCollection( name, 10 * 1024 * 1024, true );
        c.insert( name, o);
    }

    void listen(int port) {
        //testTheDb();
        MessageServer::Options options;
        options.port = port;
        options.ipList = cmdLine.bind_ip;

        MessageServer * server = createServer( options , new MyMessageHandler() );
        server->setAsTimeTracker();

        logStartup();
        startReplication();
        if ( !noHttpInterface )
            boost::thread web( boost::bind(&webServerThread, new RestAdminAccess() /* takes ownership */));

#if(TESTEXHAUST)
        boost::thread thr(testExhaust);
#endif
        server->run();
    }

    /**
     * Checks if this server was started without --replset but has a config in local.system.replset
     * (meaning that this is probably a replica set member started in stand-alone mode).
     *
     * @returns the number of documents in local.system.replset or 0 if this was started with
     *          --replset.
     */
    unsigned long long checkIfReplMissingFromCommandLine() {
        Lock::GlobalWrite lk;
        if( !cmdLine.usingReplSets() ) {
            Client::GodScope gs;
            DBDirectClient c;
            return c.count("local.system.replset");
        }
        return 0;
    }

    const char * jsInterruptCallback() {
        // should be safe to interrupt in js code, even if we have a write lock
        return killCurrentOp.checkForInterruptNoAssert();
    }

    unsigned jsGetCurrentOpIdCallback() {
        return cc().curop()->opNum();
    }

    void _initAndListen(int listenPort ) {

        Client::initThread("initandlisten");

        Logstream::get().addGlobalTee( new RamLog("global") );

        bool is32bit = sizeof(int*) == 4;

        {
#if !defined(_WIN32)
            pid_t pid = getpid();
#else
            DWORD pid=GetCurrentProcessId();
#endif
            Nullstream& l = log();
            l << "TokuMX starting : pid=" << pid << " port=" << cmdLine.port << " dbpath=" << dbpath;
            l << ( is32bit ? " 32" : " 64" ) << "-bit host=" << getHostNameCached() << endl;
        }
        DEV log() << "_DEBUG build (which is slower)" << endl;
        show_warnings();
        log() << mongodVersion() << endl;
        printGitVersion();
        printSysInfo();
        printCommandLineOpts();

        {
            stringstream ss;
            ss << endl;
            ss << "*********************************************************************" << endl;
            ss << " ERROR: dbpath (" << dbpath << ") does not exist." << endl;
            ss << " Create this directory or give existing directory in --dbpath." << endl;
            ss << " See http://dochub.mongodb.org/core/startingandstoppingmongo" << endl;
            ss << "*********************************************************************" << endl;
            uassert( 10296 ,  ss.str().c_str(), boost::filesystem::exists( dbpath ) );
        }

        acquirePathLock();

        // the last thing we do before initializing storage is to install the
        // txn complete hooks, which live in namespace_details.cpp
        extern TxnCompleteHooks _txnCompleteHooks;
        setTxnCompleteHooks(&_txnCompleteHooks);
        storage::startup();

        unsigned long long missingRepl = checkIfReplMissingFromCommandLine();
        if (missingRepl) {
            log() << startupWarningsLog;
            log() << "** warning: mongod started without --replSet yet " << missingRepl
                  << " documents are present in local.system.replset" << startupWarningsLog;
            log() << "**          restart with --replSet unless you are doing maintenance and no"
                  << " other clients are connected" << startupWarningsLog;
            log() << startupWarningsLog;
        }

        Module::initAll();

        if ( scriptingEnabled ) {
            ScriptEngine::setup();
            globalScriptEngine->setCheckInterruptCallback( jsInterruptCallback );
            globalScriptEngine->setGetCurrentOpIdCallback( jsGetCurrentOpIdCallback );
        }

        /* this is for security on certain platforms (nonce generation) */
        srand((unsigned) (curTimeMicros() ^ startupSrandTimer.micros()));

        if (!cmdLine.pluginsDir.empty()) {
            plugins::loader->setPluginsDir(cmdLine.pluginsDir);
        }
        plugins::loader->autoload(cmdLine.plugins);

        snapshotThread.go();
        d.clientCursorMonitor.go();
        PeriodicTask::theRunner->go();
        if (missingRepl) {
            log() << "** warning: not starting TTL monitor" << startupWarningsLog;
            log() << "**          if this member is not part of a replica set and you want to use "
                  << startupWarningsLog;
            log() << "**          TTL collections, remove local.system.replset and restart"
                  << startupWarningsLog;
        }
        else {
            startTTLBackgroundJob();
        }

#ifndef _WIN32
        CmdLine::launchOk();
#endif

        if( !noauth ) {
            // open admin db in case we need to use it later. TODO this is not the right way to
            // resolve this.
            Client::WriteContext c("admin", dbpath);
        }

        listen(listenPort);

        // listen() will return when exit code closes its socket.
        exitCleanly(EXIT_NET_ERROR);
    }

    void initAndListen(int listenPort) {
        try {
            _initAndListen(listenPort);
        }
        catch ( DBException &e ) {
            log() << "exception in initAndListen: " << e.toString() << ", terminating" << endl;
            dbexit( EXIT_UNCAUGHT );
        }
        catch ( std::exception &e ) {
            log() << "exception in initAndListen std::exception: " << e.what() << ", terminating" << endl;
            dbexit( EXIT_UNCAUGHT );
        }
        catch ( int& n ) {
            log() << "exception in initAndListen int: " << n << ", terminating" << endl;
            dbexit( EXIT_UNCAUGHT );
        }
        catch(...) {
            log() << "exception in initAndListen, terminating" << endl;
            dbexit( EXIT_UNCAUGHT );
        }
    }

#if defined(_WIN32)
    void initService() {
        ntservice::reportStatus( SERVICE_RUNNING );
        log() << "Service running" << endl;
        initAndListen( cmdLine.port );
    }
#endif

} // namespace mongo

using namespace mongo;

#include <boost/program_options.hpp>

namespace po = boost::program_options;

void show_help_text(po::options_description options) {
    show_warnings();
    cout << options << endl;
};

static int mongoDbMain(int argc, char* argv[], char** envp);

int main(int argc, char* argv[], char** envp) {
    int exitCode = mongoDbMain(argc, argv, envp);
    ::_exit(exitCode);
}

static void buildOptionsDescriptions(po::options_description *pVisible,
                                     po::options_description *pHidden,
                                     po::positional_options_description *pPositional) {

    po::options_description& visible_options = *pVisible;
    po::options_description& hidden_options = *pHidden;
    po::positional_options_description& positional_options = *pPositional;

    po::options_description general_options("General options");
#if defined(_WIN32)
    po::options_description windows_scm_options("Windows Service Control Manager options");
#endif
    po::options_description replication_options("Replication options");
    po::options_description ms_options("Master/slave options");
    po::options_description rs_options("Replica set options");
    po::options_description sharding_options("Sharding options");
    po::options_description ssl_options("SSL options");

    CmdLine::addGlobalOptions( general_options , hidden_options , ssl_options );

    StringBuilder dbpathBuilder;
    dbpathBuilder << "directory for datafiles - defaults to " << dbpath;

    general_options.add_options()
    ("auth", "run with security")
    ("cacheSize", po::value(&cmdLine.cacheSize), "tokumx cache size (in bytes) for data and indexes")
    ("checkpointPeriod", po::value<uint32_t>(), "tokumx time between checkpoints, 0 means never checkpoint")
    ("cleanerIterations", po::value<uint32_t>(), "tokumx number of iterations per cleaner thread operation, 0 means never run")
    ("cleanerPeriod", po::value<uint32_t>(), "tokumx time between cleaner thread operations, 0 means never run")
    ("cpu", "periodically show cpu and iowait utilization")
    ("dbpath", po::value<string>() , dbpathBuilder.str().c_str())
    ("diaglog", po::value<int>(), "0=off 1=W 2=R 3=both 7=W+some reads")
    ("directio", "use direct I/O in tokumx")
    ("fsRedzone", po::value<int>(), "percentage of free-space left on device before the system goes read-only.")
    ("logDir", po::value<string>(), "directory to store transaction log files (default is --dbpath)")
    ("tmpDir", po::value<string>(), "directory to store temporary bulk loader files (default is --dbpath)")
    ("gdb", "go into a debug-friendly mode, disabling TTL and SIGINT/TERM handlers (development use only).")
    ("gdbPath", po::value<string>(), "if specified, debugging information will be gathered on fatal error by launching gdb at the given path")
    ("ipv6", "enable IPv6 support (disabled by default)")
    ("journal", "DEPRECATED")
    ("journalCommitInterval", po::value<uint32_t>(), "how often to fsync recovery log (same as logFlushPeriod)")
    ("logFlushPeriod", po::value<uint32_t>(), "how often to fsync recovery log")
    ("expireOplogDays", po::value<uint32_t>(), "how many days of oplog data to keep")
    ("expireOplogHours", po::value<uint32_t>(), "how many hours, in addition to expireOplogDays, of oplog data to keep")
    ("journalOptions", po::value<int>(), "DEPRECATED")
    ("jsonp","allow JSONP access via http (has security implications)")
    ("lockTimeout", po::value(&cmdLine.lockTimeout), "tokumx row lock wait timeout (in ms), 0 means wait as long as necessary")
    ("locktreeMaxMemory", po::value(&cmdLine.locktreeMaxMemory), "tokumx memory limit (in bytes) for storing transactions' row locks.")
    ("noauth", "run without security")
    ("nohttpinterface", "disable http interface")
    ("nojournal", "DEPRECATED)")
    ("noprealloc", "disable data file preallocation - will often hurt performance")
    ("noscripting", "disable scripting engine")
    ("notablescan", "do not allow table scans")
    ("nssize", po::value<int>()->default_value(16), ".ns file size (in MB) for new databases")
    ("profile",po::value<int>(), "0=off 1=slow, 2=all")
    ("quota", "limits each database to a certain number of files (8 default)")
    ("quotaFiles", po::value<int>(), "number of files allowed per db, requires --quota")
    ("repair", "run repair on all dbs")
    ("rest","turn on simple rest api")
#if defined(__linux__)
    ("shutdown", "kill a running server (for init scripts)")
#endif
    ("slowms",po::value<int>(&cmdLine.slowMS)->default_value(100), "value of slow for profile and console log" )
    ("smallfiles", "DEPRECATED")
    ("syncdelay",po::value<double>(&cmdLine.syncdelay)->default_value(60), "seconds between disk syncs (0=never, but not recommended)")
    ("sysinfo", "print some diagnostic system information")
    ("txnMemLimit", po::value(&cmdLine.txnMemLimit)->default_value(cmdLine.txnMemLimit), "limit of the size of a transaction's  operation")
    ("upgrade", "upgrade db if needed")
    ;

#if defined(_WIN32)
    CmdLine::addWindowsOptions( windows_scm_options, hidden_options );
#endif

    replication_options.add_options()
    ("oplogSize", po::value<int>(), "DEPRECATED")
    ;

    ms_options.add_options()
    ("master", "master mode")
    ("slave", "slave mode")
    ("source", po::value<string>(), "when slave: specify master as <server:port>")
    ("only", po::value<string>(), "when slave: specify a single database to replicate")
    ("slavedelay", po::value<int>(), "specify delay (in seconds) to be used when applying master ops to slave")
    ("autoresync", "automatically resync if slave data is stale")
    ;

    rs_options.add_options()
    ("replSet", po::value<string>(), "arg is <setname>[/<optionalseedhostlist>]")
    ("replIndexPrefetch", po::value<string>(), "specify index prefetching behavior (if secondary) [none|_id_only|all]")
    ;

    sharding_options.add_options()
    ("configsvr", "declare this is a config db of a cluster; default port 27019; default dir /data/configdb")
    ("shardsvr", "declare this is a shard db of a cluster; default port 27018")
    ("noMoveParanoia" , "turn off paranoid saving of data for moveChunk.  this is on by default for now, but default will switch" )
    ;

    hidden_options.add_options()
    ("fastsync", "indicate that this instance is starting from a dbpath snapshot of the repl peer")
    ("pretouch", po::value<int>(), "DEPRECATED")
    ("command", po::value< vector<string> >(), "command")
    ("nodur", "DEPRECATED")
    // things we don't want people to use
    ("nohints", "ignore query hints")
    ("nopreallocj", "DEPRECATED")
    ("dur", "DEPRECATED") // old name for --journal
    ("durOptions", po::value<int>(), "DEPRECATED") // deprecated name
    // deprecated pairing command line options
    ("pairwith", "DEPRECATED")
    ("arbiter", "DEPRECATED")
    ("opIdMem", "DEPRECATED")
    ;

    positional_options.add("command", 3);
    visible_options.add(general_options);
#if defined(_WIN32)
    visible_options.add(windows_scm_options);
#endif
    visible_options.add(replication_options);
    visible_options.add(ms_options);
    visible_options.add(rs_options);
    visible_options.add(sharding_options);
#ifdef MONGO_SSL
    visible_options.add(ssl_options);
#endif
    Module::addOptions( visible_options );
}

static void processCommandLineOptions(const std::vector<std::string>& argv) {
    po::options_description visible_options("Allowed options");
    po::options_description hidden_options("Hidden options");
    po::positional_options_description positional_options;
    buildOptionsDescriptions(&visible_options, &hidden_options, &positional_options);

    {
        po::variables_map params;

        if (!CmdLine::store(argv,
                            visible_options,
                            hidden_options,
                            positional_options,
                            params)) {
            ::_exit(EXIT_FAILURE);
        }

        if (params.count("help")) {
            show_help_text(visible_options);
            ::_exit(EXIT_SUCCESS);
        }
        if (params.count("version")) {
            cout << mongodVersion() << endl;
            printGitVersion();
            ::_exit(EXIT_SUCCESS);
        }
        if (params.count("sysinfo")) {
            sysRuntimeInfo();
            ::_exit(EXIT_SUCCESS);
        }

        if ( params.count( "dbpath" ) ) {
            dbpath = params["dbpath"].as<string>();
            if ( params.count( "fork" ) && dbpath[0] != '/' ) {
                // we need to change dbpath if we fork since we change
                // cwd to "/"
                // fork only exists on *nix
                // so '/' is safe
                dbpath = cmdLine.cwd + "/" + dbpath;
            }
        }
#ifdef _WIN32
        if (dbpath.size() > 1 && dbpath[dbpath.size()-1] == '/') {
            // size() check is for the unlikely possibility of --dbpath "/"
            dbpath = dbpath.erase(dbpath.size()-1);
        }
#endif

        if (params.count("cpu")) {
            cmdLine.cpu = true;
        }
        if (params.count("noauth")) {
            noauth = true;
        }
        if (params.count("auth")) {
            noauth = false;
        }
        if (params.count("quota")) {
            cmdLine.quota = true;
        }
        if (params.count("quotaFiles")) {
            cmdLine.quota = true;
            cmdLine.quotaFiles = params["quotaFiles"].as<int>() - 1;
        }
        if (params.count("nodur")) {
            out() << "nodur deprecated" <<endl;
        }
        if (params.count("nojournal")) {
            out() << "nojournal deprecated" <<endl;
        }
        if (params.count("dur")) {
            out() << "dur deprecated" <<endl;
        }
        if (params.count("journal")) {
            out() << "journal deprecated" <<endl;
        }
        if (params.count("durOptions")) {
            out() << "durOptions deprecated" <<endl;
        }
        if( params.count("journalCommitInterval") ) {
            cmdLine.logFlushPeriod = params["journalCommitInterval"].as<uint32_t>();
            out() << "--journalCommitInterval deprecated, treating as --logFlushPeriod" << endl;
            if( cmdLine.logFlushPeriod > 300 ) {
                out() << "--logFlushPeriod out of allowed range (0-300ms)" << endl;
                dbexit( EXIT_BADOPTIONS );
            }
        }
        if( params.count("logFlushPeriod") ) {
            cmdLine.logFlushPeriod = params["logFlushPeriod"].as<uint32_t>();
            if( cmdLine.logFlushPeriod > 300 ) {
                out() << "--logFlushPeriod out of allowed range (0-300ms)" << endl;
                dbexit( EXIT_BADOPTIONS );
            }
        }
        if( params.count("expireOplogDays") ) {
            cmdLine.expireOplogDays = params["expireOplogDays"].as<uint32_t>();
        }
        if( params.count("expireOplogHours") ) {
            cmdLine.expireOplogHours = params["expireOplogHours"].as<uint32_t>();
        }
        if (params.count("journalOptions")) {
            out() << "journalOptions deprecated" <<endl;
        }
        if (params.count("directio")) {
            cmdLine.directio = true;
        }
        if (params.count("checkpointPeriod")) {
            cmdLine.checkpointPeriod = params["checkpointPeriod"].as<uint32_t>();
        }
        if (params.count("cleanerPeriod")) {
            cmdLine.cleanerPeriod = params["cleanerPeriod"].as<uint32_t>();
        }
        if (params.count("cleanerIterations")) {
            cmdLine.cleanerIterations = params["cleanerIterations"].as<uint32_t>();
        }
        if (params.count("fsRedzone")) {
            cmdLine.fsRedzone = params["fsRedzone"].as<int>();
            if (cmdLine.fsRedzone < 1 || cmdLine.fsRedzone > 99) {
                out() << "--fsRedzone must be between 1 and 99." << endl;
                dbexit( EXIT_BADOPTIONS );
            }
        }
        if (params.count("logDir")) {
            cmdLine.logDir = params["logDir"].as<string>();
            if ( cmdLine.logDir[0] != '/' ) {
                // we need to change dbpath if we fork since we change
                // cwd to "/"
                // fork only exists on *nix
                // so '/' is safe
                cmdLine.logDir = cmdLine.cwd + "/" + cmdLine.logDir;
            }
        }
        if (params.count("tmpDir")) {
            cmdLine.tmpDir = params["tmpDir"].as<string>();
            if ( cmdLine.tmpDir[0] != '/' ) {
                // we need to change dbpath if we fork since we change
                // cwd to "/"
                // fork only exists on *nix
                // so '/' is safe
                cmdLine.tmpDir = cmdLine.cwd + "/" + cmdLine.tmpDir;
            }
        }
        if (params.count("gdbPath")) {
            cmdLine.gdbPath = params["gdbPath"].as<string>();
        }
        if (params.count("txnMemLimit")) {
            uint64_t limit = (uint64_t) params["txnMemLimit"].as<BytesQuantity<uint64_t> >();
            if( limit > 1ULL<<21 ) {
                out() << "--txnMemLimit cannot be greater than 2MB" << endl;
                dbexit( EXIT_BADOPTIONS );
            }
        }
        if (params.count("nohints")) {
            useHints = false;
        }
        if (params.count("nopreallocj")) {
            out() << "nopreallocj deprecated" << endl;
        }
        if (params.count("nohttpinterface")) {
            noHttpInterface = true;
        }
        if (params.count("rest")) {
            cmdLine.rest = true;
        }
        if (params.count("jsonp")) {
            cmdLine.jsonp = true;
        }
        if (params.count("noscripting")) {
            scriptingEnabled = false;
        }
        if (params.count("noprealloc")) {
            out() << "noprealloc is a deprecated parameter" << endl;
        }
        if (params.count("smallfiles")) {
            out() << " smallfiles is a deprecated parameter." << endl;
        }
        if (params.count("diaglog")) {
            int x = params["diaglog"].as<int>();
            if ( x < 0 || x > 7 ) {
                out() << "can't interpret --diaglog setting" << endl;
                dbexit( EXIT_BADOPTIONS );
            }
            _diaglog.setLevel(x);
        }
        if (params.count("repair")) {
            out() << " repair is a deprecated parameter." << endl;
        }
        if (params.count("upgrade")) {
            out() << " upgrade is a deprecated parameter." << endl;
        }
        if (params.count("notablescan")) {
            cmdLine.noTableScan = true;
        }
        if (params.count("master")) {
            out() << " master is a deprecated parameter" << endl;
        }
        if (params.count("slave")) {
            out() << " slave is a deprecated parameter" << endl;
        }
        if (params.count("slavedelay")) {
            replSettings.slavedelay = params["slavedelay"].as<int>();
        }
        if (params.count("fastsync")) {
            replSettings.fastsync = true;
        }
        if (params.count("autoresync")) {
            replSettings.autoresync = true;
            if( params.count("replSet") ) {
                out() << "--autoresync is not used with --replSet" << endl;
                out() << "see http://dochub.mongodb.org/core/resyncingaverystalereplicasetmember" << endl;
                dbexit( EXIT_BADOPTIONS );
            }
        }
        if (params.count("source")) {
            /* specifies what the source in local.sources should be */
            cmdLine.source = params["source"].as<string>().c_str();
        }
        if( params.count("pretouch") ) {
            out() << " pretouch is a deprecated parameter" << endl;
        }
        if (params.count("replSet")) {
            if (params.count("slavedelay")) {
                out() << "--slavedelay cannot be used with --replSet" << endl;
                dbexit( EXIT_BADOPTIONS );
            }
            else if (params.count("only")) {
                out() << "--only cannot be used with --replSet" << endl;
                dbexit( EXIT_BADOPTIONS );
            }
            /* seed list of hosts for the repl set */
            cmdLine._replSet = params["replSet"].as<string>().c_str();
        }
        if (params.count("replIndexPrefetch")) {
            out() << " replIndexPrefetch is a deprecated parameter" << endl;
        }
        if (params.count("only")) {
            cmdLine.only = params["only"].as<string>().c_str();
        }
        if( params.count("nssize") ) {
            out() << " nssize is a deprecated parameter" << endl;
        }
        if (params.count("oplogSize")) {
            out() << " oplogSize is a deprecated parameter" << endl;
        }
        if (params.count("locktreeMaxMemory")) {
            uint64_t x = (uint64_t) params["locktreeMaxMemory"].as<BytesQuantity<uint64_t> >();
            if (x < 65536) {
                out() << "bad --locktreeMaxMemory arg (should never be less than 64kb)" << endl;
                dbexit( EXIT_BADOPTIONS );
            }
        }
        if (params.count("port") == 0 ) {
            if( params.count("configsvr") ) {
                cmdLine.port = CmdLine::ConfigServerPort;
            }
            if( params.count("shardsvr") ) {
                if( params.count("configsvr") ) {
                    log() << "can't do --shardsvr and --configsvr at the same time" << endl;
                    dbexit( EXIT_BADOPTIONS );
                }
                cmdLine.port = CmdLine::ShardServerPort;
            }
        }
        else {
            if ( cmdLine.port <= 0 || cmdLine.port > 65535 ) {
                out() << "bad --port number" << endl;
                dbexit( EXIT_BADOPTIONS );
            }
        }
        if ( params.count("configsvr" ) ) {
            cmdLine.configsvr = true;
            if (cmdLine.usingReplSets()) {
                log() << "replication should not be enabled on a config server" << endl;
                ::_exit(-1);
            }
            if ( params.count( "dbpath" ) == 0 )
                dbpath = "/data/configdb";
        }
        if ( params.count( "profile" ) ) {
            cmdLine.defaultProfile = params["profile"].as<int>();
        }
        if (params.count("ipv6")) {
            enableIPv6();
        }
        if (params.count("noMoveParanoia")) {
            cmdLine.moveParanoia = false;
        }
        if (params.count("pairwith") || params.count("arbiter") || params.count("opIdMem")) {
            out() << "****" << endl;
            out() << "Replica Pairs have been deprecated. Invalid options: --pairwith, --arbiter, and/or --opIdMem" << endl;
            out() << "<http://dochub.mongodb.org/core/replicapairs>" << endl;
            out() << "****" << endl;
            dbexit( EXIT_BADOPTIONS );
        }

        Module::configAll( params );

        if (params.count("command")) {
            vector<string> command = params["command"].as< vector<string> >();

            if (command[0].compare("dbpath") == 0) {
                cout << dbpath << endl;
                ::_exit(EXIT_SUCCESS);
            }

            if (command[0].compare("run") != 0) {
                cout << "Invalid command: " << command[0] << endl;
                cout << visible_options << endl;
                ::_exit(EXIT_FAILURE);
            }

            if (command.size() > 1) {
                cout << "Too many parameters to 'run' command" << endl;
                cout << visible_options << endl;
                ::_exit(EXIT_FAILURE);
            }
        }


        Module::configAll(params);

#ifdef _WIN32
        ntservice::configureService(initService,
                                    params,
                                    defaultServiceStrings,
                                    std::vector<std::string>(),
                                    argv);
#endif  // _WIN32

#ifdef __linux__
        if (params.count("shutdown")){
            bool failed = false;

            string name = ( boost::filesystem::path( dbpath ) / "mongod.lock" ).string();
            if ( !boost::filesystem::exists( name ) || boost::filesystem::file_size( name ) == 0 )
                failed = true;

            pid_t pid;
            string procPath;
            if (!failed){
                try {
                    ifstream f (name.c_str());
                    f >> pid;
                    procPath = (str::stream() << "/proc/" << pid);
                    if (!boost::filesystem::exists(procPath))
                        failed = true;
                }
                catch (const std::exception& e){
                    cerr << "Error reading pid from lock file [" << name << "]: " << e.what() << endl;
                    failed = true;
                }
            }

            if (failed) {
                cerr << "There doesn't seem to be a server running with dbpath: " << dbpath << endl;
                ::_exit(EXIT_FAILURE);
            }

            cout << "killing process with pid: " << pid << endl;
            int ret = kill(pid, SIGTERM);
            if (ret) {
                int e = errno;
                cerr << "failed to kill process: " << errnoWithDescription(e) << endl;
                ::_exit(EXIT_FAILURE);
            }

            while (boost::filesystem::exists(procPath)) {
                sleepsecs(1);
            }

            ::_exit(EXIT_SUCCESS);
        }
#endif
    }
}

static int mongoDbMain(int argc, char* argv[], char **envp) {
    static StaticObserver staticObserver;

    getcurns = ourgetns;

    setupCoreSignals();
    setupQuittingSignals();

    dbExecCommand = argv[0];

    srand(curTimeMicros());

    {
        unsigned x = 0x12345678;
        unsigned char& b = (unsigned char&) x;
        if ( b != 0x78 ) {
            out() << "big endian cpus not yet supported" << endl;
            return 33;
        }
    }

    if( argc == 1 )
        cout << dbExecCommand << " --help for help and startup options" << endl;


    processCommandLineOptions(std::vector<std::string>(argv, argv + argc));
    mongo::runGlobalInitializersOrDie(argc, argv, envp);
    CmdLine::censor(argc, argv);

    if (!initializeServerGlobalState())
        ::_exit(EXIT_FAILURE);

#if defined(_WIN32)
    if (ntservice::shouldStartService()) {
        ntservice::startService();
        // exits directly and so never reaches here either.
    }
#endif

    setupSignals( false );

    StartupTest::runTests();
    initAndListen(cmdLine.port);
    dbexit(EXIT_CLEAN);
    return 0;
}

namespace mongo {

    string getDbContext();

#undef out


#if !defined(_WIN32)

} // namespace mongo

#include <signal.h>
#include <string.h>

namespace mongo {

    void abruptQuit(int x) {
        ostringstream ossSig;
        ossSig << "Got signal: " << x << " (" << strsignal( x ) << ")." << endl;
        rawOut( ossSig.str() );

        ostringstream oss;
        oss << "Backtrace:" << endl;
        printStackTrace( oss );
        rawOut( oss.str() );

        // Try to get even more information if gdbPath was set to a gdb executable.
        if (cmdLine.gdbPath != "") {
            db_env_try_gdb_stack_trace(cmdLine.gdbPath.c_str());
        }

        // Reinstall default signal handler, to generate core if necessary.
        signal(x, SIG_DFL);
    }

    void abruptQuitWithAddrSignal( int signal, siginfo_t *siginfo, void * ) {
        ostringstream oss;
        oss << "Invalid";
        if ( signal == SIGSEGV || signal == SIGBUS ) {
            oss << " access";
        } else {
            oss << " operation";
        }
        oss << " at address: " << siginfo->si_addr << " from thread: " << getThreadName() << endl;
        rawOut( oss.str() );
        abruptQuit( signal );
    }

    sigset_t asyncSignals;
    // The above signals will be processed by this thread only, in order to
    // ensure the db and log mutexes aren't held.
    void interruptThread() {
        while (true) {
            int actualSignal = 0;
            int status = sigwait( &asyncSignals, &actualSignal );
            fassert(16854, status == 0);
            switch (actualSignal) {
            case SIGUSR1:
                // log rotate signal
                fassert(16855, rotateLogs());
                break;
            default:
                // interrupt/terminate signal
                Client::initThread( "signalProcessingThread" );
                log() << "got signal " << actualSignal << " (" << strsignal( actualSignal )
                      << "), will terminate after current cmd ends" << endl;
                exitCleanly( EXIT_CLEAN );
                break;
            }
        }
    }

    // this will be called in certain c++ error cases, for example if there are two active
    // exceptions
    void myterminate() {
        rawOut( "terminate() called, printing stack (if implemented for platform):" );
        printStackTrace();
        ::abort();
    }

    // this gets called when new fails to allocate memory
    void my_new_handler() {
        rawOut( "out of memory, printing stack and exiting:" );
        printStackTrace();
        ::_exit(EXIT_ABRUPT);
    }

    void setupSignals_ignoreHelper( int signal ) {}

    void setupQuittingSignals() {
        struct sigaction addrSignals;
        memset( &addrSignals, 0, sizeof( struct sigaction ) );
        addrSignals.sa_sigaction = abruptQuitWithAddrSignal;
        sigemptyset( &addrSignals.sa_mask );
        addrSignals.sa_flags = SA_SIGINFO;

        verify( sigaction(SIGSEGV, &addrSignals, 0) == 0 );
        verify( sigaction(SIGBUS, &addrSignals, 0) == 0 );
        verify( sigaction(SIGILL, &addrSignals, 0) == 0 );
        verify( sigaction(SIGFPE, &addrSignals, 0) == 0 );

        verify( signal(SIGABRT, abruptQuit) != SIG_ERR );
        verify( signal(SIGQUIT, abruptQuit) != SIG_ERR );
        verify( signal(SIGPIPE, SIG_IGN) != SIG_ERR );

        setupSIGTRAPforGDB();

        set_terminate( myterminate );
        set_new_handler( my_new_handler );
    }

    void setupSignals( bool ignored ) {
        sigemptyset( &asyncSignals );
        if ( !cmdLine.gdb ) {
            sigaddset( &asyncSignals, SIGINT );
            sigaddset( &asyncSignals, SIGTERM );
            sigaddset( &asyncSignals, SIGUSR1 );
            verify( pthread_sigmask( SIG_SETMASK, &asyncSignals, 0 ) == 0 );
        }
        boost::thread it( interruptThread );
    }

#else   // WIN32
    void consoleTerminate( const char* controlCodeName ) {
        Client::initThread( "consoleTerminate" );
        log() << "got " << controlCodeName << ", will terminate after current cmd ends" << endl;
        exitCleanly( EXIT_KILL );
    }

    BOOL CtrlHandler( DWORD fdwCtrlType ) {

        switch( fdwCtrlType ) {

        case CTRL_C_EVENT:
            rawOut( "Ctrl-C signal" );
            consoleTerminate( "CTRL_C_EVENT" );
            return TRUE ;

        case CTRL_CLOSE_EVENT:
            rawOut( "CTRL_CLOSE_EVENT signal" );
            consoleTerminate( "CTRL_CLOSE_EVENT" );
            return TRUE ;

        case CTRL_BREAK_EVENT:
            rawOut( "CTRL_BREAK_EVENT signal" );
            consoleTerminate( "CTRL_BREAK_EVENT" );
            return TRUE;

        case CTRL_LOGOFF_EVENT:
            // only sent to services, and only in pre-Vista Windows; FALSE means ignore
            return FALSE;

        case CTRL_SHUTDOWN_EVENT:
            rawOut( "CTRL_SHUTDOWN_EVENT signal" );
            consoleTerminate( "CTRL_SHUTDOWN_EVENT" );
            return TRUE;

        default:
            return FALSE;
        }
    }

    LPTOP_LEVEL_EXCEPTION_FILTER filtLast = 0;

    /* create a process dump.
        To use, load up windbg.  Set your symbol and source path.
        Open the crash dump file.  To see the crashing context, use .ecxr
        */
    void doMinidump(struct _EXCEPTION_POINTERS* exceptionInfo) {
        LPCWSTR dumpFilename = L"mongo.dmp";
        HANDLE hFile = CreateFileW(dumpFilename,
            GENERIC_WRITE,
            0,
            NULL,
            CREATE_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            NULL);
        if ( INVALID_HANDLE_VALUE == hFile ) {
            DWORD lasterr = GetLastError();
            log() << "failed to open minidump file " << toUtf8String(dumpFilename) << " : "
                  << errnoWithDescription( lasterr ) << endl;
            return;
        }

        MINIDUMP_EXCEPTION_INFORMATION aMiniDumpInfo;
        aMiniDumpInfo.ThreadId = GetCurrentThreadId();
        aMiniDumpInfo.ExceptionPointers = exceptionInfo;
        aMiniDumpInfo.ClientPointers = TRUE;

        log() << "writing minidump diagnostic file " << toUtf8String(dumpFilename) << endl;
        BOOL bstatus = MiniDumpWriteDump(GetCurrentProcess(),
            GetCurrentProcessId(),
            hFile,
            MiniDumpNormal,
            &aMiniDumpInfo,
            NULL,
            NULL);
        if ( FALSE == bstatus ) {
            DWORD lasterr = GetLastError();
            log() << "failed to create minidump : "
                  << errnoWithDescription( lasterr ) << endl;
        }

        CloseHandle(hFile);
    }

    LONG WINAPI exceptionFilter( struct _EXCEPTION_POINTERS *excPointers ) {
        char exceptionString[128];
        sprintf_s( exceptionString, sizeof( exceptionString ),
                ( excPointers->ExceptionRecord->ExceptionCode == EXCEPTION_ACCESS_VIOLATION ) ?
                "(access violation)" : "0x%08X", excPointers->ExceptionRecord->ExceptionCode );
        char addressString[32];
        sprintf_s( addressString, sizeof( addressString ), "0x%p",
                 excPointers->ExceptionRecord->ExceptionAddress );
        log() << "*** unhandled exception " << exceptionString <<
                " at " << addressString << ", terminating" << endl;
        if ( excPointers->ExceptionRecord->ExceptionCode == EXCEPTION_ACCESS_VIOLATION ) {
            ULONG acType = excPointers->ExceptionRecord->ExceptionInformation[0];
            const char* acTypeString;
            switch ( acType ) {
            case 0:
                acTypeString = "read from";
                break;
            case 1:
                acTypeString = "write to";
                break;
            case 8:
                acTypeString = "DEP violation at";
                break;
            default:
                acTypeString = "unknown violation at";
                break;
            }
            sprintf_s( addressString, sizeof( addressString ), " 0x%p",
                     excPointers->ExceptionRecord->ExceptionInformation[1] );
            log() << "*** access violation was a " << acTypeString << addressString << endl;
        }

        log() << "*** stack trace for unhandled exception:" << endl;
        printWindowsStackTrace( *excPointers->ContextRecord );
        doMinidump(excPointers);

        // Don't go through normal shutdown procedure. It may make things worse.
        log() << "*** immediate exit due to unhandled exception" << endl;
        ::_exit(EXIT_ABRUPT);

        // We won't reach here
        return EXCEPTION_EXECUTE_HANDLER;
    }

    // called by mongoAbort()
    extern void (*reportEventToSystem)(const char *msg);
    void reportEventToSystemImpl(const char *msg) {
        static ::HANDLE hEventLog = RegisterEventSource( NULL, TEXT("mongod") );
        if( hEventLog ) {
            std::wstring s = toNativeString(msg);
            LPCTSTR txt = s.c_str();
            BOOL ok = ReportEvent(
              hEventLog, EVENTLOG_ERROR_TYPE,
              0, 0, NULL,
              1,
              0,
              &txt,
              0);
            wassert(ok);
        }
    }

    void myPurecallHandler() {
        printStackTrace();
        mongoAbort("pure virtual");
    }

    void setupSignals( bool inFork ) { }

    void setupQuittingSignals() {
        reportEventToSystem = reportEventToSystemImpl;
        filtLast = SetUnhandledExceptionFilter(exceptionFilter);
        massert(10297 , "Couldn't register Windows Ctrl-C handler", SetConsoleCtrlHandler((PHANDLER_ROUTINE) CtrlHandler, TRUE));
        _set_purecall_handler( myPurecallHandler );
    }

#endif  // if !defined(_WIN32)

} // namespace mongo
