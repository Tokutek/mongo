// server.cpp

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

#include "pch.h"

#include <boost/thread/thread.hpp>

#include "mongo/base/init.h"
#include "mongo/base/initializer.h"
#include "mongo/base/status.h"
#include "mongo/client/connpool.h"
#include "mongo/db/auth/authz_manager_external_state_s.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_global.h"
#include "mongo/db/dbwebserver.h"
#include "mongo/db/initialize_server_global_state.h"
#include "../util/startup_test.h"
#include "../util/stringutils.h"
#include "../util/version.h"

#include "mongo/db/lasterror.h"
#include "mongo/plugins/loader.h"
#include "mongo/platform/process_id.h"
#include "mongo/s/balance.h"
#include "mongo/s/chunk.h"
#include "mongo/s/client_info.h"
#include "mongo/s/config.h"
#include "mongo/s/config_server_checker_service.h"
#include "mongo/s/config_upgrade.h"
#include "mongo/s/cursors.h"
#include "mongo/s/grid.h"
#include "mongo/s/mongos_options.h"
#include "mongo/s/request.h"
#include "mongo/s/server.h"
#include "mongo/scripting/engine.h"
#include "mongo/util/admin_access.h"
#include "mongo/util/concurrency/task.h"
#include "mongo/util/concurrency/thread_name.h"
#include "mongo/util/exception_filter_win32.h"
#include "mongo/util/log.h"
#include "mongo/util/net/message.h"
#include "mongo/util/net/message_server.h"
#include "mongo/util/ntservice.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/ramlog.h"
#include "mongo/util/signal_handlers.h"
#include "mongo/util/stacktrace.h"

namespace {
    bool _isUpgradeSwitchSet = false;
}

namespace mongo {

#if defined(_WIN32)
    ntservice::NtServiceDefaultStrings defaultServiceStrings = {
        L"MongoS",
        L"Mongo DB Router",
        L"Mongo DB Sharding Router"
    };
    static void initService();
#endif

    CmdLine cmdLine;
    moe::Environment params;
    moe::OptionSection options("Allowed options");
    Database *database = 0;
    string mongosCommand;
    bool dbexitCalled = false;
    static bool scriptingEnabled = true;
    static vector<string> configdbs;

    bool inShutdown() {
        return dbexitCalled;
    }

    string getDbContext() {
        return "?";
    }

    bool haveLocalShardingInfo( const string& ns ) {
        verify( 0 );
        return false;
    }

    class ShardedMessageHandler : public MessageHandler {
    public:
        virtual ~ShardedMessageHandler() {}

        virtual void connected( AbstractMessagingPort* p ) {
            ClientInfo::create(p);
        }

        virtual void process( Message& m , AbstractMessagingPort* p , LastError * le) {
            verify( p );
            Request r( m , p );

            verify( le );
            lastError.startRequest( m , le );

            try {
                r.init();
                r.process();

                // Release connections after non-write op 
                if ( ShardConnection::releaseConnectionsAfterResponse && r.expectResponse() ) {
                    LOG(2) << "release thread local connections back to pool" << endl;
                    ShardConnection::releaseMyConnections();
                }
            }
            catch ( AssertionException & e ) {
                LOG( e.isUserAssertion() ? 1 : 0 ) << "AssertionException while processing op type : " << m.operation() << " to : " << r.getns() << causedBy(e) << endl;

                le->raiseError( e.getCode() , e.what() );

                m.header()->id = r.id();

                if ( r.expectResponse() ) {
                    BSONObj err = BSON( "$err" << e.what() << "code" << e.getCode() );
                    replyToQuery( ResultFlag_ErrSet, p , m , err );
                }
            }
            catch ( DBException& e ) {
                // note that e.toString() is more detailed on a SocketException than 
                // e.what().  we should think about what is the right level of detail both 
                // for logging and return code.
                log() << "DBException in process: " << e.what() << endl;

                le->raiseError( e.getCode() , e.what() );

                m.header()->id = r.id();

                if ( r.expectResponse() ) {
                    BSONObjBuilder b;
                    b.append("$err",e.what()).append("code",e.getCode());
                    if( !e._shard.empty() ) {
                        b.append("shard",e._shard);
                    }
                    replyToQuery( ResultFlag_ErrSet, p , m , b.obj() );
                }
            }
        }

        virtual void disconnected( AbstractMessagingPort* p ) {
            // all things are thread local
        }
    };

    void sighandler(int sig) {
        dbexit(EXIT_CLEAN, (string("received signal ") + BSONObjBuilder::numStr(sig)).c_str());
    }

    // this gets called when new fails to allocate memory
    void my_new_handler() {
        rawOut( "out of memory, printing stack and exiting:" );
        printStackTrace();
        ::_exit(EXIT_ABRUPT);
    }

    sigset_t asyncSignals;

    void signalProcessingThread() {
        while (true) {
            int actualSignal = 0;
            int status = sigwait( &asyncSignals, &actualSignal );
            fassert(17025, status == 0);
            switch (actualSignal) {
            case SIGUSR1:
                // log rotate signal
                fassert(17026, rotateLogs());
                break;
            default:
                // no one else should be here
                fassertFailed(17027);
                break;
            }
        }
    }

    void startSignalProcessingThread() {
#ifndef _WIN32
        verify( pthread_sigmask( SIG_SETMASK, &asyncSignals, 0 ) == 0 );
        boost::thread it( signalProcessingThread );
#endif
    }

    void setupSignalHandlers() {
        setupSIGTRAPforGDB();
        setupCoreSignals();

        signal(SIGTERM, sighandler);
        if (!cmdLine.gdb) {
            signal(SIGINT, sighandler);
        }
#if defined(SIGXCPU)
        signal(SIGXCPU, sighandler);
#endif

#if defined(SIGQUIT)
        signal( SIGQUIT , printStackAndExit );
#endif
        signal( SIGSEGV , printStackAndExit );
        signal( SIGABRT , printStackAndExit );
        signal( SIGFPE , printStackAndExit );
#if defined(SIGBUS)
        signal( SIGBUS , printStackAndExit );
#endif
#if defined(SIGPIPE)
        signal( SIGPIPE , SIG_IGN );
#endif

#ifndef _WIN32
        sigemptyset( &asyncSignals );
        sigaddset( &asyncSignals, SIGUSR1 );
        startSignalProcessingThread();
#endif

        setWindowsUnhandledExceptionFilter();
        set_new_handler( my_new_handler );
    }

    void init() {
        serverID.init();
    }

    void start( const MessageServer::Options& opts ) {
        balancer.go();
        cursorCache.startTimeoutThread();
        PeriodicTask::theRunner->go();

        ShardedMessageHandler handler;
        MessageServer * server = createServer( opts , &handler );
        server->setAsTimeTracker();
        server->setupSockets();
        server->run();
    }

    DBClientBase *createDirectClient() {
        uassert( 10197 ,  "createDirectClient not implemented for sharding yet" , 0 );
        return 0;
    }

    void printShardingVersionInfo( bool out ) {
        if ( out ) {
            cout << "TokuMX mongos router v" << fullVersionString() << " starting: pid=" <<
                    ProcessId::getCurrent() << " port=" << cmdLine.port <<
                    ( sizeof(int*) == 4 ? " 32" : " 64" ) << "-bit host=" << getHostNameCached() <<
                    " (--help for usage)" << endl;
            DEV cout << "_DEBUG build" << endl;
            cout << "git version: " << gitVersion() << endl;
            cout << openSSLVersion("OpenSSL version: ") << endl;
            cout <<  "build sys info: " << sysInfo() << endl;
        }
        else {
            log() << "TokuMX mongos router v" << fullVersionString() << " starting: pid=" <<
                    ProcessId::getCurrent() << " port=" << cmdLine.port <<
                    ( sizeof( int* ) == 4 ? " 32" : " 64" ) << "-bit host=" << getHostNameCached() <<
                    " (--help for usage)" << endl;
            DEV log() << "_DEBUG build" << endl;
            printGitVersion();
            printTokukvVersion();
            printOpenSSLVersion();
            printSysInfo();
            printCommandLineOpts();
        }
    }

} // namespace mongo

using namespace mongo;

static bool runMongosServer( bool doUpgrade ) {
    setupSignalHandlers();
    setThreadName( "mongosMain" );
    printShardingVersionInfo( false );

    // set some global state

    pool.addHook( new ShardingConnectionHook( false ) );
    pool.setName( "mongos connectionpool" );

    shardConnectionPool.addHook( new ShardingConnectionHook( true ) );
    shardConnectionPool.setName( "mongos shardconnection connectionpool" );

    // Mongos shouldn't lazily kill cursors, otherwise we can end up with extras from migration
    DBClientConnection::setLazyKillCursor( false );

    ReplicaSetMonitor::setConfigChangeHook( boost::bind( &ConfigServer::replicaSetChange , &configServer , _1 ) );

    if ( ! configServer.init( configdbs ) ) {
        log() << "couldn't resolve config db address" << endl;
        return false;
    }

    if ( ! configServer.ok( true ) ) {
        log() << "configServer connection startup check failed" << endl;
        return false;
    }

    startConfigServerChecker();

    VersionType initVersionInfo;
    VersionType versionInfo;
    string errMsg;
    bool upgraded = checkAndUpgradeConfigVersion(ConnectionString(configServer.getPrimary()
                                                         .getConnString()),
                                                 doUpgrade,
                                                 &initVersionInfo,
                                                 &versionInfo,
                                                 &errMsg);

    if (!upgraded) {
        error() << "error upgrading config database to v" << CURRENT_CONFIG_VERSION
                << causedBy(errMsg) << endl;
        return false;
    }

    configServer.reloadSettings();

    init();

    if (!cmdLine.pluginsDir.empty()) {
        plugins::loader->setPluginsDir(cmdLine.pluginsDir);
    }
    plugins::loader->autoload(cmdLine.plugins);

#if !defined(_WIN32)
    CmdLine::launchOk();
#endif

    if ( cmdLine.isHttpInterfaceEnabled )
        boost::thread web( boost::bind(&webServerThread, new NoAdminAccess() /* takes ownership */) );

    MessageServer::Options opts;
    opts.port = cmdLine.port;
    opts.ipList = cmdLine.bind_ip;
    start(opts);

    // listen() will return when exit code closes its socket.
    dbexit( EXIT_NET_ERROR );
    return true;
}

static Status processCommandLineOptions(const std::vector<std::string>& argv) {
    Status ret = addMongosOptions(&options);
    if (!ret.isOK()) {
        StringBuilder sb;
        sb << "Error getting mongos options descriptions: " << ret.toString();
        return Status(ErrorCodes::InternalError, sb.str());
    }

    // parse options
    ret = CmdLine::store(argv, options, params);
    if (!ret.isOK()) {
        std::cerr << "Error parsing command line: " << ret.toString() << std::endl;
        ::_exit(EXIT_BADOPTIONS);
    }

    if ( params.count( "help" ) ) {
        std::cout << options.helpString() << std::endl;
        ::_exit(EXIT_SUCCESS);
    }
    if ( params.count( "version" ) ) {
        printShardingVersionInfo(true);
        ::_exit(EXIT_SUCCESS);
    }

    if ( params.count( "chunkSize" ) ) {
        int csize = params["chunkSize"].as<int>();

        // validate chunksize before proceeding
        if ( csize == 0 ) {
            std::cerr << "error: need a non-zero chunksize" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }

        if ( !Chunk::setMaxChunkSizeSizeMB( csize ) ) {
            std::cerr << "MaxChunkSize invalid" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
    }

    if ( params.count( "localThreshold" ) ) {
        cmdLine.defaultLocalThresholdMillis = params["localThreshold"].as<int>();
    }

    if ( params.count( "ipv6" ) ) {
        enableIPv6();
    }

    if ( params.count( "jsonp" ) ) {
        cmdLine.jsonp = true;
    }

    if ( params.count( "test" ) ) {
        ::mongo::logger::globalLogDomain()->setMinimumLoggedSeverity(
                                                        ::mongo::logger::LogSeverity::Debug(5));
        StartupTest::runTests();
        ::_exit(EXIT_SUCCESS);
    }

    if (params.count("noscripting")) {
        scriptingEnabled = false;
    }

    if (params.count("httpinterface")) {
        if (params.count("nohttpinterface")) {
            std::cerr << "can't have both --httpinterface and --nohttpinterface" << std::endl;
            ::_exit(EXIT_BADOPTIONS);
        }
        cmdLine.isHttpInterfaceEnabled = true;
    }

    if (params.count("noAutoSplit")) {
        warning() << "running with auto-splitting disabled" << endl;
        Chunk::ShouldAutoSplit = false;
    }

    if (params.count("releaseConnectionsAfterResponse")) {
        warning() << "releaseConnectionsAfterResponse set to true" << endl;
        ShardConnection::releaseConnectionsAfterResponse = true;
    }

    if ( ! params.count( "configdb" ) ) {
        std::cerr << "error: no args for --configdb" << std::endl;
        ::_exit(EXIT_BADOPTIONS);
    }

    splitStringDelim( params["configdb"].as<string>() , &configdbs , ',' );
    if ( configdbs.size() != 1 && configdbs.size() != 3 ) {
        std::cerr << "need either 1 or 3 configdbs" << std::endl;
        ::_exit(EXIT_BADOPTIONS);
    }

    if( configdbs.size() == 1 ) {
        warning() << "running with 1 config server should be done only for testing purposes and is not recommended for production" << endl;
    }

    _isUpgradeSwitchSet = params.count("upgrade");

    return Status::OK();
}

MONGO_INITIALIZER_GENERAL(ParseStartupConfiguration,
                            ("GlobalLogManager"),
                            ("default", "completedStartupConfig"))(InitializerContext* context) {

    Status ret = processCommandLineOptions(context->args());
    if (!ret.isOK()) {
        return ret;
    }

    return Status::OK();
}

MONGO_INITIALIZER_GENERAL(ForkServerOrDie,
                          ("completedStartupConfig"),
                          ("default"))(InitializerContext* context) {
    mongo::forkServerOrDie();
    return Status::OK();
}

/*
 * This function should contain the startup "actions" that we take based on the startup config.  It
 * is intended to separate the actions from "storage" and "validation" of our startup configuration.
 */
static void startupConfigActions(const std::vector<std::string>& argv) {
#if defined(_WIN32)
    vector<string> disallowedOptions;
    disallowedOptions.push_back( "upgrade" );
    ntservice::configureService(initService,
                                params,
                                defaultServiceStrings,
                                disallowedOptions,
                                argv);
#endif
}

static int _main() {

    if (!initializeServerGlobalState())
        return EXIT_FAILURE;

    // we either have a setting where all processes are in localhost or none are
    for ( vector<string>::const_iterator it = configdbs.begin() ; it != configdbs.end() ; ++it ) {
        try {

            HostAndPort configAddr( *it );  // will throw if address format is invalid

            if ( it == configdbs.begin() ) {
                grid.setAllowLocalHost( configAddr.isLocalHost() );
            }

            if ( configAddr.isLocalHost() != grid.allowLocalHost() ) {
                out() << "cannot mix localhost and ip addresses in configdbs" << endl;
                return 10;
            }

        }
        catch ( DBException& e) {
            out() << "configdb: " << e.what() << endl;
            return 9;
        }
    }

#if defined(_WIN32)
    if (ntservice::shouldStartService()) {
        ntservice::startService();
        // if we reach here, then we are not running as a service.  service installation
        // exits directly and so never reaches here either.
    }
#endif

    return !runMongosServer(_isUpgradeSwitchSet);
}

#if defined(_WIN32)
namespace mongo {
    static void initService() {
        ntservice::reportStatus( SERVICE_RUNNING );
        log() << "Service running" << endl;
        runMongosServer( false );
    }
}  // namespace mongo
#endif

MONGO_INITIALIZER(CreateAuthorizationManager)(InitializerContext* context) {
    setGlobalAuthorizationManager(new AuthorizationManager(new AuthzManagerExternalStateMongos()));
    return Status::OK();
}

int main(int argc, char* argv[], char** envp) {
    static StaticObserver staticObserver;
    if (argc < 1)
        ::_exit(EXIT_FAILURE);

    mongosCommand = argv[0];

    mongo::runGlobalInitializersOrDie(argc, argv, envp);
    startupConfigActions(std::vector<std::string>(argv, argv + argc));
    CmdLine::censor(argc, argv);
    try {
        int exitCode = _main();
        ::_exit(exitCode);
    }
    catch(SocketException& e) {
        cout << "uncaught SocketException in mongos main:" << endl;
        cout << e.toString() << endl;
    }
    catch(DBException& e) {
        cout << "uncaught DBException in mongos main:" << endl;
        cout << e.toString() << endl;
    }
    catch(std::exception& e) {
        cout << "uncaught std::exception in mongos main:" << endl;
        cout << e.what() << endl;
    }
    catch(...) {
        cout << "uncaught unknown exception in mongos main" << endl;
    }
    ::_exit(20);
}

#undef exit

void mongo::exitCleanly( ExitCode code ) {
    // TODO: do we need to add anything?
    plugins::loader->shutdown();
    mongo::dbexit( code );
}

void mongo::dbexit( ExitCode rc, const char *why ) {
    dbexitCalled = true;
#if defined(_WIN32)
    if ( rc == EXIT_WINDOWS_SERVICE_STOP ) {
        log() << "dbexit: exiting because Windows service was stopped" << endl;
        return;
    }
#endif
    log() << "dbexit: " << why
          << " rc:" << rc
          << " " << ( why ? why : "" )
          << endl;
#ifdef _COVERAGE
    // Need to make sure coverage data is properly flushed before exit.
    // It appears that ::_exit() does not do this.
    log() << "calling regular ::exit() so coverage data may flush..." << endl;
    ::exit(rc);
#else
    ::_exit(rc);
#endif
}
