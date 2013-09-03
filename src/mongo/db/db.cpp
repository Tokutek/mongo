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

#include <db.h>

#include "mongo/base/init.h"
#include "mongo/base/initializer.h"
#include "mongo/base/status.h"
#include "mongo/db/auth/authz_manager_external_state_d.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_global.h"
#include "mongo/db/collection.h"
#include "mongo/db/collection_map.h"
#include "mongo/db/client.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/crash.h"
#include "mongo/db/cursor.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/d_globals.h"
#include "mongo/db/database.h"
#include "mongo/db/dbmessage.h"
#include "mongo/db/dbwebserver.h"
#include "mongo/db/initialize_server_global_state.h"
#include "mongo/db/instance.h"
#include "mongo/db/introspect.h"
#include "mongo/db/json.h"
#include "mongo/db/kill_current_op.h"
#include "mongo/db/log_process_details.h"
#include "mongo/db/module.h"
#include "mongo/db/mongod_options.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/repl.h"
#include "mongo/db/repl/replication_server_status.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/restapi.h"
#include "mongo/db/startup_warnings.h"
#include "mongo/db/stats/counters.h"
#include "mongo/db/stats/snapshots.h"
#include "mongo/db/storage_options.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/ttl.h"
#include "mongo/db/txn_complete_hooks.h"
#include "mongo/plugins/loader.h"
#include "mongo/platform/process_id.h"
#include "mongo/s/d_writeback.h"
#include "mongo/scripting/engine.h"
#include "mongo/util/background.h"
#include "mongo/util/cmdline_utils/censor_cmdline.h"
#include "mongo/util/concurrency/task.h"
#include "mongo/util/concurrency/thread_name.h"
#include "mongo/util/exception_filter_win32.h"
#include "mongo/util/net/message_server.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/ntservice.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/ramlog.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/signal_win32.h"
#include "mongo/util/stacktrace.h"
#include "mongo/util/startup_test.h"
#include "mongo/util/text.h"
#include "mongo/util/version.h"
#include "mongo/util/version_reporting.h"

#if !defined(_WIN32)
# include <sys/file.h>
#endif

namespace mongo {

    /* only off if --nohints */
    extern bool useHints;

    extern int diagLogging;
    extern int lockFile;

    static void setupSignalHandlers();
    void startReplication();
    static void startSignalProcessingThread();
    void exitCleanly( ExitCode code );

#ifdef _WIN32
    ntservice::NtServiceDefaultStrings defaultServiceStrings = {
        L"MongoDB",
        L"Mongo DB",
        L"Mongo DB Server"
    };
#endif

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
        toLog.append( "startTimeLocal", dateToCtimeString(curTimeMillis64()) );

        toLog.append("cmdLine", serverGlobalParams.parsedOpts);
        toLog.append( "pid", ProcessId::getCurrent().asLongLong() );

        BSONObjBuilder buildinfo( toLog.subobjStart("buildinfo"));
        appendBuildInfo(buildinfo);
        buildinfo.doneFast();

        const BSONObj o = toLog.obj();
        Client::GodScope gs;
        DBDirectClient c;
        c.insert("local.startup_log", o);
    }

    void listen(int port) {
        //testTheDb();
        MessageServer::Options options;
        options.port = port;
        options.ipList = serverGlobalParams.bind_ip;

        MessageServer * server = createServer( options , new MyMessageHandler() );
        server->setAsTimeTracker();
        // we must setupSockets prior to logStartup() to avoid getting too high
        // a file descriptor for our calls to select()
        server->setupSockets();

        logStartup();
        startReplication();
        if (serverGlobalParams.isHttpInterfaceEnabled)
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
        LOCK_REASON(lockReason, "startup: checking whether --replSet is missing");
        Lock::GlobalWrite lk(lockReason);
        if (!replSettings.usingReplSets()) {
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

    class DiskFormatVersion {
      public:
        enum VersionID {
            DISK_VERSION_INVALID = 0,
            DISK_VERSION_1 = 1,  // < 1.4
            DISK_VERSION_2 = 2,  // 1.4.0: changed how ints with abs value larger than 2^52 are packed (#820)
            DISK_VERSION_3 = 3,  // 1.4.0+hotfix.1: moved upgrade of system.users collections into DiskFormatVersion framework (#978)
            DISK_VERSION_4 = 4,  // 1.4.1: remove old partitioned collection entries from system.namespaces (#967)
            DISK_VERSION_5 = 5,  // 1.4.2: #1087, fix indexes that were not properly serialized
            DISK_VERSION_NEXT,
            DISK_VERSION_CURRENT = DISK_VERSION_NEXT - 1,
            MIN_SUPPORTED_VERSION = 1,
            MAX_SUPPORTED_VERSION = DISK_VERSION_CURRENT,
            FIRST_SERIALIZED_VERSION = DISK_VERSION_2,
        };

      private:
        VersionID _startupVersion;
        VersionID _currentVersion;
        static size_t _numNamespaces;
        static ProgressMeterHolder *_pm;

        static const string versionNs;
        static const BSONFieldValue<string> versionIdValue;
        static const BSONField<int> valueField;
        static const BSONField<int> upgradedToField;
        static const BSONField<Date_t> upgradedAtField;
        static const BSONField<BSONObj> upgradedByField;
        static const BSONField<string> tokumxVersionField;
        static const BSONField<string> mongodbVersionField;
        static const BSONField<string> tokumxGitField;
        static const BSONField<string> tokukvGitField;


        static Status countNamespaces(const StringData&) {
            _numNamespaces++;
            return Status::OK();
        }

        static Status removeZombieNamespaces(const StringData &dbname) {
            Client::Context ctx(dbname);
            CollectionMap *cm = collectionMap(dbname);
            if (!cm) {
                return Status(ErrorCodes::InternalError, mongoutils::str::stream() << "did not find collection map for database " << dbname);
            }
            list<string> collNses;
            cm->getNamespaces(collNses);
            for (list<string>::const_iterator cit = collNses.begin(); cit != collNses.end(); ++cit) {
                const string &collNs = *cit;
                Collection *c = getCollection(collNs);
                if (c == NULL) {
                    LOG(1) << "collection " << collNs << " was dropped but had a zombie entry in the collection map" << startupWarningsLog;
                    cm->kill_ns(collNs);
                }
            }
            string dbpath = cc().database()->path();
            Database::closeDatabase(dbname, dbpath);
            _pm->hit();
            return Status::OK();
        }

        static Status upgradeSystemUsersCollection(const StringData &dbname) {
            Client::UpgradingSystemUsersScope usus;
            string ns = getSisterNS(dbname, "system.users");
            Client::Context ctx(ns);
            // Just by calling getCollection, if a collection that needed upgrade is opened, it'll
            // get upgraded.  This fixes #674.
            getCollection(ns);
            string dbpath = cc().database()->path();
            Database::closeDatabase(dbname, dbpath);
            _pm->hit();
            return Status::OK();
        }

        static Status cleanupPartitionedNamespacesEntries(const StringData &dbname) {
            string ns = getSisterNS(dbname, "system.namespaces");
            Client::Context ctx(ns);
            Collection *nscl = getCollection(ns);
            if (nscl == NULL) {
                return Status(ErrorCodes::InternalError, mongoutils::str::stream() << "didn't find system.namespaces collection for db " << dbname);
            }
            for (shared_ptr<Cursor> cursor = Cursor::make(nscl); cursor->ok(); cursor->advance()) {
                BSONObj cur = cursor->current();
                if (!cur.getObjectField("options")["partitioned"].trueValue()) {
                    continue;
                }
                string pat = mongoutils::str::stream() << "^\\Q" << cur["name"].String() + "$$\\E";
                long long ndeleted = _deleteObjects(ns.c_str(), BSON("name" << BSONRegEx(pat)), false, false);
                if (ndeleted < 1) {
                    LOG(0) << "didn't find any system.namespaces entries to delete for partitioned collection "
                           << cur["name"].Stringdata() << ", used pattern /" << pat << "/." << startupWarningsLog;
                }
            }
            string dbpath = cc().database()->path();
            Database::closeDatabase(dbname, dbpath);
            _pm->hit();
            return Status::OK();
        }

        static Status fixMissingIndexesInNS(const StringData &dbname) {
            string ns = getSisterNS(dbname, "system.indexes");
            Client::Context ctx(ns);
            Collection *sysIndexes = getCollection(ns);
            if (sysIndexes == NULL) {
                return Status(ErrorCodes::InternalError, mongoutils::str::stream() << "didn't find system.indexes collection for db " << dbname);
            }
            for (shared_ptr<Cursor> cursor = Cursor::make(sysIndexes); cursor->ok(); cursor->advance()) {
                BSONObj cur = cursor->current();
                if (!cur["background"].trueValue()) {
                    continue;
                }
                StringData collns = cur["ns"].Stringdata();
                Collection *cl = getCollection(collns);
                // It's possible for the collection to have been dropped.  If so, need to clean up
                // system.indexes and system.namespaces because those entries may not have been
                // deleted by the collection's drop.
                if (!cl || cl->indexIsOrphaned(cur)) {
                    // Don't warn the user if the collection was dropped.
                    if (cl) {
                        warning() << "Found an orphaned secondary index on collection " << collns << ": " << cur << startupWarningsLog;
                        warning() << "This is due to issue #1087 (https://github.com/Tokutek/mongo/issues/1087)." << startupWarningsLog;
                        warning() << "You will need to rebuild this index after this upgrade is complete." << startupWarningsLog;
                        warning() << "To do this, run this command in the shell:" << startupWarningsLog;
                        warning() << "> use " << dbname << startupWarningsLog;
                        warning() << "> db.system.indexes.insert(" << cur << ")" << startupWarningsLog;
                    }
                    cleanupOrphanedIndex(cur);
                }
            }
            _pm->hit();
            return Status::OK();
        }

        Status upgradeToVersion(VersionID targetVersion) {
            if (_currentVersion + 1 != targetVersion) {
                return Status(ErrorCodes::BadValue, "bad version in upgrade");
            }

            std::stringstream upgradeLogPrefixStream;
            upgradeLogPrefixStream << "Running upgrade of disk format version " << static_cast<int>(_currentVersion)
                                   << " to " << static_cast<int>(targetVersion);
            std::string upgradeLogPrefix = upgradeLogPrefixStream.str();
            log() << upgradeLogPrefix << "." << endl;

            // This is pretty awkward.  We want a static member to point to a stack
            // ProgressMeterHolder so it can be used by other static member callback functions, but
            // not longer than that object exists.  TODO: maybe it would be easier to pass a mem_fn
            // to applyToDatabaseNames, but this isn't too crazy yet.
            class ScopedPMH : boost::noncopyable {
                ProgressMeterHolder *&_pmhp;
              public:
                ScopedPMH(ProgressMeterHolder *&pmhp, ProgressMeterHolder *val) : _pmhp(pmhp) {
                    _pmhp = val;
                }
                ~ScopedPMH() {
                    _pmhp = NULL;
                }
            };

            switch (targetVersion) {
                case DISK_VERSION_INVALID:
                case DISK_VERSION_1:
                case DISK_VERSION_NEXT: {
                    warning() << "should not be trying to upgrade to " << static_cast<int>(targetVersion) << startupWarningsLog;
                    return Status(ErrorCodes::BadValue, "bad version in upgrade");
                }

                case DISK_VERSION_2: {
                    // Due to #879, we need to look at each existing database and remove
                    // entries from the collection map which were dropped but their entries
                    // weren't removed.
                    verify(Lock::isW());
                    verify(cc().hasTxn());

                    ProgressMeter pmObj(_numNamespaces, 3, 1, "databases", upgradeLogPrefix);
                    ProgressMeterHolder pm(pmObj);
                    ScopedPMH scpmh(_pm, &pm);

                    Status s = applyToDatabaseNames(&DiskFormatVersion::removeZombieNamespaces);
                    if (!s.isOK()) {
                        return s;
                    }

                    break;
                }

                case DISK_VERSION_3: {
                    // We used to do this (force upgrade of system.users collections in a write
                    // lock) in initAndListen but it should really be in the upgrade path, and this
                    // way we won't do it on every startup, just one more time on upgrade to version
                    // 3.
                    verify(Lock::isW());
                    verify(cc().hasTxn());

                    ProgressMeter pmObj(_numNamespaces, 3, 1, "databases", upgradeLogPrefix);
                    ProgressMeterHolder pm(pmObj);
                    ScopedPMH scpmh(_pm, &pm);

                    Status s = applyToDatabaseNames(&DiskFormatVersion::upgradeSystemUsersCollection);
                    if (!s.isOK()) {
                        return s;
                    }

                    break;
                }

                case DISK_VERSION_4: {
                    verify(Lock::isW());
                    verify(cc().hasTxn());

                    ProgressMeter pmObj(_numNamespaces, 3, 1, "databases", upgradeLogPrefix);
                    ProgressMeterHolder pm(pmObj);
                    ScopedPMH scpmh(_pm, &pm);

                    Status s = applyToDatabaseNames(&DiskFormatVersion::cleanupPartitionedNamespacesEntries);
                    if (!s.isOK()) {
                        return s;
                    }

                    break;
                }
                case DISK_VERSION_5: {
                    verify(Lock::isW());
                    verify(cc().hasTxn());

                    ProgressMeter pmObj(_numNamespaces, 3, 1, "databases", upgradeLogPrefix);
                    ProgressMeterHolder pm(pmObj);
                    ScopedPMH scpmh(_pm, &pm);

                    Status s = applyToDatabaseNames(&DiskFormatVersion::fixMissingIndexesInNS);
                    if (!s.isOK()) {
                        return s;
                    }

                    break;
                }
            }

            verify(Lock::isW());
            verify(cc().hasTxn());
            Client::Context lctx(versionNs);
            updateObjects(versionNs.c_str(),
                          BSON(versionIdValue << valueField(targetVersion)),
                          BSON(versionIdValue),
                          true,    // upsert
                          false   // multi
                          );  // logop

            // Keep a little history of what we've done
            insertObject(versionNs.c_str(), BSON(upgradedToField(targetVersion) <<
                                                 upgradedAtField(jsTime()) <<
                                                 upgradedByField(BSON(tokumxVersionField(tokumxVersionString) <<
                                                                      mongodbVersionField(mongodbVersionString) <<
                                                                      tokumxGitField(gitVersion()) <<
                                                                      tokukvGitField(tokukvVersion())))),
                         0, false, false);
            _currentVersion = targetVersion;
            return Status::OK();
        }

      public:
        DiskFormatVersion()
                : _startupVersion(DISK_VERSION_INVALID),
                  _currentVersion(DISK_VERSION_INVALID) {}

        Status initialize() {
            verify(Lock::isW());
            verify(cc().hasTxn());

            Client::Context ctx(versionNs);

            Collection *c = getCollection(versionNs);
            if (c == NULL) {
                _startupVersion = DISK_VERSION_1;
            } else {
                BSONObj versionObj;
                bool ok = c->findOne(versionNs, BSON(versionIdValue), versionObj);
                if (!ok) {
                    warning() << "found local.system.version collection but query " << BSON(versionIdValue) << " found nothing" << startupWarningsLog;
                    _startupVersion = DISK_VERSION_1;
                } else {
                    BSONElement versionElt = versionObj[valueField()];
                    if (!versionElt.isNumber()) {
                        return Status(ErrorCodes::BadValue, mongoutils::str::stream() << "found malformed version object " << versionObj);
                    }
                    _startupVersion = static_cast<VersionID>(versionElt.numberLong());
                }
            }

            if (_startupVersion < MIN_SUPPORTED_VERSION) {
                warning() << "Found unsupported disk format version: " << static_cast<int>(_startupVersion) << "." << startupWarningsLog;
                warning() << "The minimum supported disk format version by TokuMX " << tokumxVersionString
                          << " is " << static_cast<int>(MIN_SUPPORTED_VERSION) << "." << startupWarningsLog;
                warning() << "Please upgrade to an earlier version of TokuMX before upgrading to this version." << startupWarningsLog;
                return Status(ErrorCodes::UnsupportedFormat, "version on disk too low");
            }

            if (_startupVersion > MAX_SUPPORTED_VERSION) {
                warning() << "Found unsupported disk format version: " << static_cast<int>(_startupVersion) << "." << startupWarningsLog;
                warning() << "The maximum supported disk format version by TokuMX " << tokumxVersionString
                          << " is " << static_cast<int>(MAX_SUPPORTED_VERSION) << "." << startupWarningsLog;
                warning() << "Please upgrade to a later version of TokuMX to use the data on disk." << startupWarningsLog;
                return Status(ErrorCodes::UnsupportedFormat, "version on disk too high");
            }

            _currentVersion = _startupVersion;

            return Status::OK();
        }

        Status upgradeToCurrent() {
            Status s = Status::OK();
            if (_currentVersion < DISK_VERSION_CURRENT) {
                s = applyToDatabaseNames(&DiskFormatVersion::countNamespaces);
                log() << "Need to upgrade from disk format version " << static_cast<int>(_currentVersion)
                      << " to " << static_cast<int>(DISK_VERSION_CURRENT) << "." << endl;
                log() << _numNamespaces << " databases will be upgraded." << endl;
            }
            while (_currentVersion < DISK_VERSION_CURRENT && s.isOK()) {
                s = upgradeToVersion(static_cast<VersionID>(static_cast<int>(_currentVersion) + 1));
            }
            return s;
        }
    };

    size_t DiskFormatVersion::_numNamespaces = 0;
    ProgressMeterHolder *DiskFormatVersion::_pm = NULL;
    const string DiskFormatVersion::versionNs("local.system.version");
    const BSONFieldValue<string> DiskFormatVersion::versionIdValue("_id", "diskFormatVersion");
    const BSONField<int> DiskFormatVersion::valueField("value");
    const BSONField<int> DiskFormatVersion::upgradedToField("upgradedTo");
    const BSONField<Date_t> DiskFormatVersion::upgradedAtField("upgradedAt");
    const BSONField<BSONObj> DiskFormatVersion::upgradedByField("upgradedBy");
    const BSONField<string> DiskFormatVersion::tokumxVersionField("tokumxVersion");
    const BSONField<string> DiskFormatVersion::mongodbVersionField("mongodbVersion");
    const BSONField<string> DiskFormatVersion::tokumxGitField("tokumx");
    const BSONField<string> DiskFormatVersion::tokukvGitField("tokukv");

    void _initAndListen(int listenPort ) {

        Client::initThread("initandlisten");

        bool is32bit = sizeof(int*) == 4;

        {
            ProcessId pid = ProcessId::getCurrent();
            LogstreamBuilder l = log();
            l << "TokuMX starting : pid=" << pid
              << " port=" << serverGlobalParams.port
              << " dbpath=" << storageGlobalParams.dbpath;
            l << ( is32bit ? " 32" : " 64" ) << "-bit host=" << getHostNameCached() << endl;
        }
        DEV log() << "_DEBUG build (which is slower)" << endl;
        logStartupWarnings();
#if defined(_WIN32)
        printTargetMinOS();
#endif
        logProcessDetails();
        {
            stringstream ss;
            ss << endl;
            ss << "*********************************************************************" << endl;
            ss << " ERROR: dbpath (" << storageGlobalParams.dbpath << ") does not exist." << endl;
            ss << " Create this directory or give existing directory in --dbpath." << endl;
            ss << " See http://dochub.mongodb.org/core/startingandstoppingmongo" << endl;
            ss << "*********************************************************************" << endl;
            uassert(10296,  ss.str().c_str(), boost::filesystem::exists(storageGlobalParams.dbpath));
        }

        acquirePathLock();

        extern storage::UpdateCallback _storageUpdateCallback;
        storage::startup(&_txnCompleteHooks, &_storageUpdateCallback);

        {
            LOCK_REASON(lockReason, "startup: running upgrade hooks");
            Lock::GlobalWrite lk(lockReason);
            Client::UpgradingDiskFormatVersionScope udfvs;
            Client::Transaction txn(DB_SERIALIZABLE);
            DiskFormatVersion formatVersion;
            Status s = formatVersion.initialize();
            if (!s.isOK()) {
                warning() << "Error while fetching disk format version: " << s << "." << startupWarningsLog;
                exitCleanly(EXIT_NEED_UPGRADE);
            }
            s = formatVersion.upgradeToCurrent();
            if (!s.isOK()) {
                warning() << "Error while upgrading disk format version: " << s << "." << startupWarningsLog;
                exitCleanly(EXIT_NEED_UPGRADE);
            }
            txn.commit();
        }

        unsigned long long missingRepl = checkIfReplMissingFromCommandLine();
        if (missingRepl) {
            log() << startupWarningsLog;
            log() << "** WARNING: mongod started without --replSet yet " << missingRepl
                  << " documents are present in local.system.replset" << startupWarningsLog;
            log() << "**          Restart with --replSet unless you are doing maintenance and no"
                  << " other clients are connected." << startupWarningsLog;
            log() << "**          The TTL collection monitor will not start because of this." << startupWarningsLog;
            log() << "**          For more info see http://dochub.mongodb.org/core/ttlcollections" << startupWarningsLog;
            log() << startupWarningsLog;
        }

        Module::initAll();

        if (mongodGlobalParams.scriptingEnabled) {
            ScriptEngine::setup();
            globalScriptEngine->setCheckInterruptCallback( jsInterruptCallback );
            globalScriptEngine->setGetCurrentOpIdCallback( jsGetCurrentOpIdCallback );
        }

        AuthorizationManager* authzManager = getGlobalAuthorizationManager();
        if (authzManager->isAuthEnabled()) {
            Status status = getGlobalAuthorizationManager()->initialize();
            uassertStatusOK(status);
        }


        /* this is for security on certain platforms (nonce generation) */
        srand((unsigned) (curTimeMicros() ^ startupSrandTimer.micros()));

        if (!serverGlobalParams.pluginsDir.empty()) {
            plugins::loader->setPluginsDir(serverGlobalParams.pluginsDir);
        }
        plugins::loader->autoload(serverGlobalParams.plugins);

        snapshotThread.go();
        d.clientCursorMonitor.go();
        PeriodicTask::startRunningPeriodicTasks();
        if (missingRepl) {
            // a warning was logged earlier
        }
        else {
            startTTLBackgroundJob();
        }

#ifndef _WIN32
        mongo::signalForkSuccess();
#endif

        if(AuthorizationManager::isAuthEnabled()) {
            // open admin db in case we need to use it later. TODO this is not the right way to
            // resolve this.
            LOCK_REASON(lockReason, "startup: opening admin db");
            Client::WriteContext c("admin", lockReason);
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
        initAndListen(serverGlobalParams.port);
    }
#endif

} // namespace mongo

using namespace mongo;

static int mongoDbMain(int argc, char* argv[], char** envp);

int main(int argc, char* argv[], char** envp) {
    int exitCode = mongoDbMain(argc, argv, envp);
    ::_exit(exitCode);
}

MONGO_INITIALIZER_GENERAL(ForkServer,
                          ("EndStartupOptionHandling"),
                          ("default"))(InitializerContext* context) {
    mongo::forkServerOrDie();
    return Status::OK();
}

/*
 * This function should contain the startup "actions" that we take based on the startup config.  It
 * is intended to separate the actions from "storage" and "validation" of our startup configuration.
 */
static void startupConfigActions(const std::vector<std::string>& args) {
    // The "command" option is deprecated.  For backward compatibility, still support the "run"
    // and "dbppath" command.  The "run" command is the same as just running mongod, so just
    // falls through.
    if (moe::startupOptionsParsed.count("command")) {
        vector<string> command = moe::startupOptionsParsed["command"].as< vector<string> >();

        if (command[0].compare("dbpath") == 0) {
            cout << storageGlobalParams.dbpath << endl;
            ::_exit(EXIT_SUCCESS);
        }

        if (command[0].compare("run") != 0) {
            cout << "Invalid command: " << command[0] << endl;
            printMongodHelp(moe::startupOptions);
            ::_exit(EXIT_FAILURE);
        }

        if (command.size() > 1) {
            cout << "Too many parameters to 'run' command" << endl;
            printMongodHelp(moe::startupOptions);
            ::_exit(EXIT_FAILURE);
        }
    }

    Module::configAll(moe::startupOptionsParsed);

#ifdef _WIN32
    ntservice::configureService(initService,
            moe::startupOptionsParsed,
            defaultServiceStrings,
            std::vector<std::string>(),
            args);
#endif  // _WIN32

#ifdef __linux__
    if (moe::startupOptionsParsed.count("shutdown")){
        bool failed = false;

        string name = (boost::filesystem::path(storageGlobalParams.dbpath) / "mongod.lock").string();
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
            std::cerr << "There doesn't seem to be a server running with dbpath: "
                      << storageGlobalParams.dbpath << std::endl;
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

MONGO_INITIALIZER_GENERAL(CreateAuthorizationManager,
                          ("SetupInternalSecurityUser"),
                          MONGO_NO_DEPENDENTS)
        (InitializerContext* context) {
    AuthorizationManager* authzManager =
            new AuthorizationManager(new AuthzManagerExternalStateMongod());
    authzManager->addInternalUser(internalSecurity.user);
    setGlobalAuthorizationManager(authzManager);
    return Status::OK();
}

#ifdef MONGO_SSL
MONGO_INITIALIZER_GENERAL(setSSLManagerType, 
                          MONGO_NO_PREREQUISITES, 
                          ("SSLManager"))(InitializerContext* context) {
    isSSLServer = true;
    return Status::OK();
}
#endif

static int mongoDbMain(int argc, char* argv[], char **envp) {
    static StaticObserver staticObserver;

    getcurns = ourgetns;

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

    setupSignalHandlers();
    mongo::runGlobalInitializersOrDie(argc, argv, envp);
    startupConfigActions(std::vector<std::string>(argv, argv + argc));
    cmdline_utils::censorArgvArray(argc, argv);

    if (!initializeServerGlobalState())
        ::_exit(EXIT_FAILURE);

    // Per SERVER-7434, startSignalProcessingThread() must run after any forks
    // (initializeServerGlobalState()) and before creation of any other threads.
    startSignalProcessingThread();

#if defined(_WIN32)
    if (ntservice::shouldStartService()) {
        ntservice::startService();
        // exits directly and so never reaches here either.
    }
#endif

    StartupTest::runTests();
    initAndListen(serverGlobalParams.port);
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

    static void abruptQuitNoCrashInfo(int x) {
        severe() << "Got signal: " << x << " (" << strsignal( x ) << ")." << std::endl;

        // Reinstall default signal handler, to generate core if necessary.
        signal(x, SIG_DFL);
    }

    void abruptQuit(int x) {
        ostringstream ossSig;
        ossSig << "Got signal: " << x << " (" << strsignal( x ) << ").";
        dumpCrashInfo(ossSig.str());

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
        dumpCrashInfo(oss.str());
        abruptQuitNoCrashInfo(signal);
    }

    sigset_t asyncSignals;
    // The signals in asyncSignals will be processed by this thread only, in order to
    // ensure the db and log mutexes aren't held.
    void signalProcessingThread() {
        while (true) {
            int actualSignal = 0;
            int status = sigwait( &asyncSignals, &actualSignal );
            fassert(17023, status == 0);
            switch (actualSignal) {
            case SIGUSR1:
                // log rotate signal
                fassert(17024, rotateLogs());
                logProcessDetailsForLogRotate();
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
        dumpCrashInfo("terminate() called, printing stack (if implemented for platform):\n");
        ::abort();
    }

    // this gets called when new fails to allocate memory
    void my_new_handler() {
        dumpCrashInfo("out of memory, printing stack and exiting:\n");
        ::_exit(EXIT_ABRUPT);
    }

    void setupSignals_ignoreHelper( int signal ) {}

    void setupSignalHandlers() {
        setupCoreSignals();

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

        // asyncSignals is a global variable listing the signals that should be handled by the
        // interrupt thread, once it is started via startSignalProcessingThread().
        sigemptyset( &asyncSignals );
        sigaddset( &asyncSignals, SIGTERM );
        sigaddset( &asyncSignals, SIGHUP );
        if (!serverGlobalParams.gdb) {
            sigaddset( &asyncSignals, SIGINT );
        }
        sigaddset( &asyncSignals, SIGUSR1 );
        sigaddset( &asyncSignals, SIGXCPU );

        set_terminate( myterminate );
        set_new_handler( my_new_handler );
    }

    void startSignalProcessingThread() {
        verify( pthread_sigmask( SIG_SETMASK, &asyncSignals, 0 ) == 0 );
        boost::thread it( signalProcessingThread );
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
            log() << "Ctrl-C signal";
            consoleTerminate( "CTRL_C_EVENT" );
            return TRUE ;

        case CTRL_CLOSE_EVENT:
            log() << "CTRL_CLOSE_EVENT signal";
            consoleTerminate( "CTRL_CLOSE_EVENT" );
            return TRUE ;

        case CTRL_BREAK_EVENT:
            log() << "CTRL_BREAK_EVENT signal";
            consoleTerminate( "CTRL_BREAK_EVENT" );
            return TRUE;

        case CTRL_LOGOFF_EVENT:
            // only sent to services, and only in pre-Vista Windows; FALSE means ignore
            return FALSE;

        case CTRL_SHUTDOWN_EVENT:
            log() << "CTRL_SHUTDOWN_EVENT signal";
            consoleTerminate( "CTRL_SHUTDOWN_EVENT" );
            return TRUE;

        default:
            return FALSE;
        }
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

    void setupSignalHandlers() {
        reportEventToSystem = reportEventToSystemImpl;
        setWindowsUnhandledExceptionFilter();
        massert(10297,
                "Couldn't register Windows Ctrl-C handler",
                SetConsoleCtrlHandler(static_cast<PHANDLER_ROUTINE>(CtrlHandler), TRUE));
        _set_purecall_handler( myPurecallHandler );
    }

    void eventProcessingThread() {
        std::string eventName = getShutdownSignalName(ProcessId::getCurrent().asUInt32());

        HANDLE event = CreateEventA(NULL, TRUE, FALSE, eventName.c_str());
        if (event == NULL) {
            warning() << "eventProcessingThread CreateEvent failed: "
                << errnoWithDescription();
            return;
        }

        ON_BLOCK_EXIT(CloseHandle, event);

        int returnCode = WaitForSingleObject(event, INFINITE);
        if (returnCode != WAIT_OBJECT_0) {
            if (returnCode == WAIT_FAILED) {
                warning() << "eventProcessingThread WaitForSingleObject failed: "
                    << errnoWithDescription();
                return;
            }
            else {
                warning() << "eventProcessingThread WaitForSingleObject failed: "
                    << errnoWithDescription(returnCode);
                return;
            }
        }

        Client::initThread("eventTerminate");
        log() << "shutdown event signaled, will terminate after current cmd ends";
        exitCleanly(EXIT_KILL);
    }

    void startSignalProcessingThread() {
        if (Command::testCommandsEnabled) {
            boost::thread it(eventProcessingThread);
        }
    }

#endif  // if !defined(_WIN32)

} // namespace mongo
