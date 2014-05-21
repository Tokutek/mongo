// 2toku.cpp

/**
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

#include <exception>
#include <fstream>
#include <iostream>
#include <signal.h>
#include <string.h>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>

#include "mongo/tools/tool.h"

#include "mongo/base/error_codes.h"
#include "mongo/base/initializer.h"
#include "mongo/base/string_data.h"
#include "mongo/client/connpool.h"
#include "mongo/db/jsobj.h"
#include "mongo/util/password.h"
#include "mongo/util/timer.h"

using namespace mongo;

namespace po = boost::program_options;

static string fmtOpTime(const OpTime &t) {
    stringstream ss;
    ss << t.getSecs() << ":" << t.getInc() << " (" << t.toStringPretty() << ")";
    return ss.str();
}

class VanillaOplogPlayer : boost::noncopyable {
    mongo::DBClientBase &_conn;
    string _host;
    OpTime _maxOpTimeSynced;
    OpTime _thisTime;
    vector<BSONObj> _insertBuf;
    string _insertNs;
    size_t _insertSize;
    OpTime _insertMaxTime;

    volatile bool &_running;
    bool &_logAtExit;

    void pushInsert(const StringData &ns, const BSONObj &o) {
        uassert(16863, "cannot append an earlier optime", _thisTime > _insertMaxTime);
        // seems like enough room for headers/metadata
        static const size_t MAX_SIZE = BSONObjMaxUserSize - (4<<10);
        if (ns != _insertNs || _insertSize + o.objsize() > MAX_SIZE) {
            flushInserts();
            _insertNs = ns.toString();
        }
        _insertBuf.push_back(o.getOwned());
        _insertSize += o.objsize();
        _insertMaxTime = _thisTime;
    }

  public:
    VanillaOplogPlayer(mongo::DBClientBase &conn, const string &host, const OpTime &maxOpTimeSynced,
                       volatile bool &running, bool &logAtExit)
            : _conn(conn), _host(host), _maxOpTimeSynced(maxOpTimeSynced),
              _running(running), _logAtExit(logAtExit) {}

    void flushInserts() {
        if (!_insertBuf.empty()) {
            _conn.insert(_insertNs, _insertBuf);
            verify(_maxOpTimeSynced < _insertMaxTime);
            _maxOpTimeSynced = _insertMaxTime;
        }
        _insertBuf.clear();
        _insertSize = 0;
        _insertNs = "";
        _insertMaxTime = OpTime();
    }

    const OpTime &maxOpTimeSynced() const { return _maxOpTimeSynced; }
    const OpTime &thisTime() const { return _thisTime; }
    string maxOpTimeSyncedStr() const { return fmtOpTime(_maxOpTimeSynced); }
    string thisTimeStr() const { return fmtOpTime(_thisTime); }

    bool processObj(const BSONObj &obj) {
        if (obj.hasField("$err")) {
            log() << "error getting oplog: " << obj << endl;
            return false;
        }

        static const char *names[] = {"ts", "op", "ns", "o", "b"};
        BSONElement fields[5];
        obj.getFields(5, names, fields);

        BSONElement &tsElt = fields[0];
        if (!tsElt.ok()) {
            log() << "oplog format error: " << obj << " missing 'ts' field." << endl;
            return false;
        }
        if (tsElt.type() != Date && tsElt.type() != Timestamp) {
            log() << "oplog format error: " << obj << " wrong 'ts' field type." << endl;
            return false;
        }
        _thisTime = OpTime(tsElt.date());

        BSONElement &opElt = fields[1];
        if (!opElt.ok()) {
            log() << "oplog format error: " << obj << " missing 'op' field." << endl;
            return false;
        }
        StringData op = opElt.Stringdata();

        // nop
        if (op == "n") {
            if (!_insertBuf.empty()) {
                flushInserts();
            }
            _maxOpTimeSynced = _thisTime;
            _thisTime = OpTime();
            return true;
        }
        // "presence of a database"
        if (op == "db") {
            if (!_insertBuf.empty()) {
                flushInserts();
            }
            _maxOpTimeSynced = _thisTime;
            _thisTime = OpTime();
            return true;
        }
        if (op != "c" && op != "i" && op != "u" && op != "d") {
            log() << "oplog format error: " << obj << " has an invalid 'op' field of '" << op << "'." << endl;
            return false;
        }

        if (op != "i" && !_insertBuf.empty()) {
            flushInserts();
        }

        BSONElement &nsElt = fields[2];
        if (!nsElt.ok()) {
            log() << "oplog format error: " << obj << " missing 'ns' field." << endl;
            return false;
        }
        StringData ns = nsElt.Stringdata();
        size_t i = ns.find('.');
        if (i == string::npos) {
            log() << "oplog format error: invalid namespace '" << ns << "' in op " << obj << "." << endl;
            return false;
        }
        StringData dbname = ns.substr(0, i);
        StringData collname = ns.substr(i + 1);

        BSONElement &oElt = fields[3];
        if (!oElt.ok()) {
            log() << "oplog format error: " << obj << " missing 'o' field." << endl;
            return false;
        }
        BSONObj o = obj["o"].Obj();

        if (op == "c") {
            if (collname != "$cmd") {
                log() << "oplog format error: invalid namespace '" << ns << "' for command in op " << obj << "." << endl;
                return false;
            }
            BSONObj info;
            bool ok = _conn.runCommand(dbname.toString(), o, info);
            if (!ok) {
                StringData fieldName = o.firstElementFieldName();
                BSONElement errmsgElt = info["errmsg"];
                StringData errmsg = errmsgElt.type() == String ? errmsgElt.Stringdata() : "";
                bool isDropIndexes = (fieldName == "dropIndexes" || fieldName == "deleteIndexes");
                if (((fieldName == "drop" || isDropIndexes) && errmsg == "ns not found") ||
                    (isDropIndexes && (errmsg == "index not found" || errmsg.find("can't find index with key:") == 0))) {
                    // This is actually ok.  We don't mind dropping something that's not there.
                    LOG(1) << "Tried to replay " << o << ", got " << info << ", ignoring." << endl;
                }
                else {
                    log() << "replay of command " << o << " failed: " << info << endl;
                    return false;
                }
            }
        }
        else {
            string nsstr = ns.toString();
            if (op == "i") {
                if (collname == "system.indexes") {
                    // Can't ensure multiple indexes in the same batch.
                    flushInserts();

                    // For now, we need to strip out any background fields from
                    // ensureIndex.  Once we do hot indexing we can do something more
                    // like what vanilla applyOperation_inlock does.
                    if (o["background"].trueValue()) {
                        BSONObjBuilder builder;
                        BSONObjIterator it(o);
                        while (it.more()) {
                            BSONElement e = it.next();
                            if (strncmp(e.fieldName(), "background", sizeof("background")) != 0) {
                                builder.append(e);
                            }
                        }
                        o = builder.obj();
                    }
                    // We need to warn very carefully about dropDups.
                    if (o["dropDups"].trueValue()) {
                        BSONObjBuilder builder;
                        BSONObjIterator it(o);
                        while (it.more()) {
                            BSONElement e = it.next();
                            if (strncmp(e.fieldName(), "dropDups", sizeof("dropDups")) != 0) {
                                builder.append(e);
                            }
                        }
                        warning() << "Detected an ensureIndex with dropDups: true in " << o << "." << endl;
                        warning() << "This option is not supported in TokuMX, because it deletes arbitrary data." << endl;
                        warning() << "If it were replayed, it could result in a completely different data set than the source database." << endl;
                        warning() << "We will attempt to replay it without dropDups, but if that fails, you must restart your migration process." << endl;
                        _conn.insert(nsstr, o);
                        string err = _conn.getLastError(dbname.toString(), false, false);
                        if (!err.empty()) {
                            log() << "replay of operation " << obj << " failed: " << err << endl;
                            warning() << "You cannot continue processing this replication stream.  You need to restart the migration process." << endl;
                            _running = false;
                            _logAtExit = false;
                            return true;
                        }
                    }
                }
                pushInsert(nsstr, o);
                // Don't call GLE or update _maxOpTimeSynced yet.
                _thisTime = OpTime();
                return true;
            }
            else if (op == "u") {
                BSONElement o2Elt = obj["o2"];
                if (!o2Elt.ok()) {
                    log() << "oplog format error: " << obj << " missing 'o2' field." << endl;
                    return false;
                }
                BSONElement &bElt = fields[4];
                bool upsert = bElt.booleanSafe();
                BSONObj o2 = o2Elt.Obj();
                _conn.update(nsstr, o2, o, upsert, false);
            }
            else if (op == "d") {
                BSONElement &bElt = fields[4];
                bool justOne = bElt.booleanSafe();
                _conn.remove(nsstr, o, justOne);
            }
            string err = _conn.getLastError(dbname.toString(), false, false);
            if (!err.empty()) {
                log() << "replay of operation " << obj << " failed: " << err << endl;
                return false;
            }
        }

        // If we got here, we completed the operation successfully.
        _maxOpTimeSynced = _thisTime;
        _thisTime = OpTime();
        return true;
    }
};

class OplogTool : public Tool {
    static const char *_tsFilename;
    bool _logAtExit;
    scoped_ptr<VanillaOplogPlayer> _player;
    scoped_ptr<ScopedDbConnection> _rconn;
    string _oplogns;
    string _rpass;
    string _rauthenticationDatabase;
    string _rauthenticationMechanism;
    int _reportingPeriod;
    mutable Timer _reportingTimer;

    class CantFindTimestamp {
        OpTime _firstTime;
      public:
        CantFindTimestamp(OpTime ft) : _firstTime(ft) {}
        OpTime firstTime() const { return _firstTime; }
    };

public:
    void logPosition() const {
        if (_player) {
            if (_player->thisTime() != OpTime()) {
                log() << "Exiting while processing operation with OpTime " << _player->thisTimeStr() << endl;
            }
            report();
            OpTime t = _player->maxOpTimeSynced();
            string tsString = mongoutils::str::stream() << t.getSecs() << ":" << t.getInc();
            log() << "Use --ts=" << tsString << " to resume." << endl;
            try {
                std::ofstream tsFile;
                tsFile.exceptions(std::ifstream::badbit | std::ifstream::failbit);
                tsFile.open(_tsFilename, std::ofstream::trunc);
                tsFile << tsString;
                tsFile.close();
                log() << "Saved timestamp to file "
                      << (boost::filesystem::current_path() / _tsFilename).string() << "." << endl;
                log() << "I'll automatically use this value next time if you run from this directory "
                      << "and don't pass --ts." << endl;
            }
            catch (std::exception &e) {
                warning() << "Error saving timestamp to file " << _tsFilename << ": " << e.what() << endl;
                warning() << "Make sure you save the timestamp somewhere, because I couldn't!" << endl;
            }
        }
    }
    static volatile bool running;

    OplogTool() : Tool("2toku", REMOTE_SERVER | LOCAL_SERVER, "admin"), _logAtExit(true), _player(), _reportingTimer() {
        addFieldOptions();
        add_options()
        ("ts" , po::value<string>() , "max OpTime already applied (secs:inc)" )
        ("from", po::value<string>() , "host to pull from" )
        ("ruser", po::value<string>(), "username on source host if auth required" )
        ("rpass", new PasswordValue( &_rpass ), "password on source host" )
        ("rauthenticationDatabase",
         po::value<string>(&_rauthenticationDatabase)->default_value("admin"),
         "user source on source host (defaults to \"admin\")" )
        ("rauthenticationMechanism",
         po::value<string>(&_rauthenticationMechanism)->default_value("MONGODB-CR"),
         "authentication mechanism on source host")
        ("oplogns", po::value<string>()->default_value( "local.oplog.rs" ) , "ns to pull from" )
        ("reportingPeriod", po::value<int>(&_reportingPeriod)->default_value(10) , "seconds between progress reports" )
        ;
    }

    virtual void printExtraHelp(ostream& out) {
        out << "Pull and replay a remote MongoDB oplog.\n" << endl;
    }

    void report() const {
        const OpTime &maxOpTimeSynced = _player->maxOpTimeSynced();
        LOG(0) << "synced up to " << fmtOpTime(maxOpTimeSynced);
        if (!_rconn) {
            LOG(0) << endl;
            return;
        }
        Query lastQuery;
        lastQuery.sort("$natural", -1);
        BSONObj lastFields = BSON("ts" << 1);
        BSONObj lastObj = _rconn->conn().findOne(_oplogns, lastQuery, &lastFields);
        BSONElement tsElt = lastObj["ts"];
        if (!tsElt.ok()) {
            warning() << "couldn't find last oplog entry on remote host" << endl;
            LOG(0) << endl;
            return;
        }
        OpTime lastOpTime = OpTime(tsElt.date());
        LOG(0) << ", source has up to " << fmtOpTime(lastOpTime);
        if (maxOpTimeSynced == lastOpTime) {
            LOG(0) << ", fully synced." << endl;
        }
        else {
            int diff = lastOpTime.getSecs() - maxOpTimeSynced.getSecs();
            if (diff > 0) {
                LOG(0) << ", " << (lastOpTime.getSecs() - maxOpTimeSynced.getSecs())
                       << " seconds behind source." << endl;
            }
            else {
                LOG(0) << ", less than 1 second behind source." << endl;
            }
        }
        _reportingTimer.reset();
    }

    int run() {
        if (!hasParam("from")) {
            log() << "need to specify --from" << endl;
            return -1;
        }

        _oplogns = getParam("oplogns");

        if (currentClient.get() == 0) {
            Client::initThread( "mongo2toku" );
        }

        LOG(1) << "going to connect" << endl;
        
        _rconn.reset(ScopedDbConnection::getScopedDbConnection(getParam("from")));

        if (hasParam("ruser")) {
            if (!hasParam("rpass")) {
                log() << "if using auth on source, must specify both --ruser and --rpass" << endl;
                return -1;
            }
            try {
                _rconn->conn().auth(BSON("user" << getParam("ruser") <<
                                         "userSource" << _rauthenticationDatabase <<
                                         "pwd" << _rpass <<
                                         "mechanism" << _authenticationMechanism));
            } catch (DBException &e) {
                if (e.getCode() == ErrorCodes::AuthenticationFailed) {
                    error() << "error authenticating to " << _rauthenticationDatabase << " on source: "
                            << e.what() << endl;
                    return -1;
                }
                throw;
            }
        }

        LOG(1) << "connected" << endl;

        {
            string tsString;
            if (hasParam("ts")) {
                tsString = getParam("ts");
            }
            else {
                try {
                    ifstream tsFile;
                    tsFile.exceptions(std::ifstream::badbit | std::ifstream::failbit);
                    tsFile.open(_tsFilename);
                    tsFile >> tsString;
                    tsFile.close();
                } catch (std::exception &e) {
                    warning() << "Couldn't read OpTime from file " << _tsFilename << ": " << e.what() << endl;
                }
            }
            if (tsString.empty()) {
                warning() << "No starting OpTime provided. "
                          << "Please find the right starting point and run again with --ts." << endl;
                return -1;
            }
            unsigned secs, i;
            OpTime maxOpTimeSynced;
            int r = sscanf(tsString.c_str(), "%u:%u", &secs, &i);
            if (r != 2) {
                warning() << "need to specify --ts as <secs>:<inc>" << endl;
                return -1;
            }
            maxOpTimeSynced = OpTime(secs, i);

            _player.reset(new VanillaOplogPlayer(conn(), _host, maxOpTimeSynced, running, _logAtExit));
        }

        try {
            while (running) {
                const int tailingQueryOptions = QueryOption_CursorTailable | QueryOption_OplogReplay | QueryOption_AwaitData;

                bool shouldContinue = false;
                try {
                    try {
                        shouldContinue = attemptQuery(tailingQueryOptions | QueryOption_SlaveOk);
                    } catch (CantFindTimestamp &e) {
                        log() << "Couldn't find OpTime " << _player->maxOpTimeSyncedStr()
                              << " with slaveOk = true (couldn't find anything before " << fmtOpTime(e.firstTime())
                              << "), retrying with slaveOk = false..." << endl;
                        shouldContinue = attemptQuery(tailingQueryOptions);
                    }
                } catch (CantFindTimestamp &e) {
                    warning() << "Tried to start at OpTime " << _player->maxOpTimeSyncedStr()
                              << ", but didn't find anything before " << fmtOpTime(e.firstTime()) << "!" << endl;
                    warning() << "This may mean your oplog has been truncated past the point you are trying to resume from." << endl;
                    warning() << "Either retry with a different value of --ts, or restart your migration procedure." << endl;
                    shouldContinue = false;
                }

                if (!shouldContinue) {
                    _rconn->done();
                    _rconn.reset();
                    return -1;
                }
            }
        }
        catch (DBException &e) {
            warning() << "Caught exception " << e.what() << " while processing.  Exiting..." << endl;
            logPosition();
            _rconn->done();
            _rconn.reset();
            return -1;
        }
        catch (...) {
            warning() << "Caught unknown exception while processing.  Exiting..." << endl;
            logPosition();
            _rconn->done();
            _rconn.reset();
            return -1;
        }

        if (_logAtExit) {
            logPosition();

            _rconn->done();
            _rconn.reset();
            return 0;
        }
        else {
            _rconn->done();
            _rconn.reset();
            return -1;
        }
    }

    bool attemptQuery(int queryOptions) {
        BSONObj res;
        auto_ptr<DBClientCursor> cursor(_rconn->conn().query(
            _oplogns, QUERY("ts" << GTE << _player->maxOpTimeSynced()),
            0, 0, &res, queryOptions));

        if (!cursor->more()) {
            log() << "oplog query returned no results, sleeping 10 seconds..." << endl;
            sleepsecs(10);
            log() << "retrying" << endl;
            return true;
        }

        BSONObj firstObj = cursor->next();
        {
            BSONElement tsElt = firstObj["ts"];
            if (!tsElt.ok()) {
                log() << "oplog format error: " << firstObj << " missing 'ts' field." << endl;
                logPosition();
                return false;
            }
            OpTime firstTime(tsElt.date());
            if (firstTime != _player->maxOpTimeSynced()) {
                throw CantFindTimestamp(firstTime);
            }
        }

        report();

        while (running && cursor->more()) {
            while (running && cursor->moreInCurrentBatch()) {
                BSONObj obj = cursor->next();
                LOG(2) << obj << endl;

                bool ok = _player->processObj(obj);
                if (!ok) {
                    logPosition();
                    return false;
                }
            }
            _player->flushInserts();

            if (_reportingTimer.seconds() >= _reportingPeriod) {
                report();
            }
        }

        return true;
    }
};

const char *OplogTool::_tsFilename = "__mongo2toku_saved_timestamp__";

volatile bool OplogTool::running = false;

namespace proc_mgmt {

    OplogTool *theTool = NULL;

    static void fatal_handler(int sig) {
        signal(sig, SIG_DFL);
        log() << "Received signal " << sig << "." << endl;
        warning() << "Dying immediately on fatal signal." << endl;
        if (theTool != NULL) {
            theTool->logPosition();
        }
        ::abort();
    }
    static void exit_handler(int sig) {
        signal(sig, SIG_DFL);
        log() << "Received signal " << sig << "." << endl;
        log() << "Will exit soon." << endl;
        OplogTool::running = false;
    }

}

int main( int argc , char** argv, char **envp ) {
    mongo::runGlobalInitializersOrDie(argc, argv, envp);
    OplogTool t;
    t.running = true;
    proc_mgmt::theTool = &t;
    signal(SIGILL, proc_mgmt::fatal_handler);
    signal(SIGABRT, proc_mgmt::fatal_handler);
    signal(SIGFPE, proc_mgmt::fatal_handler);
    signal(SIGSEGV, proc_mgmt::fatal_handler);
    signal(SIGHUP, proc_mgmt::exit_handler);
    signal(SIGINT, proc_mgmt::exit_handler);
    signal(SIGQUIT, proc_mgmt::exit_handler);
    signal(SIGPIPE, proc_mgmt::exit_handler);
    signal(SIGALRM, proc_mgmt::exit_handler);
    signal(SIGTERM, proc_mgmt::exit_handler);
    signal(SIGUSR1, SIG_IGN);
    signal(SIGUSR2, SIG_IGN);
    return t.main( argc , argv );
}
