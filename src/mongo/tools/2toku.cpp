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

#include <boost/filesystem.hpp>

#include "mongo/base/error_codes.h"
#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/client/connpool.h"
#include "mongo/db/jsobj.h"
#include "mongo/tools/mongo2toku_options.h"
#include "mongo/tools/tool.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/password.h"
#include "mongo/util/timer.h"

using namespace mongo;

static string fmtOpTime(const OpTime &t) {
    stringstream ss;
    ss << t.getSecs() << ":" << t.getInc() << " (" << t.toStringPretty() << ")";
    return ss.str();
}

class VanillaOplogPlayer : boost::noncopyable {
    mongo::DBClientBase &_conn;
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
    VanillaOplogPlayer(mongo::DBClientBase &conn, const OpTime &maxOpTimeSynced,
                       volatile bool &running, bool &logAtExit)
            : _conn(conn), _maxOpTimeSynced(maxOpTimeSynced),
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
            toolError() << "error getting oplog: " << obj << std::endl;
            return false;
        }

        static const char *names[] = {"ts", "op", "ns", "o", "b"};
        BSONElement fields[5];
        obj.getFields(5, names, fields);

        BSONElement &tsElt = fields[0];
        if (!tsElt.ok()) {
            toolError() << "oplog format error: " << obj << " missing 'ts' field." << std::endl;
            return false;
        }
        if (tsElt.type() != Date && tsElt.type() != Timestamp) {
            toolError() << "oplog format error: " << obj << " wrong 'ts' field type." << std::endl;
            return false;
        }
        _thisTime = OpTime(tsElt.date());

        BSONElement &opElt = fields[1];
        if (!opElt.ok()) {
            toolError() << "oplog format error: " << obj << " missing 'op' field." << std::endl;
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
            toolError() << "oplog format error: " << obj << " has an invalid 'op' field of '" << op << "'." << std::endl;
            return false;
        }

        if (op != "i" && !_insertBuf.empty()) {
            flushInserts();
        }

        BSONElement &nsElt = fields[2];
        if (!nsElt.ok()) {
            toolError() << "oplog format error: " << obj << " missing 'ns' field." << std::endl;
            return false;
        }
        StringData ns = nsElt.Stringdata();
        size_t i = ns.find('.');
        if (i == string::npos) {
            toolError() << "oplog format error: invalid namespace '" << ns << "' in op " << obj << "." << std::endl;
            return false;
        }
        StringData dbname = ns.substr(0, i);
        StringData collname = ns.substr(i + 1);

        BSONElement &oElt = fields[3];
        if (!oElt.ok()) {
            toolError() << "oplog format error: " << obj << " missing 'o' field." << std::endl;
            return false;
        }
        BSONObj o = obj["o"].Obj();

        if (op == "c") {
            if (collname != "$cmd") {
                toolError() << "oplog format error: invalid namespace '" << ns << "' for command in op " << obj << "." << std::endl;
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
                    if (logger::globalLogDomain()->shouldLog(logger::LogSeverity::Debug(1))) {
                        toolInfoLog() << "Tried to replay " << o << ", got " << info << ", ignoring." << std::endl;
                    }
                }
                else {
                    toolError() << "replay of command " << o << " failed: " << info << std::endl;
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
                        toolError() << "Detected an ensureIndex with dropDups: true in " << o << "." << std::endl;
                        toolError() << "This option is not supported in TokuMX, because it deletes arbitrary data." << std::endl;
                        toolError() << "If it were replayed, it could result in a completely different data set than the source database." << std::endl;
                        toolError() << "We will attempt to replay it without dropDups, but if that fails, you must restart your migration process." << std::endl;
                        _conn.insert(nsstr, o);
                        string err = _conn.getLastError(dbname.toString(), false, false);
                        if (!err.empty()) {
                            toolError() << "replay of operation " << obj << " failed: " << err << std::endl;
                            toolError() << "You cannot continue processing this replication stream.  You need to restart the migration process." << std::endl;
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
                    toolError() << "oplog format error: " << obj << " missing 'o2' field." << std::endl;
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
                toolError() << "replay of operation " << obj << " failed: " << err << std::endl;
                return false;
            }
        }

        // If we got here, we completed the operation successfully.
        _maxOpTimeSynced = _thisTime;
        _thisTime = OpTime();
        return true;
    }
};

class OplogTool;
namespace proc_mgmt {
    extern OplogTool *theTool;
}

class OplogTool : public Tool {
    static const char *_tsFilename;
    bool _logAtExit;
    scoped_ptr<VanillaOplogPlayer> _player;
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
                toolError() << "Exiting while processing operation with OpTime " << _player->thisTimeStr() << std::endl;
            }
            report();
            OpTime t = _player->maxOpTimeSynced();
            string tsString = mongoutils::str::stream() << t.getSecs() << ":" << t.getInc();
            toolInfoLog() << "Use --ts=" << tsString << " to resume." << std::endl;
            try {
                std::ofstream tsFile;
                tsFile.exceptions(std::ifstream::badbit | std::ifstream::failbit);
                tsFile.open(_tsFilename, std::ofstream::trunc);
                tsFile << tsString;
                tsFile.close();
                toolInfoLog() << "Saved timestamp to file "
                              << (boost::filesystem::current_path() / _tsFilename).string() << "." << std::endl;
                toolInfoLog() << "I'll automatically use this value next time if you run from this directory "
                              << "and don't pass --ts." << std::endl;
            }
            catch (std::exception &e) {
                toolError() << "Error saving timestamp to file " << _tsFilename << ": " << e.what() << std::endl;
                toolError() << "Make sure you save the timestamp somewhere, because I couldn't!" << std::endl;
            }
        }
    }
    static volatile bool running;

    OplogTool() : Tool(), _logAtExit(true), _player(), _reportingTimer() {}

    virtual void printHelp(ostream& out) {
        printMongo2TokuHelp(&out);
    }

    void report() const {
        const OpTime &maxOpTimeSynced = _player->maxOpTimeSynced();
        LogstreamBuilder l = toolInfoLog();
        l << "synced up to " << fmtOpTime(maxOpTimeSynced);
        Query lastQuery;
        lastQuery.sort("$natural", -1);
        BSONObj lastFields = BSON("ts" << 1);
        ScopedDbConnection conn(mongo2TokuGlobalParams.from);
        if (!doAuth(conn)) {
            l << std::endl;
            conn.done();
            return;
        }
        BSONObj lastObj = conn->findOne(mongo2TokuGlobalParams.oplogns, lastQuery, &lastFields);
        conn.done();
        BSONElement tsElt = lastObj["ts"];
        if (!tsElt.ok()) {
            l << std::endl;
            toolError() << "couldn't find last oplog entry on remote host" << std::endl;
            return;
        }
        OpTime lastOpTime = OpTime(tsElt.date());
        l << ", source has up to " << fmtOpTime(lastOpTime);
        if (maxOpTimeSynced == lastOpTime) {
            l << ", fully synced." << std::endl;
        }
        else {
            int diff = lastOpTime.getSecs() - maxOpTimeSynced.getSecs();
            if (diff > 0) {
                l << ", " << (lastOpTime.getSecs() - maxOpTimeSynced.getSecs())
                  << " seconds behind source." << std::endl;
            }
            else {
                l << ", less than 1 second behind source." << std::endl;
            }
        }
        _reportingTimer.reset();
    }

    bool doAuth(ScopedDbConnection &conn) const {
        if (!mongo2TokuGlobalParams.ruser.empty()) {
            try {
                conn->auth(BSON("user" << mongo2TokuGlobalParams.ruser <<
                                "userSource" << mongo2TokuGlobalParams.rauthenticationDatabase <<
                                "pwd" << mongo2TokuGlobalParams.rpass <<
                                "mechanism" << mongo2TokuGlobalParams.rauthenticationMechanism));
            } catch (DBException &e) {
                if (e.getCode() == ErrorCodes::AuthenticationFailed) {
                    toolError() << "error authenticating to " << mongo2TokuGlobalParams.rauthenticationDatabase << " on source: "
                                << e.what() << std::endl;
                    return false;
                }
                throw;
            }
        }
        return true;
    }

    int run() {
        proc_mgmt::theTool = this;
        running = true;

        if (currentClient.get() == 0) {
            Client::initThread( "mongo2toku" );
        }

        toolInfoLog() << "going to connect" << std::endl;

        ScopedDbConnection conn(mongo2TokuGlobalParams.from);

        if (!doAuth(conn)) {
            conn.done();
            return -1;
        }

        toolInfoLog() << "connected" << std::endl;

        {
            string tsString = mongo2TokuGlobalParams.ts;
            if (tsString.empty()) {
                try {
                    ifstream tsFile;
                    tsFile.exceptions(std::ifstream::badbit | std::ifstream::failbit);
                    tsFile.open(_tsFilename);
                    tsFile >> tsString;
                    tsFile.close();
                } catch (std::exception &e) {
                    toolError() << "Couldn't read OpTime from file " << _tsFilename << ": " << e.what() << std::endl;
                }
            }
            if (tsString.empty()) {
                toolError() << "No starting OpTime provided. "
                            << "Please find the right starting point and run again with --ts." << std::endl;
                return -1;
            }
            unsigned secs, i;
            OpTime maxOpTimeSynced;
            int r = sscanf(tsString.c_str(), "%u:%u", &secs, &i);
            if (r != 2) {
                toolError() << "need to specify --ts as <secs>:<inc>" << std::endl;
                return -1;
            }
            maxOpTimeSynced = OpTime(secs, i);

            _player.reset(new VanillaOplogPlayer(conn.conn(), maxOpTimeSynced, running, _logAtExit));
        }

        try {
            while (running) {
                const int tailingQueryOptions = QueryOption_CursorTailable | QueryOption_OplogReplay | QueryOption_AwaitData;

                bool shouldContinue = false;
                try {
                    try {
                        shouldContinue = attemptQuery(conn, tailingQueryOptions | QueryOption_SlaveOk);
                    } catch (CantFindTimestamp &e) {
                        toolInfoLog() << "Couldn't find OpTime " << _player->maxOpTimeSyncedStr()
                                      << " with slaveOk = true (couldn't find anything before " << fmtOpTime(e.firstTime())
                                      << "), retrying with slaveOk = false..." << std::endl;
                        shouldContinue = attemptQuery(conn, tailingQueryOptions);
                    }
                } catch (CantFindTimestamp &e) {
                    toolError() << "Tried to start at OpTime " << _player->maxOpTimeSyncedStr()
                                << ", but didn't find anything before " << fmtOpTime(e.firstTime()) << "!" << std::endl;
                    toolError() << "This may mean your oplog has been truncated past the point you are trying to resume from." << std::endl;
                    toolError() << "Either retry with a different value of --ts, or restart your migration procedure." << std::endl;
                    shouldContinue = false;
                }

                if (!shouldContinue) {
                    conn.done();
                    return -1;
                }
            }
        }
        catch (DBException &e) {
            toolError() << "Caught exception " << e.what() << " while processing.  Exiting..." << std::endl;
            logPosition();
            conn.done();
            return -1;
        }
        catch (...) {
            toolError() << "Caught unknown exception while processing.  Exiting..." << std::endl;
            logPosition();
            conn.done();
            return -1;
        }

        if (_logAtExit) {
            logPosition();

            conn.done();
            return 0;
        }
        else {
            conn.done();
            return -1;
        }
    }

    bool attemptQuery(ScopedDbConnection &conn, int queryOptions) {
        BSONObj res;
        auto_ptr<DBClientCursor> cursor(conn->query(
            mongo2TokuGlobalParams.oplogns, QUERY("ts" << GTE << _player->maxOpTimeSynced()),
            0, 0, &res, queryOptions));

        if (!cursor->more()) {
            toolInfoLog() << "oplog query returned no results, sleeping 10 seconds..." << std::endl;
            sleepsecs(10);
            toolInfoLog() << "retrying" << std::endl;
            return true;
        }

        BSONObj firstObj = cursor->next();
        {
            BSONElement tsElt = firstObj["ts"];
            if (!tsElt.ok()) {
                toolInfoLog() << "oplog format error: " << firstObj << " missing 'ts' field." << std::endl;
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
                if (logger::globalLogDomain()->shouldLog(logger::LogSeverity::Debug(2))) {
                    toolInfoLog() << obj << std::endl;
                }

                bool ok = _player->processObj(obj);
                if (!ok) {
                    logPosition();
                    return false;
                }
            }
            _player->flushInserts();

            if (_reportingTimer.seconds() >= mongo2TokuGlobalParams.reportingPeriod) {
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
        toolInfoLog() << "Received signal " << sig << "." << std::endl;
        toolError() << "Dying immediately on fatal signal." << std::endl;
        if (theTool != NULL) {
            theTool->logPosition();
        }
        ::abort();
    }
    static void exit_handler(int sig) {
        signal(sig, SIG_DFL);
        toolInfoLog() << "Received signal " << sig << "." << std::endl;
        toolInfoLog() << "Will exit soon." << std::endl;
        OplogTool::running = false;
    }

}

MONGO_INITIALIZER(setupMongo2TokuSignalHandler)(
        ::mongo::InitializerContext* context) {
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

    return Status::OK();
}

REGISTER_MONGO_TOOL(OplogTool);
