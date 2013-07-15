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
#include <signal.h>
#include <string.h>

#include <boost/program_options.hpp>

#include "mongo/tools/tool.h"

#include "mongo/client/connpool.h"
#include "mongo/db/jsobj.h"

using namespace mongo;

namespace po = boost::program_options;

static string fmtOpTime(const OpTime &t) {
    stringstream ss;
    ss << t.getSecs() << ":" << t.getInc();
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

    void pushInsert(const string &ns, const BSONObj &o) {
        uassert(16862, "cannot append an earlier optime", _thisTime > _insertMaxTime);
        // semes like enough room for headers/metadata
        static const size_t MAX_SIZE = BSONObjMaxUserSize - (4<<10);
        if (ns != _insertNs || _insertSize + o.objsize() > MAX_SIZE) {
            flushInserts();
            _insertNs = ns;
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
        string op = opElt.String();

        // nop
        if (op == "n") {
            return true;
        }
        // "presence of a database"
        if (op == "db") {
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
        string ns = nsElt.String();
        size_t i = ns.find('.');
        if (i == string::npos) {
            log() << "oplog format error: invalid namespace '" << ns << "' in op " << obj << "." << endl;
            return false;
        }
        string dbname = ns.substr(0, i);
        string collname = ns.substr(i + 1);

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
            bool ok = _conn.runCommand(dbname, o, info);
            if (!ok) {
                const char *fieldName = o.firstElementFieldName();
                string errmsg = info["errmsg"].str();
                bool isDropIndexes = (strncmp(fieldName, "dropIndexes", sizeof("dropIndexes")) == 0 ||
                                      strncmp(fieldName, "deleteIndexes", sizeof("deleteIndexes")) == 0);
                if (((strncmp(fieldName, "drop", sizeof("drop")) == 0 || isDropIndexes) &&
                     errmsg == "ns not found") ||
                    (isDropIndexes && (errmsg == "index not found" ||
                                       errmsg.find("can't find index with key:") == 0))) {
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
            if (op == "i") {
                if (collname == "system.indexes") {
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
                        flushInserts();
                        warning() << "Detected an ensureIndex with dropDups: true in " << o << "." << endl;
                        warning() << "This option is not supported in TokuMX, because it deletes arbitrary data." << endl;
                        warning() << "If it were replayed, it could result in a completely different data set than the source database." << endl;
                        warning() << "We will attempt to replay it without dropDups, but if that fails, you must restart your migration process." << endl;
                        _conn.insert(ns, o);
                        string err = _conn.getLastError(dbname, false, false);
                        if (!err.empty()) {
                            log() << "replay of operation " << obj << " failed: " << err << endl;
                            warning() << "You cannot continue processing this replication stream.  You need to restart the migration process." << endl;
                            _running = false;
                            _logAtExit = false;
                            return true;
                        }
                    }
                }
                pushInsert(ns, o);
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
                _conn.update(ns, o2, o, upsert, false);
            }
            else if (op == "d") {
                BSONElement &bElt = fields[4];
                bool justOne = bElt.booleanSafe();
                _conn.remove(ns, o, justOne);
            }
            string err = _conn.getLastError(dbname, false, false);
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
    bool _logAtExit;
    scoped_ptr<VanillaOplogPlayer> _player;

public:
    void logPosition() const {
        if (_player) {
            if (_player->thisTime() != OpTime()) {
                log() << "Exiting while processing operation with OpTime " << _player->thisTimeStr() << endl;
            }
            log() << "Synced up to OpTime " << _player->maxOpTimeSyncedStr() << endl
                  << "Use --ts=" << _player->maxOpTimeSyncedStr() << " to resume." << endl;
        }
    }
    static volatile bool running;

    OplogTool() : Tool("2toku"), _logAtExit(true) {
        addFieldOptions();
        add_options()
        ("ts" , po::value<string>() , "max OpTime already applied (secs:inc)" )
        ("from", po::value<string>() , "host to pull from" )
        ("oplogns", po::value<string>()->default_value( "local.oplog.rs" ) , "ns to pull from" )
        ;
    }

    virtual void printExtraHelp(ostream& out) {
        out << "Pull and replay a remote MongoDB oplog.\n" << endl;
    }

    int run() {
        if (!hasParam("from")) {
            log() << "need to specify --from" << endl;
            return -1;
        }

        const string &oplogns = getParam("oplogns");

        Client::initThread( "mongo2toku" );

        LOG(1) << "going to connect" << endl;
        
        scoped_ptr<ScopedDbConnection> rconn(ScopedDbConnection::getScopedDbConnection(getParam("from")));

        LOG(1) << "connected" << endl;

        {
            OpTime maxOpTimeSynced;
            if (hasParam("ts")) {
                unsigned secs, i;
                const string &ts(getParam("ts"));
                int r = sscanf(ts.c_str(), "%u:%u", &secs, &i);
                if (r != 2) {
                    log() << "need to specify --ts as <secs>:<inc>" << endl;
                    return -1;
                }
                maxOpTimeSynced = OpTime(secs, i);
            }

            _player.reset(new VanillaOplogPlayer(conn(), _host, maxOpTimeSynced, running, _logAtExit));
        }

        try {
            while (running) {
                const int tailingQueryOptions = QueryOption_SlaveOk | QueryOption_CursorTailable | QueryOption_OplogReplay | QueryOption_AwaitData;

                BSONObjBuilder queryBuilder;
                BSONObjBuilder gteBuilder(queryBuilder.subobjStart("ts"));
                gteBuilder.appendTimestamp("$gte", _player->maxOpTimeSynced().asDate());
                gteBuilder.doneFast();
                BSONObj query = queryBuilder.done();

                BSONObj res;
                auto_ptr<DBClientCursor> cursor(rconn->conn().query(oplogns, query, 0, 0, &res, tailingQueryOptions));

                if (!cursor->more()) {
                    log() << "oplog query returned no results, sleeping";
                    for (int i = 0; running && i < 10; ++i) {
                        log() << '.';
                        Logstream::get().flush();
                        sleepsecs(1);
                    }
                    log() << "retrying" << endl;
                    continue;
                }

                BSONObj firstObj = cursor->next();
                {
                    BSONElement tsElt = firstObj["ts"];
                    if (!tsElt.ok()) {
                        log() << "oplog format error: " << firstObj << " missing 'ts' field." << endl;
                        rconn->done();
                        logPosition();
                        return -1;
                    }
                    OpTime firstTime(tsElt.date());
                    if (firstTime != _player->maxOpTimeSynced()) {
                        warning() << "Tried to start at OpTime " << _player->maxOpTimeSyncedStr()
                                  << ", but didn't find anything before " << fmtOpTime(firstTime) << "!" << endl;
                        warning() << "This may mean your oplog has been truncated past the point you are trying to resume from." << endl;
                        warning() << "Either retry with a different value of --ts, or restart your migration procedure." << endl;
                        rconn->done();
                        return -1;
                    }
                }

                while (running && cursor->more()) {
                    while (running && cursor->moreInCurrentBatch()) {
                        BSONObj obj = cursor->next();
                        LOG(2) << obj << endl;

                        bool ok = _player->processObj(obj);
                        if (!ok) {
                            rconn->done();
                            logPosition();
                            return -1;
                        }
                    }
                    _player->flushInserts();
                }
            }
        }
        catch (DBException &e) {
            warning() << "Caught exception " << e.what() << " while processing.  Exiting..." << endl;
            rconn->done();
            logPosition();
            return -1;
        }
        catch (...) {
            warning() << "Caught unknown exception while processing.  Exiting..." << endl;
            rconn->done();
            logPosition();
            return -1;
        }

        rconn->done();

        if (_logAtExit) {
            logPosition();
            return 0;
        }
        else {
            return -1;
        }
    }
};

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

int main( int argc , char** argv ) {
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
