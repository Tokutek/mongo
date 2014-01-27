/* commands.cpp
   db "commands" (sent via db.$cmd.findOne(...))
 */

/*    Copyright 2009 10gen Inc.
 *    Copyright (C) 2013 Tokutek Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#include "pch.h"

#include <db.h>

#include "mongo/db/commands.h"

#include <string>
#include <vector>

#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/client.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/ops/query.h"
#include "mongo/db/replutil.h"
#include "mongo/db/server_parameters.h"

namespace mongo {

    map<string,Command*> * Command::_commandsByBestName;
    map<string,Command*> * Command::_webCommands;
    map<string,Command*> * Command::_commands;

    int Command::testCommandsEnabled = 0;

    namespace {
        ExportedServerParameter<int> testCommandsParameter(ServerParameterSet::getGlobal(),
                                                           "enableTestCommands",
                                                           &Command::testCommandsEnabled,
                                                           true,
                                                           false);
    }

    string Command::parseNsFullyQualified(const string& dbname, const BSONObj& cmdObj) const { 
        string s = cmdObj.firstElement().valuestr();
        StringData dbstr = nsToDatabaseSubstring(s);
        // these are for security, do not remove:
        massert(15962, "need to specify namespace" , !dbstr.empty() );
        massert(15966, str::stream() << "dbname not ok in Command::parseNsFullyQualified: " << dbname , dbname == dbstr || dbname == "admin" );
        return s;
    }

    /*virtual*/ string Command::parseNs(const string& dbname, const BSONObj& cmdObj) const {
        string coll = cmdObj.firstElement().valuestr();
#if defined(CLC)
        DEV if( mongoutils::str::startsWith(coll, dbname+'.') ) { 
            log() << "DEBUG parseNs Command's collection name looks like it includes the db name\n"
                << dbname << '\n' 
                << coll << '\n'
                << cmdObj.toString() << endl;
            dassert(false);
        }
#endif
        return dbname + '.' + coll;
    }

    void Command::htmlHelp(stringstream& ss) const {
        string helpStr;
        {
            stringstream h;
            help(h);
            helpStr = h.str();
        }
        ss << "\n<tr><td>";
        bool web = _webCommands->count(name) != 0;
        if( web ) ss << "<a href=\"/" << name << "?text=1\">";
        ss << name;
        if( web ) ss << "</a>";
        ss << "</td>\n";
        ss << "<td>";
        int l = locktype();
        //if( l == NONE ) ss << "N ";
        if( l == READ ) ss << "R ";
        else if( l == WRITE ) ss << "W ";
        if( slaveOk() )
            ss << "S ";
        if( adminOnly() )
            ss << "A";
        if( lockGlobally() ) 
            ss << " lockGlobally ";
        ss << "</td>";
        ss << "<td>";
        if( helpStr != "no help defined" ) {
            const char *p = helpStr.c_str();
            while( *p ) {
                if( *p == '<' ) {
                    ss << "&lt;";
                    p++; continue;
                }
                else if( *p == '{' )
                    ss << "<code>";
                else if( *p == '}' ) {
                    ss << "}</code>";
                    p++;
                    continue;
                }
                if( strncmp(p, "http:", 5) == 0 ) {
                    ss << "<a href=\"";
                    const char *q = p;
                    while( *q && *q != ' ' && *q != '\n' )
                        ss << *q++;
                    ss << "\">";
                    q = p;
                    if( startsWith(q, "http://www.mongodb.org/display/") )
                        q += 31;
                    while( *q && *q != ' ' && *q != '\n' ) {
                        ss << (*q == '+' ? ' ' : *q);
                        q++;
                        if( *q == '#' )
                            while( *q && *q != ' ' && *q != '\n' ) q++;
                    }
                    ss << "</a>";
                    p = q;
                    continue;
                }
                if( *p == '\n' ) ss << "<br>";
                else ss << *p;
                p++;
            }
        }
        ss << "</td>";
        ss << "</tr>\n";
    }

    Command::Command(const char *_name, bool web, const char *oldName) : name(_name) {
        // register ourself.
        if ( _commands == 0 )
            _commands = new map<string,Command*>;
        if( _commandsByBestName == 0 )
            _commandsByBestName = new map<string,Command*>;
        Command*& c = (*_commands)[name];
        if ( c )
            log() << "warning: 2 commands with name: " << _name << endl;
        c = this;
        (*_commandsByBestName)[name] = this;

        if( web ) {
            if( _webCommands == 0 )
                _webCommands = new map<string,Command*>;
            (*_webCommands)[name] = this;
        }

        if( oldName )
            (*_commands)[oldName] = this;
    }

    void Command::help( stringstream& help ) const {
        help << "no help defined";
    }

    Command* Command::findCommand( const string& name ) {
        map<string,Command*>::iterator i = _commands->find( name );
        if ( i == _commands->end() )
            return 0;
        return i->second;
    }

    Command::LockType Command::locktype( const string& name ) {
        Command * c = findCommand( name );
        if ( ! c )
            return WRITE;
        return c->locktype();
    }

    void Command::appendCommandStatus(BSONObjBuilder& result, bool ok, const std::string& errmsg) {
        BSONObj tmp = result.asTempObj();
        bool have_ok = tmp.hasField("ok");
        bool have_errmsg = tmp.hasField("errmsg");

        if (!have_ok)
            result.append( "ok" , ok ? 1.0 : 0.0 );

        if (!ok && !have_errmsg) {
            result.append("errmsg", errmsg);
        }
    }

    void Command::logIfSlow( const Timer& timer, const string& msg ) {
        int ms = timer.millis();
        if ( ms > cmdLine.slowMS ) {
            out() << msg << " took " << ms << " ms." << endl;
        }
    }

    bool isCursorCommand(BSONObj cmdObj) {
        BSONElement cursorElem = cmdObj["cursor"];
        if (cursorElem.eoo())
            return false;

        uassert(16954, "cursor field must be missing or an object",
                cursorElem.type() == Object);

        BSONObj cursor = cursorElem.embeddedObject();
        BSONElement batchSizeElem = cursor["batchSize"];
        if (batchSizeElem.eoo()) {
            uassert(16955, "cursor object can't contain fields other than batchSize",
                cursor.isEmpty());
        }
        else {
            uassert(16956, "cursor.batchSize must be a number",
                    batchSizeElem.isNumber());

            // This can change in the future, but for now all negatives are reserved.
            uassert(16957, "Cursor batchSize must not be negative",
                    batchSizeElem.numberLong() >= 0);
        }

        return true;
    }

    void handleCursorCommand(CursorId id, BSONObj& cmdObj, BSONObjBuilder& result) {
        BSONElement batchSizeElem = cmdObj.getFieldDotted("cursor.batchSize");
        const long long batchSize = batchSizeElem.isNumber()
                                    ? batchSizeElem.numberLong()
                                    : 101; // same as query

        // Using limited cursor API that ignores many edge cases. Should be sufficient for commands.
        ClientCursor::Pin pin(id);
        ClientCursor* cursor = pin.c();

        massert(16958, "Cursor shouldn't have been deleted",
                cursor);

        // Make sure this cursor won't disappear on us
        fassert(16959, !cursor->c()->shouldDestroyOnNSDeletion());

        try {
            const string cursorNs = cursor->ns(); // we need this after cursor may have been deleted

            // can't use result BSONObjBuilder directly since it won't handle exceptions correctly.
            BSONArrayBuilder resultsArray;
            const int byteLimit = MaxBytesToReturnToClientAtOnce;
            for (int objs = 0;
                    objs < batchSize && cursor->ok() && resultsArray.len() <= byteLimit;
                    objs++) {
                // TODO may need special logic if cursor->current() would cause results to be > 16MB
                resultsArray.append(cursor->current());
                cursor->advance();
            }

            // The initial ok() on a cursor may be very expensive so we don't do it when batchSize
            // is 0 since that indicates a desire for a fast return.
            if (batchSize != 0 && !cursor->ok()) {
                // There is no more data. Kill the cursor.
                pin.release();
                ClientCursor::erase(id);
                id = 0;
                cursor = NULL; // make it an obvious error to use cursor after this point
            }

            BSONObjBuilder cursorObj(result.subobjStart("cursor"));
            cursorObj.append("id", id);
            cursorObj.append("ns", cursorNs);
            cursorObj.append("firstBatch", resultsArray.arr());
            cursorObj.done();
        }
        catch (...) {
            // Clean up cursor on way out of scope.
            pin.release();
            ClientCursor::erase(id);
            throw;
        }
    }

}

#include "../client/connpool.h"

namespace mongo {

    extern DBConnectionPool pool;

    class PoolFlushCmd : public InformationCommand {
    public:
        PoolFlushCmd() : InformationCommand( "connPoolSync" , false , "connpoolsync" ) {}
        virtual void help( stringstream &help ) const { help<<"internal"; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::connPoolSync);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        virtual bool run(const string&, mongo::BSONObj&, int, std::string&, mongo::BSONObjBuilder& result, bool) {
            pool.flush();
            return true;
        }
    } poolFlushCmd;

    class PoolStats : public InformationCommand {
    public:
        PoolStats() : InformationCommand( "connPoolStats" ) {}
        virtual void help( stringstream &help ) const { help<<"stats about connection pool"; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::connPoolStats);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        virtual bool run(const string&, mongo::BSONObj&, int, std::string&, mongo::BSONObjBuilder& result, bool) {
            pool.appendInfo( result );
            result.append( "numDBClientConnection" , DBClientConnection::getNumConnections() );
            result.append( "numAScopedConnection" , AScopedConnection::getNumConnections() );
            return true;
        }
    } poolStatsCmd;

} // namespace mongo
