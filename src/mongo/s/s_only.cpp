// s_only.cpp

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

#include "mongo/pch.h"
#include "mongo/s/request.h"
#include "mongo/s/client_info.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/matcher.h"
#include "mongo/db/commands.h"
#include "mongo/db/namespacestring.h"

/*
  most a pile of hacks to make linking nicer

 */
namespace mongo {

    void* remapPrivateView(void *oldPrivateAddr) {
        log() << "remapPrivateView called in mongos, aborting" << endl;
        fassertFailed(16462);
    }

    /** When this callback is run, we record a shard that we've used for useful work
     *  in an operation to be read later by getLastError()
    */
    void usingAShardConnection( const string& addr ) {
        ClientInfo::get()->addShard( addr );
    }

    TSP_DEFINE(Client,currentClient)

    LockState::LockState(){} // ugh

    Client::Client(const char *desc , AbstractMessagingPort *p) :
        ClientBasic(p),
        _context(0),
        _shutdown(false),
        _desc(desc),
        _god(0),
        _lastGTID() {
    }
    Client::~Client() {}
    bool Client::shutdown() { return true; }

    Client& Client::initThread(const char *desc, AbstractMessagingPort *mp) {
        // mp is non-null only for client connections, and mongos uses ClientInfo for those
        massert(16478, "Mongos Client being used for incoming connection thread", mp == NULL);
        setThreadName(desc);
        verify( currentClient.get() == 0 );
        Client *c = new Client(desc, mp);
        currentClient.reset(c);
        mongo::lastError.initThread();
        return *c;
    }

    string Client::clientAddress(bool includePort) const {
        ClientInfo * ci = ClientInfo::get();
        if ( ci )
            return ci->getRemote();
        return "";
    }

    bool execCommand( Command * c ,
                      Client& client , int queryOptions ,
                      const char *ns, BSONObj& cmdObj ,
                      BSONObjBuilder& result,
                      bool fromRepl ) {
        verify(c);

        string dbname = nsToDatabase( ns );

        if ( cmdObj["help"].trueValue() ) {
            stringstream ss;
            ss << "help for: " << c->name << " ";
            c->help( ss );
            result.append( "help" , ss.str() );
            result.append( "lockType" , c->locktype() );
            return true;
        }

        if ( c->adminOnly() ) {
            if ( dbname != "admin" ) {
                result.append( "errmsg" ,  "access denied- use admin db" );
                log() << "command denied: " << cmdObj.toString() << endl;
                return false;
            }
            LOG( 2 ) << "command: " << cmdObj << endl;
        }

        if (!client.getAuthenticationInfo()->isAuthorized(dbname)) {
            result.append("errmsg" , "unauthorized");
            result.append("note" , "from execCommand" );
            return false;
        }

        string errmsg;
        int ok = c->run( dbname , cmdObj , queryOptions, errmsg , result , fromRepl );
        if ( ! ok )
            result.append( "errmsg" , errmsg );
        return ok;
    }
}
