/* commands.cpp
   db "commands" (sent via db.$cmd.findOne(...))
 */

/**
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

#include <time.h>

#include "mongo/pch.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/introspect.h"
#include "mongo/db/cursor.h"
#include "mongo/db/json.h"
#include "mongo/db/repl.h"
#include "mongo/db/commands.h"
#include "mongo/db/cmdline.h"
#include "mongo/scripting/engine.h"
#include "mongo/util/lruishmap.h"

namespace mongo {

    const int edebug=0;

    bool dbEval(const string& dbName, BSONObj& cmd, BSONObjBuilder& result, string& errmsg) {
        BSONElement e = cmd.firstElement();
        uassert( 10046 ,  "eval needs Code" , e.type() == Code || e.type() == CodeWScope || e.type() == String );

        const char *code = 0;
        switch ( e.type() ) {
        case String:
        case Code:
            code = e.valuestr();
            break;
        case CodeWScope:
            code = e.codeWScopeCode();
            break;
        default:
            verify(0);
        }
        verify( code );

        if ( ! globalScriptEngine ) {
            errmsg = "db side execution is disabled";
            return false;
        }

        auto_ptr<Scope> s = globalScriptEngine->getPooledScope( dbName );
        ScriptingFunction f = s->createFunction(code);
        if ( f == 0 ) {
            errmsg = (string)"compile failed: " + s->getError();
            return false;
        }

        if ( e.type() == CodeWScope )
            s->init( e.codeWScopeScopeDataUnsafe() );
        s->localConnect( dbName.c_str() );

        BSONObj args;
        {
            BSONElement argsElement = cmd.getField("args");
            if ( argsElement.type() == Array ) {
                args = argsElement.embeddedObject();
                if ( edebug ) {
                    out() << "args:" << args.toString() << endl;
                    out() << "code:\n" << code << endl;
                }
            }
        }

        int res;
        {
            Timer t;
            res = s->invoke(f, &args, 0, cmdLine.quota ? 10 * 60 * 1000 : 0 );
            int m = t.millis();
            if ( m > cmdLine.slowMS ) {
                out() << "dbeval slow, time: " << dec << m << "ms " << dbName << endl;
                if ( m >= 1000 ) log() << code << endl;
                else OCCASIONALLY log() << code << endl;
            }
        }
        if ( res ) {
            result.append("errno", (double) res);
            errmsg = "invoke failed: ";
            errmsg += s->getError();
            return false;
        }

        s->append( result , "retval" , "return" );

        return true;
    }

    // SERVER-4328 todo review for concurrency
    class CmdEval : public Command {
    public:
        virtual bool slaveOk() const {
            return false;
        }
        virtual void help( stringstream &help ) const {
            help << "Evaluate javascript at the server.\n" "http://dochub.mongodb.org/core/serversidecodeexecution";
        }
        virtual LockType locktype() const { return NONE; }
        CmdEval() : Command("eval", false, "$eval") { }
        virtual bool needsTxn() const { return false; }
        bool run(const string& dbname , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {

            AuthenticationInfo *ai = cc().getAuthenticationInfo();
            uassert( 12598 , "$eval reads unauthorized", ai->isAuthorizedReads(dbname.c_str()) );

            if ( cmdObj["nolock"].trueValue() ) {
                return dbEval(dbname, cmdObj, result, errmsg);
            }

            // write security will be enforced in DBDirectClient
            // TODO: should this be a db lock?
            scoped_ptr<Lock::ScopedLock> lk( ai->isAuthorized( dbname.c_str() ) ? 
                                             static_cast<Lock::ScopedLock*>( new Lock::GlobalWrite() ) : 
                                             static_cast<Lock::ScopedLock*>( new Lock::GlobalRead() ) );
            // If we create a context here, and the js we're running does fileops, we'll create too many txns.
            //Client::Context ctx( dbname );

            return dbEval(dbname, cmdObj, result, errmsg);
        }
    } cmdeval;

} // namespace mongo
