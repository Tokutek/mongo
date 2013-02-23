// dbcommands_admin.cpp

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

/**
   this file has dbcommands that are for dba type administration
   mostly around dbs and collections
   NOT system stuff
*/

#include "mongo/pch.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/commands.h"
#include "mongo/db/cmdline.h"
#include "mongo/db/cursor.h"
#include "mongo/db/curop.h"
#include "mongo/scripting/engine.h"
#include "mongo/util/background.h"
#include "mongo/util/alignedbuilder.h"
#include "mongo/util/paths.h"
#include "mongo/util/timer.h"

#include <boost/filesystem/convenience.hpp>
#include <boost/filesystem/operations.hpp>

namespace mongo {

    class CleanCmd : public Command {
    public:
        CleanCmd() : Command( "clean" ) {}

        virtual bool slaveOk() const { return true; }
        virtual LockType locktype() const { return WRITE; }

        virtual void help(stringstream& h) const { h << "internal"; }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            string dropns = dbname + "." + cmdObj.firstElement().valuestrsafe();

            if ( !cmdLine.quiet )
                tlog() << "CMD: clean " << dropns << endl;

            NamespaceDetails *d = nsdetails(dropns.c_str());

            if ( ! d ) {
                errmsg = "ns not found";
                return 0;
            }

            problem() << "\"clean\" is a deprecated command, doing nothing!" << endl;

            result.append("ns", dropns.c_str());
            return 1;
        }

    } cleanCmd;

    namespace dur {
        boost::filesystem::path getJournalDir();
    }
 
    class JournalLatencyTestCmd : public Command {
    public:
        JournalLatencyTestCmd() : Command( "journalLatencyTest" ) {}

        virtual bool slaveOk() const { return true; }
        virtual LockType locktype() const { return NONE; }
        virtual bool adminOnly() const { return true; }
        virtual void help(stringstream& h) const { h << "test how long to write and fsync to a test file in the journal/ directory"; }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            problem() << "journalLatencyTest is a deprecated command, doing nothing!" << endl;

            result.append("timeMillis", 0);
            result.append("timeMillisWithPrealloc", 0);

            return 1;
        }
    } journalLatencyTestCmd;

    class ValidateCmd : public Command {
    public:
        ValidateCmd() : Command( "validate" ) {}

        virtual bool slaveOk() const {
            return true;
        }

        virtual void help(stringstream& h) const { h << "Validate contents of a namespace by scanning its data structures for correctness.  Slow.\n"
                                                        "Add full:true option to do a more thorough check"; }

        virtual LockType locktype() const { return READ; }
        //{ validate: "collectionnamewithoutthedbpart" [, scandata: <bool>] [, full: <bool> } */

        bool run(const string& dbname , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl ) {
            string ns = dbname + "." + cmdObj.firstElement().valuestrsafe();
            NamespaceDetails * d = nsdetails( ns.c_str() );
            if ( !cmdLine.quiet )
                tlog() << "CMD: validate " << ns << endl;

            if ( ! d ) {
                errmsg = "ns not found";
                return false;
            }

            problem() << "validate is a deprecated command, assuming everything is ok." << endl;

            result.append( "ns", ns );
            result.appendBool("valid", true);
            return true;
        }
    } validateCmd;

}
