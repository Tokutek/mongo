/* hashcmd.cpp
 *
 * Defines a shell command for hashing a BSONElement value
 */


/**
*    Copyright (C) 2012 10gen Inc.
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

#include "mongo/base/init.h"
#include "mongo/db/commands.h"
#include "mongo/db/repl/rs.h"

namespace mongo {

    class CmdTestHooks : public InformationCommand {
    public:
        CmdTestHooks() : InformationCommand("_testHooks") {};
        virtual void help( stringstream& help ) const {
            help << "internal command used for testing";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {}

        bool run(
            const string& db,
            BSONObj& cmdObj,
            int options, string& errmsg,
            BSONObjBuilder& result,
            bool fromRepl = false )
        {
            if (cmdObj.hasField("keepOplogAlive")) {
                uint64_t val = cmdObj["keepOplogAlive"].numberLong();
                if (theReplSet) {
                    theReplSet->setKeepOplogAlivePeriod(val);
                }
            }

            return true;
        }
    };

    MONGO_INITIALIZER(RegisterTestHooksCmd)(InitializerContext* context) {
        if (Command::testCommandsEnabled) {
            // Leaked intentionally: a Command registers itself when constructed.
            new CmdTestHooks();
        }
        return Status::OK();
    }

}
