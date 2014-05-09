// fsync.cpp

/**
*    Copyright (C) 2012 10gen Inc.
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
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#include "mongo/pch.h"

#include "mongo/db/commands/fsync.h"

#include <string>
#include <vector>

#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/commands.h"
#include "mongo/db/client.h"
#include "mongo/db/jsobj.h"
#include "mongo/util/background.h"

namespace mongo {
    
    class FSyncCommand : public Command {
    public:
        FSyncCommand() : Command("fsync") {}
        virtual LockType locktype() const { return NONE; }
        virtual bool slaveOk() const { return true; }
        virtual bool requiresShardedOperationScope() const { return false; }
        virtual bool requiresSync() const { return false; }
        virtual bool needsTxn() const { return false; }
        virtual int txnFlags() const { return noTxnFlags(); }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual OpSettings getOpSettings() const { return OpSettings(); }
        virtual bool adminOnly() const { return true; }
        virtual void help(stringstream& h) const { h << "http://dochub.mongodb.org/core/fsynccommand"; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::fsync);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
        virtual bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            const bool lock = cmdObj["lock"].trueValue();
            if (lock) {
                const char *msg = "fsync with lock:true is not suitable for locked snapshotting in TokuMX";
                warning() << msg << endl;
                result.append("info", msg);
            }
            return true;
        }
    } fsyncCmd;

} // namespace mongo
