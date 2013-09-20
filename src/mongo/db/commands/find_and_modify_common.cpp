// find_and_modify.cpp

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
*/

#include "mongo/db/commands/find_and_modify.h"

#include <string>
#include <vector>

#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/auth/resource_pattern.h"
#include "mongo/db/commands.h"
#include "mongo/db/jsobj.h"

namespace mongo {
namespace find_and_modify {

        void addPrivilegesRequiredForFindAndModify(Command* commandTemplate,
                                                   const std::string& dbname,
                                                   const BSONObj& cmdObj,
                                                   std::vector<Privilege>* out) {
            bool update = cmdObj["update"].trueValue();
            bool upsert = cmdObj["upsert"].trueValue();
            bool remove = cmdObj["remove"].trueValue();

            ActionSet actions;
            actions.addAction(ActionType::find);
            if (update) {
                actions.addAction(ActionType::update);
            }
            if (upsert) {
                actions.addAction(ActionType::insert);
            }
            if (remove) {
                actions.addAction(ActionType::remove);
            }
            ResourcePattern resource(commandTemplate->parseResourcePattern(dbname, cmdObj));
            uassert(17137, "Invalid target namespace " + resource.toString(),
                    resource.isExactNamespacePattern());
            out->push_back(Privilege(resource, actions));
        }

} // namespace find_and_modify
} // namespace mongo
