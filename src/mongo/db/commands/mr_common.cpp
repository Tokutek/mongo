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

#include "mongo/db/commands/mr.h"

#include <string>
#include <vector>

#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/commands.h"
#include "mongo/db/jsobj.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    namespace mr {
        Config::OutputOptions Config::parseOutputOptions(const std::string& dbname,
                                                         const BSONObj& cmdObj) {
            Config::OutputOptions outputOptions;

            outputOptions.outNonAtomic = false;
            if (cmdObj["out"].type() == String) {
                outputOptions.collectionName = cmdObj["out"].String();
                outputOptions.outType = REPLACE;
            }
            else if (cmdObj["out"].type() == Object) {
                BSONObj o = cmdObj["out"].embeddedObject();

                BSONElement e = o.firstElement();
                string t = e.fieldName();

                if (t == "normal" || t == "replace") {
                    outputOptions.outType = REPLACE;
                    outputOptions.collectionName = e.String();
                }
                else if (t == "merge") {
                    outputOptions.outType = MERGE;
                    outputOptions.collectionName = e.String();
                }
                else if (t == "reduce") {
                    outputOptions.outType = REDUCE;
                    outputOptions.collectionName = e.String();
                }
                else if (t == "inline") {
                    outputOptions.outType = INMEMORY;
                }
                else {
                    uasserted(13522,
                              mongoutils::str::stream() << "unknown out specifier [" << t << "]");
                }

                if (o.hasElement("db")) {
                    outputOptions.outDB = o["db"].String();
                }
            }
            else {
                uasserted(13606 , "'out' has to be a string or an object");
            }

            if (outputOptions.outType != INMEMORY) {
                outputOptions.finalNamespace = mongoutils::str::stream()
                    << (outputOptions.outDB.empty() ? dbname : outputOptions.outDB)
                    << "." << outputOptions.collectionName;
            }

            return outputOptions;
        }

        void addPrivilegesRequiredForMapReduce(Command* commandTemplate,
                                               const std::string& dbname,
                                               const BSONObj& cmdObj,
                                               std::vector<Privilege>* out) {
            Config::OutputOptions outputOptions = Config::parseOutputOptions(dbname, cmdObj);

            ResourcePattern inputResource(commandTemplate->parseResourcePattern(dbname, cmdObj));
            uassert(17142, mongoutils::str::stream() <<
                    "Invalid input resource " << inputResource.toString(),
                    inputResource.isExactNamespacePattern());
            out->push_back(Privilege(inputResource, ActionType::find));

            if (outputOptions.outType != Config::INMEMORY) {
                ActionSet outputActions;
                outputActions.addAction(ActionType::insert);
                if (outputOptions.outType == Config::REPLACE) {
                    outputActions.addAction(ActionType::remove);
                }
                else {
                    outputActions.addAction(ActionType::update);
                }

                ResourcePattern outputResource(
                        ResourcePattern::forExactNamespace(
                                NamespaceString(outputOptions.finalNamespace)));
                uassert(17143, mongoutils::str::stream() << "Invalid target namespace " <<
                        outputResource.ns().ns(),
                        outputResource.ns().isValid());

                // TODO: check if outputNs exists and add createCollection privilege if not
                out->push_back(Privilege(outputResource, outputActions));
            }
        }
    }

}
