// @file plugin.h

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

#pragma once

#include "mongo/pch.h"

#include <vector>

#include "mongo/db/commands.h"
#include "mongo/db/jsobj.h"
#include "mongo/plugins/plugins.h"

namespace mongo {

    namespace plugins {

        /**
         * An abstract interface for loading/unloading a set of commands and doing nothing else.
         * If you use this, implement commands() and name(), and make sure none of the commands you use have a web UI or an oldName.
         */
        class CommandLoader : public PluginInterface {
          public:
            typedef vector<shared_ptr<Command> > CommandVector;
          private:
            CommandVector _loadedCommands;

            /** Gives us access to the _commands map for removing commands */
            class CommandRemover : private Command {
              public:
                static bool remove(const string &thename) {
                    if (_commands == NULL) {
                        return false;
                    }
                    map<string, Command *>::iterator it = _commands->find(thename);
                    if (it == _commands->end()) {
                        return false;
                    }
                    _commands->erase(it);
                    return true;
                }
            };

          protected:
            /**
             * Return a CommandVector populated with Command objects this plugin provides.
             * Do not instantiate the Command objects until this is called.
             * Do not use the "web" or "oldName" options in these commands.
             */
            virtual CommandVector commands() const = 0;

            /**
             * Anything that should be done before loading.  Returning false stops the plugin from being loaded.
             */
            virtual bool preLoad(string &errmsg, BSONObjBuilder &result) {
                return true;
            }

          public:
            virtual ~CommandLoader() {}

            bool load(string &errmsg, BSONObjBuilder &result) {
                if (!preLoad(errmsg, result)) {
                    return false;
                }
                CommandVector allCommands = commands();
                _loadedCommands.insert(_loadedCommands.end(), allCommands.begin(), allCommands.end());
                return true;
            }

            virtual bool info(string &errmsg, BSONObjBuilder &result) const {
                BSONArrayBuilder b(result.subarrayStart("commands"));
                for (CommandVector::const_iterator it = _loadedCommands.begin(); it != _loadedCommands.end(); ++it) {
                    const shared_ptr<Command> &cmd = *it;
                    b.append(cmd->name);
                }
                b.doneFast();
                return true;
            }

            void unload(string &errmsg) {
                while (!_loadedCommands.empty()) {
                    shared_ptr<Command> cmd = _loadedCommands.back();
                    _loadedCommands.pop_back();
                    bool ok = CommandRemover::remove(cmd->name);
                    if (!ok) {
                        errmsg += "couldn't find command " + cmd->name;
                    }
                }
            }
        };

    } // namespace plugins

} // namespace mongo
