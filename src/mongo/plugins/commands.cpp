// @file commands.cpp

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

#include "mongo/base/string_data.h"
#include "mongo/db/commands.h"
#include "mongo/db/jsobj.h"
#include "mongo/plugins/loader.h"

namespace mongo {

    /**
     * Load a plugin into mongod.
     */
    class LoadPluginCommand : public InformationCommand {
      public:
        // Be strict for now, relax later if we need to.
        virtual LockType locktype() const { return WRITE; }
        virtual bool lockGlobally() const { return true; }
        virtual bool requiresAuth() { return true; }
        virtual bool adminOnly() const { return true; }
        virtual void help(stringstream &h) const {
            h << "Load a plugin into mongod." << endl
              << "{ loadPlugin : <name> [, filename: <filename>] }" << endl;
        }
        LoadPluginCommand() : InformationCommand("loadPlugin", false) {}

        virtual bool run(const string &db, BSONObj &cmdObj, int options, string &errmsg, BSONObjBuilder &result, bool fromRepl) {
            if (db != "admin") {
                errmsg = "loadPlugin must be run on the admin db";
                return false;
            }
            BSONElement filenameElt = cmdObj["filename"];
            if (filenameElt.ok()) {
                if (filenameElt.type() != String) {
                    errmsg = "filename argument must be a string";
                    return false;
                }
                StringData filename = filenameElt.Stringdata();
                if (filename.empty()) {
                    errmsg = "filename argument must not be empty";
                    return false;
                }
                return plugins::loader.load(filename, errmsg, result);
            }
            else {
                BSONElement e = cmdObj.firstElement();
                if (e.type() != String) {
                    errmsg = "loadPlugin argument must be a string";
                    return false;
                }
                string name = e.str();
                if (name.empty()) {
                    errmsg = "loadPlugin argument must not be empty";
                    return false;
                }
                string filename = mongoutils::str::stream() << "lib" << name << ".so";
                return plugins::loader.load(filename, errmsg, result);
            }
        }
    } loadPluginCommand;

    /**
     * List plugins currently loaded into mongod.
     */
    class ListPluginsCommand : public InformationCommand {
      public:
        // Be strict for now, relax later if we need to.
        virtual LockType locktype() const { return READ; }
        virtual bool lockGlobally() const { return true; }
        virtual bool requiresAuth() { return true; }
        virtual bool adminOnly() const { return true; }
        virtual void help(stringstream &h) const {
            h << "List plugins currently loaded into mongod." << endl
              << "{ listPlugins : 1 }" << endl;
        }
        ListPluginsCommand() : InformationCommand("listPlugins", false) {}

        virtual bool run(const string &db, BSONObj &cmdObj, int options, string &errmsg, BSONObjBuilder &result, bool fromRepl) {
            return plugins::loader.list(errmsg, result);
        }
    } listPluginsCommand;

    /**
     * Unload a plugin from mongod.
     */
    class UnloadPluginCommand : public InformationCommand {
      public:
        // Be strict for now, relax later if we need to.
        virtual LockType locktype() const { return WRITE; }
        virtual bool lockGlobally() const { return true; }
        virtual bool requiresAuth() { return true; }
        virtual bool adminOnly() const { return true; }
        virtual void help(stringstream &h) const {
            h << "Unload a plugin from mongod." << endl
              << "{ unloadPlugin : <name> }" << endl;
        }
        UnloadPluginCommand() : InformationCommand("unloadPlugin", false) {}

        virtual bool run(const string &db, BSONObj &cmdObj, int options, string &errmsg, BSONObjBuilder &result, bool fromRepl) {
            if (db != "admin") {
                errmsg = "unloadPlugin must be run on the admin db";
                return false;
            }
            BSONElement e = cmdObj.firstElement();
            if (e.type() != String) {
                errmsg = "unloadPlugin argument must be a string";
                return false;
            }
            StringData name = e.Stringdata();
            if (name.empty()) {
                errmsg = "unloadPlugin argument must not be empty";
                return false;
            }
            return plugins::loader.unload(name, errmsg, result);
        }
    } unloadPluginCommand;

}  // namespace mongo
