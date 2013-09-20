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
#include "mongo/db/commands.h"
#include "mongo/db/client.h"
#include "mongo/db/repl/rs.h"

namespace mongo {

    class LoaderCommand : public InformationCommand {
    public:
        LoaderCommand(const char *name, bool webUI=false, const char *oldName=NULL) :
            InformationCommand(name, webUI, oldName) {
        }
        virtual bool adminOnly() const { return false; }
        virtual LockType locktype() const { return WRITE; }
        virtual bool needsTxn() const { return false; }
        virtual bool logTheOp() { return true; }
        virtual bool slaveOk() const { return false; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::loaderCommands);
            out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
        }
    };

    class BeginLoadCmd : public LoaderCommand {
    public:
        BeginLoadCmd() : LoaderCommand("beginLoad") {}

        virtual void help( stringstream& help ) const {
            help << "begin load" << endl << 
                "Begin a bulk load into a collection." << endl <<
                "Must be inside an existing multi-statement transaction." << endl <<
                "{ beginLoad: 1, ns : collName, indexes: [ { ... }, ... ], options: { ... }  }" << endl;
        }

        virtual bool run(const string& db, 
                         BSONObj& cmdObj, 
                         int options, 
                         string& errmsg, 
                         BSONObjBuilder& result, 
                         bool fromRepl) 
        {
            uassert( 16892, "Must be in a multi-statement transaction to begin a load.",
                            cc().hasTxn());
            uassert( 16882, "The ns field must be a string.",
                            cmdObj["ns"].type() == mongo::String );
            uassert( 16883, "The indexes field must be an array of index objects.",
                            cmdObj["indexes"].type() == mongo::Array );
            uassert( 16884, "The options field must be an object.",
                            cmdObj["options"].type() == mongo::Object );
            LOG(0) << "Beginning bulk load, cmd: " << cmdObj << endl;

            const string ns = db + "." + cmdObj["ns"].String();
            const BSONObj &optionsObj = cmdObj["options"].Obj();
            vector<BSONElement> indexElements = cmdObj["indexes"].Array();
            vector<BSONObj> indexes;
            for (vector<BSONElement>::const_iterator i = indexElements.begin(); i != indexElements.end(); i++) {
                uassert( 16885, "Each index spec must be an object describing the index to be built",
                                i->type() == mongo::Object );

                BSONObj obj = i->Obj();
                indexes.push_back(obj.copy());
            }

            cc().beginClientLoad(ns, indexes, optionsObj);
            result.append("status", "load began");
            result.append("ok", true);
            return true;
        }
    } beginLoadCmd;

    class CommitLoadCmd : public LoaderCommand {
    public:
        CommitLoadCmd() : LoaderCommand("commitLoad") {}

        virtual void help( stringstream& help ) const {
            help << "commit load" << endl <<
                "Commits a load in progress." << endl <<
                "{ commitLoad }" << endl;
        }

        virtual bool run(const string& db, 
                         BSONObj& cmdObj, 
                         int options, 
                         string& errmsg, 
                         BSONObjBuilder& result, 
                         bool fromRepl) 
        {
            cc().commitClientLoad();
            result.append("status", "load committed");
            result.append("ok", true);
            return true;
        }
    } commitLoadCmd;

    class AbortLoadCmd : public LoaderCommand {
    public:
        AbortLoadCmd() : LoaderCommand("abortLoad") {}

        virtual void help( stringstream& help ) const {
            help << "abort load" << endl <<
                "Aborts a load in progress." << endl <<
                "{ abortLoad }" << endl;
        }

        virtual bool run(const string& db, 
                         BSONObj& cmdObj, 
                         int options, 
                         string& errmsg, 
                         BSONObjBuilder& result, 
                         bool fromRepl) 
        {
            cc().abortClientLoad();
            result.append("status", "load aborted");
            result.append("ok", true);
            return true;
        }
    } abortLoadCmd;

} // namespace mongo

