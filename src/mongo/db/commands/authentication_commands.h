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

#pragma once

#include "mongo/db/commands.h"

namespace mongo {

    class CmdAuthenticate : public InformationCommand {
    public:
        static void disableCommand();

        virtual void help(stringstream& ss) const { ss << "internal"; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {} // No auth required
        CmdAuthenticate() : InformationCommand("authenticate") {}
        bool run(const string& dbname , BSONObj& cmdObj, int options, string& errmsg, BSONObjBuilder& result, bool fromRepl);
    };

    extern CmdAuthenticate cmdAuthenticate;
}


