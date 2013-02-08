//@file insert.cpp

/**
 *    Copyright (C) 2012 Tokutek Inc.
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

#include "pch.h"

#include "mongo/db/client.h"
#include "mongo/db/database.h"
#include "mongo/db/ops/insert.h"
#include "mongo/util/log.h"

namespace mongo {

    void insertObject(const char *ns, const BSONObj &obj) {
        tokulog() << "TODO: insert into " << ns << " object " << obj.toString() << endl;
        {
            Client::WriteContext ctx(ns);
            Database *db = ctx.ctx().db();
            tokulog() << "Got database " << db << " named " << db->name << " at path " << db->path << endl;
            NamespaceDetails *details = nsdetails_maybe_create(ns);
            tokulog() << "Got the deets " << details << endl;
        }
    }
    
} // namespace mongo
