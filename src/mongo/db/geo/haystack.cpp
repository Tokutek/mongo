// db/geo/haystack.cpp

/**
 *    Copyright (C) 2008-2012 10gen Inc.
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

#include <vector>

#include "mongo/db/collection.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/index/haystack.h"
#include "mongo/db/commands.h"

/**
 * Provides the geoHaystack index type and the command "geoSearch."
 * Examines all documents in a given radius of a given point.
 * Returns all documents that match a given search restriction.
 * See http://dochub.mongodb.org/core/haystackindexes
 *
 * Use when you want to look for restaurants within 25 miles with a certain name.
 * Don't use when you want to find the closest open restaurants; see 2d.cpp for that.
 */

namespace mongo {

    static const string GEOSEARCHNAME = "geoHaystack";

    class GeoHaystackSearchCommand : public QueryCommand {
    public:
        GeoHaystackSearchCommand() : QueryCommand("geoSearch") {}

        virtual LockType locktype() const { return READ; }
        bool slaveOk() const { return true; }
        bool slaveOverrideOk() const { return true; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseNs(dbname, cmdObj), actions));
        }
        bool run(const string& dbname, BSONObj& cmdObj, int,
                 string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            string ns = dbname + "." + cmdObj.firstElement().valuestr();

            Collection *cl = getCollection(ns);
            if (NULL == cl) {
                errmsg = "can't find ns";
                return false;
            }

            vector<int> idxs;
            cl->findIndexByType(GEOSEARCHNAME, idxs);
            if (idxs.size() == 0) {
                errmsg = "no geoSearch index";
                return false;
            }
            if (idxs.size() > 1) {
                errmsg = "more than 1 geosearch index";
                return false;
            }

            int idxNum = idxs[0];

            IndexDetails& id = cl->idx(idxNum);
            HaystackIndex *hsIdx =
                dynamic_cast<HaystackIndex *>(const_cast<IndexDetails *>(&id));

            BSONElement nearElt = cmdObj["near"];
            BSONElement maxDistance = cmdObj["maxDistance"];
            BSONElement search = cmdObj["search"];

            uassert(13318, "near needs to be an array", nearElt.isABSONObj());
            uassert(13319, "maxDistance needs a number", maxDistance.isNumber());
            uassert(13320, "search needs to be an object", search.type() == Object);

            unsigned limit = 50;
            if (cmdObj["limit"].isNumber())
                limit = static_cast<unsigned>(cmdObj["limit"].numberInt());

            hsIdx->searchCommand(cl, nearElt.Obj(), maxDistance.numberDouble(), search.Obj(),
                                 result, limit);
            return 1;
        }

    } nameSearchCommand;
}
