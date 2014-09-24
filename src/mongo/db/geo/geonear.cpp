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

#include <vector>

#include "mongo/db/geo/geonear.h"

#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/collection.h"
#include "mongo/db/commands.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/index/s2.h"
#include "mongo/db/geo/2d.h"
#include "mongo/db/geo/s2common.h"
#include "mongo/db/geo/s2nearcursor.h"

namespace mongo {
    GeoNearArguments::GeoNearArguments(const BSONObj &cmdObj) {
        const char* limitName = cmdObj["num"].isNumber() ? "num" : "limit";
        if (cmdObj[limitName].isNumber()) {
            numWanted = cmdObj[limitName].numberInt();
        } else {
            numWanted = 100;
        }

        if (!cmdObj["uniqueDocs"].eoo()) {
            uniqueDocs = cmdObj["uniqueDocs"].trueValue();
        } else {
            uniqueDocs = false;
        }

        if (!cmdObj["includeLocs"].eoo()) {
            includeLocs = cmdObj["includeLocs"].trueValue();
        } else {
            includeLocs = false;
        }

        if (cmdObj["query"].isABSONObj()) {
            query = cmdObj["query"].embeddedObject();
        }

        if (cmdObj["distanceMultiplier"].isNumber()) {
            distanceMultiplier = cmdObj["distanceMultiplier"].number();
        } else {
            distanceMultiplier = 1.0;
        }

        isSpherical = cmdObj["spherical"].trueValue();
    }

    class Geo2dFindNearCmd : public QueryCommand {
    public:
        Geo2dFindNearCmd() : QueryCommand("geoNear") {}

        virtual LockType locktype() const { return READ; }
        bool slaveOk() const { return true; }
        bool slaveOverrideOk() const { return true; }

        void help(stringstream& h) const {
            h << "http://dochub.mongodb.org/core/geo#GeospatialIndexing-geoNearCommand";
        }

        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseNs(dbname, cmdObj), actions));
        }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            string ns = dbname + "." + cmdObj.firstElement().valuestr();
            Collection *cl = getCollection(ns);

            if (NULL == cl) {
                errmsg = "can't find ns";
                return false;
            }

            GeoNearArguments commonArgs(cmdObj);
            if (commonArgs.numWanted < 0) {
                errmsg = "numWanted must be >= 0";
                return false;
            }

            vector<int> idxs;

            cl->findIndexByType("2d", idxs);
            if (idxs.size() > 1) {
                errmsg = "more than one 2d index, not sure which to run geoNear on";
                return false;
            }

            if (1 == idxs.size()) {
                result.append("ns", ns);
                return run2DGeoNear(cl->idx(idxs[0]), cmdObj, commonArgs, errmsg, result);
            }

            cl->findIndexByType("2dsphere", idxs);
            if (idxs.size() > 1) {
                errmsg = "more than one 2dsphere index, not sure which to run geoNear on";
                return false;
            }

            if (1 == idxs.size()) {
                result.append("ns", ns);
                return run2DSphereGeoNear(cl->idx(idxs[0]), cmdObj, commonArgs, errmsg, result);
            }

            errmsg = "no geo indices for geoNear";
            return false;
        }
    private:
    } geo2dFindNearCmd;
}  // namespace mongo
