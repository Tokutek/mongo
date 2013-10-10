/** @file touch.cpp
    compaction of deleted space in pdfiles (datafiles)
*/

/**
 *    Copyright (C) 2012 10gen Inc.
 *    Copyright (C) 2013 Tokutek Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,b
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "pch.h"

#include <string>
#include <vector>

#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/commands.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/curop.h"
#include "mongo/db/index.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/jsobj.h"
#include "mongo/util/timer.h"

namespace mongo {

    class TouchCmd : public QueryCommand {
    public:
        virtual bool adminOnly() const { return false; }
        virtual void help( stringstream& help ) const {
            help << "touch collection\n"
                "Page in all data for the given collection\n"
                "{ touch : <collection_name>, [data : true] , [index : true] }\n"
                " at least one of data or index must be true; default is both are false\n";
        }
        virtual bool requiresAuth() { return true; }
        TouchCmd() : QueryCommand("touch") {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::touch);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }

        virtual bool run(const string& db, 
                         BSONObj& cmdObj, 
                         int, 
                         string& errmsg, 
                         BSONObjBuilder& result, 
                         bool fromRepl) {
            string coll = cmdObj.firstElement().valuestr();
            if( coll.empty() || db.empty() ) {
                errmsg = "no collection name specified";
                return false;
            }
                        
            string ns = db + '.' + coll;
            if ( ! NamespaceString::normal(ns) ) {
                errmsg = "bad namespace name";
                return false;
            }

            bool touch_indexes( cmdObj["index"].trueValue() );
            bool touch_data( cmdObj["data"].trueValue() );

            if ( ! (touch_indexes || touch_data) ) {
                errmsg = "must specify at least one of (data:true, index:true)";
                return false;
            }
            bool ok = touch( ns, errmsg, touch_data, touch_indexes, result );
            return ok;
        }

        bool touch( std::string& ns, 
                    std::string& errmsg, 
                    bool touch_data, 
                    bool touch_indexes, 
                    BSONObjBuilder& result ) {

            NamespaceDetails *nsd = nsdetails(ns);
            if (!nsd) {
                errmsg = "ns not found";
                return false;
            }

            for (int i = 0; i < nsd->nIndexes(); i++) {
                IndexDetails &idx = nsd->idx(i);
                if ((nsd->isPKIndex(idx) && touch_data) || (!nsd->isPKIndex(idx) && touch_indexes)) {
                    for (shared_ptr<Cursor> c(IndexCursor::make(nsd, idx, minKey, maxKey, true, 1)); c->ok(); c->advance()) {
                        c->current();
                    }
                }
            }

            return true;
        }
    } touchCmd;

}
