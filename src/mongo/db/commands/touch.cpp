/** @file touch.cpp
    compaction of deleted space in pdfiles (datafiles)
*/

/**
 *    Copyright (C) 2012 10gen Inc.
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

#include "mongo/db/commands.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/curop.h"
#include "mongo/db/index.h"
#include "mongo/db/namespace_details.h"
#include "mongo/util/timer.h"

namespace mongo {

    static int touchIndexCursorCallback(const DBT *key, const DBT *val, void *extra) {
        return TOKUDB_CURSOR_CONTINUE;
    }
    
    static void touchIndex(IndexDetails* idx) {
        DBC* cursor = idx->newCursor();
        DB* db = NULL; // create a dummy db so we get access to negative and positive infinity
        int r = 0;
        r = db_create(&db, storage::env, 0);
        verify(r == 0);
        // prelock to induce prefetching
        r = cursor->c_pre_acquire_range_lock(cursor, db->dbt_neg_infty(), db->dbt_pos_infty());
        while (r != DB_NOTFOUND) {
            killCurrentOp.checkForInterrupt(false); // uasserts if we should stop
            r = cursor->c_getf_next(cursor, 0, touchIndexCursorCallback, NULL);
        }
        db->close(db, 0);
    }

    class TouchCmd : public Command {
    public:
        virtual LockType locktype() const { return NONE; }
        virtual bool adminOnly() const { return false; }
        virtual bool slaveOk() const { return true; }
        virtual bool maintenanceMode() const { return true; }
        virtual bool logTheOp() { return false; }
        virtual void help( stringstream& help ) const {
            help << "touch collection\n"
                "Page in all data for the given collection\n"
                "{ touch : <collection_name>, [data : true] , [index : true] }\n"
                " at least one of data or index must be true; default is both are false\n";
        }
        virtual bool requiresAuth() { return true; }
        TouchCmd() : Command("touch") { }

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
            if ( ! NamespaceString::normal(ns.c_str()) ) {
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

            Client::ReadContext ctx(ns);
            ctx.ctx().beginTransaction(DB_READ_UNCOMMITTED | DB_TXN_READ_ONLY);
            NamespaceDetails *nsd = nsdetails(ns.c_str());
            if (!nsd) {
                errmsg = "ns not found";
                return false;
            }
            int id = nsd->findIdIndex();

            if (touch_data) {
                log() << " touching namespace " << ns << endl;
                IndexDetails& idx = nsd->idx(id);
                touchIndex(&idx);                
            }

            if (touch_indexes) {
                for (int i = 0; i < nsd->nIndexes(); i++) {
                    if (i != id) {
                        touchIndex(&nsd->idx(i));
                    }
                }
            }

            ctx.ctx().commitTransaction();
            return true;
        }
        
    };
    static TouchCmd touchCmd;
}
