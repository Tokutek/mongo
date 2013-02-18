/** @file compact.cpp
   compaction of deleted space in pdfiles (datafiles)
*/

/**
*    Copyright (C) 2010 10gen Inc.
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

#include "mongo/db/background.h"
#include "mongo/db/commands.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/curop.h"
#include "mongo/db/index.h"
#include "mongo/db/namespace_details.h"
#include "mongo/util/concurrency/task.h"
#include "mongo/util/timer.h"

namespace mongo {

    // run hot optimize on each index
    bool _compact(const char *ns, NamespaceDetails *d, string& errmsg, BSONObjBuilder& result) { 
        Client::RootTransaction txn;

        // TODO: Track progress with this class
        //ProgressMeterHolder pm( cc().curop()->setMessage( string("compacting ns ") + string(ns) , nIndexes ) );

        const int nIndexes = d->nIndexes();
        for( int i = 0; i < nIndexes; i++ ) {
            killCurrentOp.checkForInterrupt(false); // uasserts if we should stop
            BSONObj info = d->idx(i).info();
            log() << "compact optimizing index " << info["key"].Obj().toString() << endl;

            // TODO: Hot optimize
            problem() << "compaction is not implemented, doing nothign!" << endl;

        }

        txn.commit();
        return true;
    }

    bool compact(const string& ns, string &errmsg, BSONObjBuilder& result) {
        massert( 14028, "bad ns", NamespaceString::normal(ns.c_str()) );
        // defensive, there's no need fo optimize a system namespace, probably.
        massert( 14027, "can't compact a system namespace", !str::contains(ns, ".system.") );

        bool ok;
        {
            Lock::DBWrite lk(ns);

            //BackgroundOperation::assertNoBgOpInProgForNs(ns.c_str());
            Client::Context ctx(ns);
            NamespaceDetails *d = nsdetails(ns.c_str());
            massert( 13660, str::stream() << "namespace " << ns << " does not exist", d );
            massert( 13661, "cannot compact capped collection", !d->isCapped() );
            log() << "compact " << ns << " begin" << endl;
            try { 
                ok = _compact(ns.c_str(), d, errmsg, result);
            }
            catch(...) { 
                log() << "compact " << ns << " end (with error)" << endl;
                throw;
            }
            log() << "compact " << ns << " end" << endl;
        }
        return ok;
    }

    bool isCurrentlyAReplSetPrimary();

    class CompactCmd : public Command {
    public:
        virtual LockType locktype() const { return NONE; }
        virtual bool adminOnly() const { return false; }
        virtual bool slaveOk() const { return true; }
        virtual bool maintenanceMode() const { return true; }
        virtual bool logTheOp() { return false; }
        virtual void help( stringstream& help ) const {
            help << "compact collection\n"
                "note: this operation performs a non-blocking rebuild of all indexes, using moderate CPU and some I/O. you can cancel with cancelOp()\n"
                "{ compact : <collection_name>, [force:<bool>], [validate:<bool>],\n"
                "  [paddingFactor:<num>], [paddingBytes:<num>] }\n"
                "  paddingFactor/paddingBytes - deprecated\n"
                "  force - allows to run on a replica set primary\n"
                "  validate - deprecated\n";
        }
        virtual bool requiresAuth() { return true; }
        CompactCmd() : Command("compact") { }

        virtual bool run(const string& db, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            string coll = cmdObj.firstElement().valuestr();
            if( coll.empty() || db.empty() ) {
                errmsg = "no collection name specified";
                return false;
            }

            if( isCurrentlyAReplSetPrimary() && !cmdObj["force"].trueValue() ) { 
                errmsg = "will not run compact on an active replica set primary as this is a slow blocking operation. use force:true to force";
                return false;
            }
            
            string ns = db + '.' + coll;
            if ( ! NamespaceString::normal(ns.c_str()) ) {
                errmsg = "bad namespace name";
                return false;
            }
            
            // parameter validation to avoid triggering assertions in compact()
            if ( str::contains(ns, ".system.") ) {
                errmsg = "can't compact a system namespace";
                return false;
            }
            
            {
                Lock::DBWrite lk(ns);
                Client::Context ctx(ns);
                NamespaceDetails *d = nsdetails(ns.c_str());
                if( ! d ) {
                    errmsg = "namespace does not exist";
                    return false;
                }

                if ( d->isCapped() ) {
                    errmsg = "cannot compact a capped collection";
                    return false;
                }
            }

            bool ok = compact(ns, errmsg, result);
            return ok;
        }
    };
    static CompactCmd compactCmd;

}
