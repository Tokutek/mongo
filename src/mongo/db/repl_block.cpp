// repl_block.cpp

/**
*    Copyright (C) 2008 10gen Inc.
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

#include "pch.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "repl.h"
#include "repl_block.h"
#include "instance.h"
#include "../util/background.h"
#include "../util/mongoutils/str.h"
#include "replutil.h"

//#define REPLDEBUG(x) log() << "replBlock: "  << x << endl;
#define REPLDEBUG(x)

namespace mongo {

    // this is defined in fsync.cpp
    // need to figure out where to put for real
    bool lockedForWriting();

    using namespace mongoutils;

    OP_REPL_STATUS gtidReplicated(GTID myGTID, GTID remoteGTID) {
        //
        //
        // bleh, don't like this clause
        //
        //
        if (remoteGTID == GTID_MAX) {
            return REPL_SUCCESS;
        }
        // if our primary is less than the remote's primary, that means
        // a failover happened and we cannot be sure of whether the write
        // truly replicated
        else if (myGTID.getPrimary() < remoteGTID.getPrimary()) {
            return REPL_FAIL;
        }
        // otherwise, if myGTID < remoteGTID, then we know
        // the primary values of the GTIDs are the same, and the write
        // replicated
        else if (GTID::cmp(myGTID, remoteGTID) <= 0) {
            return REPL_SUCCESS;
        }
        // otherwise, we are waiting
        return REPL_WAITING;
    }

    class SlaveTracking { // SERVER-4328 todo review
    public:
        string name() const { return "SlaveTracking"; }
        struct Ident {

            Ident(const BSONObj& r, const BSONObj& config, const string& n) {
                BSONObjBuilder b;
                b.appendElements( r );
                b.append( "config" , config );
                b.append( "ns" , n );
                obj = b.obj();
            }

            bool operator<( const Ident& other ) const {
                return obj["_id"].OID() < other.obj["_id"].OID();
            }

            BSONObj obj;
        };

        SlaveTracking() : _mutex("SlaveTracking") {
        }

        void run() {
            Client::initThread( "slaveTracking" );
            DBDirectClient db;
            while ( ! inShutdown() ) {
                sleepsecs( 1 );
                if ( inShutdown() )
                    return;                
            }
        }

        void reset() {
            scoped_lock mylk(_mutex);
            _slaves.clear();
        }

        void update( const BSONObj& rid , const BSONObj config , const string& ns , GTID gtid ) {
            REPLDEBUG( config << " " << rid << " " << ns << " " << last );

            Ident ident(rid, config, ns);

            scoped_lock mylk(_mutex);

            _slaves[ident] = gtid;

            if (theReplSet && theReplSet->isPrimary()) {
                theReplSet->ghost->updateSlave(ident.obj["_id"].OID(), gtid);
            }

            _threadsWaitingForReplication.notify_all();
        }

        OP_REPL_STATUS opReplicatedEnough( const GTID& gtid, BSONElement w ) {
            RARELY {
                REPLDEBUG( "looking for : " << op << " w=" << w );
            }

            if (w.isNumber()) {
                return replicatedToNum(gtid, w.numberInt());
            }

            uassert( 16250 , "w has to be a string or a number" , w.type() == String );

            uassert(0, "need to be running with replication to call opReplicatedEnough", theReplSet);

            string wStr = w.String();
            if (wStr == "majority") {
                // use the entire set, including arbiters, to prevent writing
                // to a majority of the set but not a majority of voters
                return replicatedToNum(gtid, theReplSet->config().getMajority());
            }

            map<string,ReplSetConfig::TagRule*>::const_iterator it = theReplSet->config().rules.find(wStr);
            uassert(14830, str::stream() << "unrecognized getLastError mode: " << wStr,
                    it != theReplSet->config().rules.end());

            return gtidReplicated(gtid, (*it).second->last);
        }

        OP_REPL_STATUS replicatedToNum(const GTID& gtid, int w) {
            if ( w <= 1 )
                return REPL_SUCCESS;

            w--; // now this is the # of slaves i need
            scoped_lock mylk(_mutex);
            return _replicatedToNum_slaves_locked( gtid, w );
        }

        OP_REPL_STATUS _replicatedToNum_slaves_locked(const GTID& gtid, int numSlaves ) {
            for ( map<Ident,GTID>::iterator i=_slaves.begin(); i!=_slaves.end(); i++) {
                GTID s = i->second;
                OP_REPL_STATUS currStatus = gtidReplicated(gtid, s);
                if (currStatus == REPL_FAIL) {
                    return REPL_FAIL;
                }
                else if (currStatus == REPL_WAITING) {
                    continue;
                }
                // must be REPL_SUCCESS
                if ( --numSlaves == 0 ) {
                    return REPL_SUCCESS;
                }
            }
            return (numSlaves <= 0) ? REPL_SUCCESS : REPL_WAITING;
        }

        std::vector<BSONObj> getHostsAtOp(GTID gtid) {
            std::vector<BSONObj> result;
            if (theReplSet) {
                result.push_back(theReplSet->myConfig().asBson());
            }

            scoped_lock mylk(_mutex);
            for (map<Ident,GTID>::iterator i = _slaves.begin(); i != _slaves.end(); i++) {
                GTID replicatedTo = i->second;
				if (GTID::cmp(gtid, replicatedTo) <= 0) {
                    result.push_back(i->first.obj["config"].Obj());
                }
            }

            return result;
        }

        unsigned getSlaveCount() const {
            scoped_lock mylk(_mutex);

            return _slaves.size();
        }

        // need to be careful not to deadlock with this
        mutable mongo::mutex _mutex;
        boost::condition _threadsWaitingForReplication;

        map<Ident,GTID> _slaves;

    } slaveTracking;

    void updateSlaveLocation( CurOp& curop, const char * ns , GTID lastGTID ) {
        if ( lastGTID.isInitial() )
            return;

        verify( str::startsWith(ns, "local.oplog.") );

        Client * c = curop.getClient();
        verify(c);
        BSONObj rid = c->getRemoteID();
        if ( rid.isEmpty() )
            return;

        BSONObj handshake = c->getHandshake();
        if (handshake.hasField("config")) {
            slaveTracking.update(rid, handshake["config"].Obj(), ns, lastGTID);
        }
        else {
            BSONObjBuilder bob;
            bob.append("host", curop.getRemoteString());
            bob.append("upgradeNeeded", true);
            slaveTracking.update(rid, bob.done(), ns, lastGTID);
        }

        if (theReplSet && !theReplSet->isPrimary()) {
            // we don't know the slave's port, so we make the replica set keep
            // a map of rids to slaves
            LOG(2) << "percolating " << lastGTID.toString() << " from " << rid << endl;
            theReplSet->ghost->send( boost::bind(&GhostSync::percolate, theReplSet->ghost, rid, lastGTID) );
        }
    }

    OP_REPL_STATUS opReplicatedEnough( GTID gtid, BSONElement w ) {
        return slaveTracking.opReplicatedEnough( gtid, w );
    }

    // TODO: THIS IS ONLY CALLED IN SHARDING,
    // make this better
    bool opReplicatedEnough( GTID gtid, int w ) {
        OP_REPL_STATUS s = slaveTracking.replicatedToNum( gtid, w );
        return (s == REPL_SUCCESS);
    }

    vector<BSONObj> getHostsWrittenTo(GTID gtid) {
        return slaveTracking.getHostsAtOp(gtid);
    }

    void resetSlaveCache() {
        slaveTracking.reset();
    }

    unsigned getSlaveCount() {
        return slaveTracking.getSlaveCount();
    }

    
    class CmdUpdateSlave : public Command {
    public:
        CmdUpdateSlave() : Command("updateSlave") {}
        virtual bool slaveOk() const { return true; }
        virtual bool requiresShardedOperationScope() const { return false; }
        virtual LockType locktype() const { return NONE; }
        virtual bool requiresSync() const { return false; }
        virtual bool needsTxn() const { return false; }
        virtual int txnFlags() const { return noTxnFlags(); }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual OpSettings getOpSettings() const { return OpSettings(); }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::updateSlave);
            out->push_back(Privilege(AuthorizationManager::CLUSTER_RESOURCE_NAME, actions));
        }
        virtual void help( stringstream& help ) const {
            help << "internal." << endl;
        }
        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            GTID value = getGTIDFromBSON("gtid", cmdObj);
            updateSlaveLocation( *cc().curop(), "local.oplog.rs", value);
            return true;
        }
    } cmdUpdateSlave;

}
