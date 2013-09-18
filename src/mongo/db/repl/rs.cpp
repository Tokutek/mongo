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
#include "mongo/platform/basic.h"

#include "mongo/base/owned_pointer_vector.h"
#include "mongo/base/status.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/principal.h"
#include "mongo/db/client.h"
#include "mongo/db/instance.h"
#include "mongo/db/repl.h"
#include "mongo/db/repl/bgsync.h"
#include "mongo/db/repl/connections.h"
#include "mongo/db/repl/replication_server_status.h"  // replSettings
#include "mongo/db/repl/rs.h"
#include "mongo/db/server_parameters.h"
#include "mongo/platform/bits.h"
#include "mongo/db/gtid.h"
#include "mongo/db/txn_context.h"
#include "mongo/util/time_support.h"
#include "mongo/db/oplog.h"
#include "mongo/db/replutil.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/s/d_logic.h"
#include "mongo/util/net/sock.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/db/kill_current_op.h"

using namespace std;

namespace mongo {
    
    using namespace bson;

    bool replSet = false;
    ReplSet *theReplSet = 0;

    // This is a bitmask with the first bit set. It's used to mark connections that should be kept
    // open during stepdowns
    const unsigned ScopedConn::keepOpen = 1;

    bool isCurrentlyAReplSetPrimary() {
        return theReplSet && theReplSet->isPrimary();
    }

    void sethbmsg(const string& s, const int level) {
        if (theReplSet) {
            theReplSet->sethbmsg(s, level);
        }
    }

    void ReplSetImpl::sethbmsg(const std::string& s, int logLevel) {
        static time_t lastLogged;
        _hbmsgTime = time(0);

        if( s == _hbmsg ) {
            // unchanged
            if( _hbmsgTime - lastLogged < 60 )
                return;
        }

        unsigned sz = s.size();
        if( sz >= 256 )
            memcpy(_hbmsg, s.c_str(), 255);
        else {
            _hbmsg[sz] = 0;
            memcpy(_hbmsg, s.c_str(), sz);
        }
        if( !s.empty() ) {
            lastLogged = _hbmsgTime;
            LOG(logLevel) << "replSet " << s << rsLog;
        }
    }

    void ReplSetImpl::goToRollbackState() {
        changeState(MemberState::RS_ROLLBACK);
    }

    void ReplSetImpl::leaveRollbackState() {
        changeState(MemberState::RS_SECONDARY);
    }
    
    bool ReplSetImpl::assumePrimary() {
        boost::unique_lock<boost::mutex> lock(stateChangeMutex);
        
        // Make sure replication has stopped
        stopReplication();

        // Theoretically, we could have been in the rollback state when
        // we decided to assume primary, and then transitioned to fatal
        // before stopping replication. If so, just get out.
        // Given that we are not a secondary, it is ok that replication is
        // stopped
        if (state() != MemberState::RS_SECONDARY) {
            return false;
        }
        LOG(2) << "replSet assuming primary" << endl;
        verify( iAmPotentiallyHot() );

        RSBase::lock rslk(this);
        // will get running operations to interrupt so
        // acquisition of global lock will be faster
        NoteStateTransition nst;
        LOCK_REASON(lockReason, "repl: transitioning to primary");
        Lock::GlobalWrite lk(lockReason);

        gtidManager->verifyReadyToBecomePrimary();

        gtidManager->resetManager();
        changeState(MemberState::RS_PRIMARY);
        return true;
    }

    void ReplSetImpl::changeState(MemberState s) { box.change(s, _self); }

    bool ReplSetImpl::setMaintenanceMode(const bool inc, string& errmsg) {
        boost::unique_lock<boost::mutex> lock(stateChangeMutex);
        {
            RSBase::lock lk(this);
            if (box.getState().primary()) {
                errmsg = "primaries can't modify maintenance mode";
                return false;
            }
            else if (myConfig().arbiterOnly) {
                errmsg = "arbiters can't modify maintenance mode";
                return false;
            }
        }

        if (inc) {
            log() << "replSet going into maintenance mode (" << _maintenanceMode << " other tasks)" << rsLog;

            stopReplication();
            // check after stopReplication because we may be fatal
            if (!box.getState().secondary() && !box.getState().recovering()) {
                errmsg = "cannot modify maintenance mode unless in secondary state or recovering state";
                return false;
            }
            RSBase::lock lk(this);
            // Lock here to prevent state from changing between checking the state and changing it
            // also, grab GlobalWrite here, because it must be grabbed after rslock
            LOCK_REASON(lockReason, "repl: entering maintenance mode");
            Lock::GlobalWrite writeLock(lockReason);
            _maintenanceMode++;
            changeState(MemberState::RS_RECOVERING);
        }
        else {
            // Lock here to prevent state from changing between checking the state and changing it
            RSBase::lock lk(this);
            LOCK_REASON(lockReason, "repl: leaving maintenance mode");
            Lock::GlobalWrite writeLock(lockReason);
            // user error
            if (_maintenanceMode <= 0) {
                errmsg = "cannot set maintenance mode to false when not in maintenance mode to begin with";
                return false;
            }
            _maintenanceMode--;
            if (_maintenanceMode == 0) {
                tryToGoLiveAsASecondary();
            }
            log() << "leaving maintenance mode (" << _maintenanceMode << " other tasks)" << rsLog;
        }

        return true;
    }

    bool ReplSetImpl::inMaintenanceMode() {
        return _maintenanceMode > 0;
    }

    Member* ReplSetImpl::getMostElectable() {
        lock lk(this);

        Member *max = 0;
        set<unsigned>::iterator it = _electableSet.begin();
        while ( it != _electableSet.end() ) {
            const Member *temp = findById(*it);
            if (!temp) {
                log() << "couldn't find member: " << *it << endl;
                set<unsigned>::iterator it_delete = it;
                it++;
                _electableSet.erase(it_delete);
                continue;
            }
            if (!max || max->config().priority < temp->config().priority) {
                max = (Member*)temp;
            }
            it++;
        }

        return max;
    }

    // Note, on input, stateChangeMutex and rslock must be held
    void ReplSetImpl::relinquish(bool startRepl) {
        {
            verify(lockedByMe());
            // will get running operations to interrupt so
            // acquisition of global lock will be faster
            NoteStateTransition nst;
            // so no operations are simultaneously occurring
            RWLockRecursive::Exclusive e(operationLock);
            // so we know writes are not simultaneously occurring
            LOCK_REASON(lockReason, "repl: stepping down from primary");
            Lock::GlobalWrite lk(lockReason);

            LOG(2) << "replSet attempting to relinquish" << endl;
            if( box.getState().primary() ) {
                log() << "replSet relinquishing primary state" << rsLog;
                changeState(MemberState::RS_SECONDARY);

                // close sockets that were talking to us so they don't blithly send many writes that
                // will fail with "not master" (of course client could check result code, but in
                // case they are not)
                log() << "replSet closing client sockets after relinquishing primary" << rsLog;
                MessagingPort::closeAllSockets(ScopedConn::keepOpen);

                // abort all transactions lying around in clients. There should be no
                // transaction happening. Because we have global write lock,  and
                // we are not at risk of aborting a transaction that is in the middle
                // of doing something in a thread. The only transaction a thread
                // begins without holding some lock should be replication, and replication
                // is not running yet. We also need to invalidate all cursors
                // because they may have been part of multi statement
                // transactions.
                ClientCursor::invalidateAllCursors();
                Client::abortLiveTransactions();
                // note the transition is complete, otherwise 
                // replication, which will start, may not work because
                // queries done during replication will be interrupted
                nst.noteTransitionComplete();
                if (startRepl) {
                    startReplication();
                }
            }
        }

        // now that all connections were closed, strip this mongod from all sharding details
        // if and when it gets promoted to a primary again, only then it should reload the sharding state
        // the rationale here is that this mongod won't bring stale state when it regains primaryhood
        shardingState.resetShardingState();
    }

    // for the replSetStepDown command
    bool ReplSetImpl::_stepDown(int secs) {
        boost::unique_lock<boost::mutex> lock(stateChangeMutex);
        RSBase::lock lk(this);
        if( box.getState().primary() ) {
            elect.steppedDown = time(0) + secs;
            log() << "replSet info stepping down as primary secs=" << secs << rsLog;
            relinquish();
            return true;
        }
        return false;
    }

    bool ReplSetImpl::_freeze(int secs) {
        lock lk(this);
        /* note if we are primary we remain primary but won't try to elect ourself again until
           this time period expires.
           */
        if( secs == 0 ) {
            elect.steppedDown = 0;
            log() << "replSet info 'unfreezing'" << rsLog;
        }
        else {
            if( !box.getState().primary() ) {
                elect.steppedDown = time(0) + secs;
                log() << "replSet info 'freezing' for " << secs << " seconds" << rsLog;
            }
            else {
                log() << "replSet info received freeze command but we are primary" << rsLog;
            }
        }
        return true;
    }

    void ReplSetImpl::msgUpdateHBInfo(HeartbeatInfo h) {
        for( Member *m = _members.head(); m; m=m->next() ) {
            if( m->id() == h.id() ) {
                m->_hbinfo = h;
                return;
            }
        }
    }

    list<HostAndPort> ReplSetImpl::memberHostnames() const {
        list<HostAndPort> L;
        L.push_back(_self->h());
        for( Member *m = _members.head(); m; m = m->next() )
            L.push_back(m->h());
        return L;
    }

    void ReplSetImpl::_fillIsMasterHost(const Member *m, vector<string>& hosts, vector<string>& passives, vector<string>& arbiters) {
        verify( m );
        if( m->config().hidden )
            return;

        if( m->potentiallyHot() ) {
            hosts.push_back(m->h().toString());
        }
        else if( !m->config().arbiterOnly ) {
            if( m->config().slaveDelay ) {
                /* hmmm - we don't list these as they are stale. */
            }
            else {
                passives.push_back(m->h().toString());
            }
        }
        else {
            arbiters.push_back(m->h().toString());
        }
    }

    void ReplSetImpl::_fillIsMaster(BSONObjBuilder& b) {
        lock lk(this);
        
        const StateBox::SP sp = box.get();
        bool isp = sp.state.primary();
        b.append("setName", name());
        b.append("ismaster", isp);
        b.append("secondary", sp.state.secondary());
        {
            vector<string> hosts, passives, arbiters;
            _fillIsMasterHost(_self, hosts, passives, arbiters);

            for( Member *m = _members.head(); m; m = m->next() ) {
                verify( m );
                _fillIsMasterHost(m, hosts, passives, arbiters);
            }

            if( hosts.size() > 0 ) {
                b.append("hosts", hosts);
            }
            if( passives.size() > 0 ) {
                b.append("passives", passives);
            }
            if( arbiters.size() > 0 ) {
                b.append("arbiters", arbiters);
            }
        }

        if( !isp ) {
            const Member *m = sp.primary;
            if( m )
                b.append("primary", m->h().toString());
        }
        else {
            b.append("primary", _self->fullName());
        }

        if( myConfig().arbiterOnly )
            b.append("arbiterOnly", true);
        if( myConfig().priority == 0 && !myConfig().arbiterOnly)
            b.append("passive", true);
        if( myConfig().slaveDelay )
            b.append("slaveDelay", myConfig().slaveDelay);
        if( myConfig().hidden )
            b.append("hidden", true);
        if( !myConfig().buildIndexes )
            b.append("buildIndexes", false);
        if( !myConfig().tags.empty() ) {
            BSONObjBuilder a;
            for( map<string,string>::const_iterator i = myConfig().tags.begin(); i != myConfig().tags.end(); i++ )
                a.append((*i).first, (*i).second);
            b.append("tags", a.done());
        }
        b.append("me", myConfig().h.toString());
    }

    /** @param cfgString <setname>/<seedhost1>,<seedhost2> */

    void parseReplsetCmdLine(const std::string& cfgString,
                             string& setname,
                             vector<HostAndPort>& seeds,
                             set<HostAndPort>& seedSet ) {
        const char *p = cfgString.c_str();
        const char *slash = strchr(p, '/');
        if( slash )
            setname = string(p, slash-p);
        else
            setname = p;
        uassert(13093, "bad --replSet config string format is: <setname>[/<seedhost1>,<seedhost2>,...]", !setname.empty());

        if( slash == 0 )
            return;

        p = slash + 1;
        while( 1 ) {
            const char *comma = strchr(p, ',');
            if( comma == 0 ) comma = strchr(p,0);
            if( p == comma )
                break;
            {
                HostAndPort m;
                try {
                    m = HostAndPort( string(p, comma-p) );
                }
                catch(...) {
                    uassert(13114, "bad --replSet seed hostname", false);
                }
                uassert(13096, "bad --replSet command line config string - dups?", seedSet.count(m) == 0 );
                seedSet.insert(m);
                //uassert(13101, "can't use localhost in replset host list", !m.isLocalHost());
                if( m.isSelf() ) {
                    LOG(1) << "replSet ignoring seed " << m.toString() << " (=self)" << rsLog;
                }
                else
                    seeds.push_back(m);
                if( *comma == 0 )
                    break;
                p = comma + 1;
            }
        }
    }

    void ReplSetImpl::init(ReplSetCmdline& replSetCmdline) {
        mgr = new Manager(this);
        ghost = new GhostSync(this);

        _cfg = 0;
        memset(_hbmsg, 0, sizeof(_hbmsg));
        strcpy( _hbmsg , "initial startup" );
        changeState(MemberState::RS_STARTUP);

        _seeds = &replSetCmdline.seeds;

        LOG(1) << "replSet beginning startup..." << rsLog;

        loadConfig();

        unsigned sss = replSetCmdline.seedSet.size();
        for( Member *m = head(); m; m = m->next() ) {
            replSetCmdline.seedSet.erase(m->h());
        }
        for( set<HostAndPort>::iterator i = replSetCmdline.seedSet.begin(); i != replSetCmdline.seedSet.end(); i++ ) {
            if( i->isSelf() ) {
                if( sss == 1 ) {
                    LOG(1) << "replSet warning self is listed in the seed list and there are no other seeds listed did you intend that?" << rsLog;
                }
            }
            else {
                log() << "replSet warning command line seed " << i->toString() << " is not present in the current repl set config" << rsLog;
            }
        }
    }

    ReplSetImpl::ReplSetImpl() :
        _replInfoUpdateRunning(false),
        _replOplogPartitionRunning(false),
        _replKeepOplogAliveRunning(false),
        _keepOplogPeriodMillis(600*1000), // 10 minutes
        _replBackgroundShouldRun(true),
        elect(this),
        _forceSyncTarget(0),
        _blockSync(false),
        _hbmsgTime(0),
        _self(0),
        _maintenanceMode(0),
        mgr(0),
        ghost(0),
        oplogVersion(0) {
    }

    ReplSet::ReplSet() {
    }

    ReplSet* ReplSet::make(ReplSetCmdline& replSetCmdline) {
        auto_ptr<ReplSet> ret(new ReplSet());
        ret->init(replSetCmdline);
        return ret.release();
    }

    void ReplSetImpl::loadGTIDManager() {
        LOCK_REASON(lockReason, "repl: initializing GTID manager");
        Lock::DBWrite lk(rsoplog, lockReason);
        Client::Transaction txn(DB_SERIALIZABLE);
        const BSONObj o = getLastEntryInOplog();
        if (!o.isEmpty()) {
            GTID lastGTID = getGTIDFromBSON("_id", o);
            uint64_t lastTime = o["ts"]._numberLong();
            uint64_t lastHash = o["h"].numberLong();
            gtidManager.reset(new GTIDManager(lastGTID, lastTime, lastHash, _id));
            setTxnGTIDManager(gtidManager.get());            
        }
        else {
            // make a GTIDManager that starts from scratch
            GTID lastGTID;
            // note we initialize the lastTime to 0, so arbiters never get fooled
            // into thinking they are ahead of actual running systems.
            // Either this, or we need to change the code in 
            // ReplSetHealthPollTask::up, where we check if a potential
            // primary is within 10 seconds of this machine
            gtidManager.reset(new GTIDManager(lastGTID, 0, 0, _id));
            setTxnGTIDManager(gtidManager.get());
        }
        txn.commit();
    }

    /* call after constructing to start */
    void ReplSetImpl::_go() {
        try {
            // this might now work on secondaries
            // on secondaries, at this point in the code, we may not have yet created
            // the oplog, but we will see
            loadGTIDManager();
        }
        catch(std::exception& e) {
            log() << "replSet error fatal couldn't query the local " << rsoplog << " collection.  Terminating mongod after 30 seconds." << rsLog;
            log() << e.what() << rsLog;
            sleepsecs(30);
            dbexit( EXIT_REPLICATION_ERROR );
            return;
        }

        changeState(MemberState::RS_STARTUP2);

        // initial sync depends on the manager's threads having started
        // so that heartbeats can be registered. Therefore, we must run this
        // before starting the initial sync below. The rest of the replication
        // threads are started in startThreads below.
        task::fork(mgr);

        bool goLiveAsSecondary = false;
        if (!theReplSet->myConfig().arbiterOnly) {
            // if we are the only member of the config, start us up as the primary.
            // don't depend on threads to startup first.
            if (theReplSet->config().members.size() == 1 &&
                theReplSet->myConfig().potentiallyHot()
                )
            {
                LOCK_REASON(lockReason, "repl: stepping up as primary");
                Lock::GlobalWrite lk(lockReason);
                theReplSet->gtidManager->catchUnappliedToLive();
                GTID lastLiveGTID;
                GTID lastUnappliedGTID;
                theReplSet->gtidManager->getLiveGTIDs(&lastLiveGTID, &lastUnappliedGTID);
                convertOplogToPartitionedIfNecessary(lastLiveGTID);
                changeState(MemberState::RS_PRIMARY);
            }
            else {
                // always do an initial sync on startup
                // if an oplog exists, it will catch up whatever data it needs,
                // acting like a fast sync. If the oplog is not there, it will do
                // a full clone from someone
                syncDoInitialSync();
                GTID lastGTID;
                {
                    LOCK_REASON(lockReason, "repl: updating GTID manager after initial sync");
                    Client::ReadContext ctx(rsoplog, lockReason);
                    Client::Transaction transaction(0);
                    BSONObj o = getLastEntryInOplog();
                    verify(!o.isEmpty());
                    lastGTID = getGTIDFromBSON("_id", o);
                    uint64_t lastTime = o["ts"]._numberLong();
                    uint64_t lastHash = o["h"].numberLong();
                    theReplSet->gtidManager->resetAfterInitialSync(
                        lastGTID,
                        lastTime,
                        lastHash
                        );
                }
                convertOplogToPartitionedIfNecessary(lastGTID);
                goLiveAsSecondary = true;
            }
        }
        else {
            changeState(MemberState::RS_ARBITER);
        }

        // When we get here,
        // we know either the server is the sole primary in a single node
        // replica set, or it does not require an initial sync
        startThreads();
        if (goLiveAsSecondary) {
            boost::unique_lock<boost::mutex> lock(stateChangeMutex);
            RSBase::lock lk(this);
            LOCK_REASON(lockReason, "repl: going live as a secondary");
            Lock::GlobalWrite writeLock(lockReason);
            // temporarily change state to secondary to follow pattern
            // that all threads going live as secondary are transitioning
            // from RS_RECOVERING.
            changeState(MemberState::RS_RECOVERING);
            if (replSettings.startInRecovery) {
                _maintenanceMode++;
            }
            else {
                tryToGoLiveAsASecondary();
            }
        }
    }

    ReplSetImpl::StartupStatus ReplSetImpl::startupStatus = PRESTART;
    DiagStr ReplSetImpl::startupStatusMsg;

    extern BSONObj *getLastErrorDefault;

    void ReplSetImpl::setSelfTo(Member *m) {
        // already locked in initFromConfig
        _self = m;
        _id = m->id();
        _config = m->config();
        if( m ) _buildIndexes = m->config().buildIndexes;
        else _buildIndexes = true;
    }

    /** @param reconf true if this is a reconfiguration and not an initial load of the configuration.
        @return true if ok; throws if config really bad; false if config doesn't include self
    */
    bool ReplSetImpl::initFromConfig(ReplSetConfig& c, bool reconf) {
        /* NOTE: haveNewConfig() writes the new config to disk before we get here.  So
                 we cannot error out at this point, except fatally.  Check errors earlier.
                 */
        lock lk(this);

        if( getLastErrorDefault || !c.getLastErrorDefaults.isEmpty() ) {
            // see comment in dbcommands.cpp for getlasterrordefault
            getLastErrorDefault = new BSONObj( c.getLastErrorDefaults );
        }

        list<ReplSetConfig::MemberCfg*> newOnes;
        // additive short-cuts the new config setup. If we are just adding a
        // node/nodes and nothing else is changing, this is additive. If it's
        // not a reconfig, we're not adding anything
        bool additive = reconf;
        {
            unsigned nfound = 0;
            int me = 0;
            for( vector<ReplSetConfig::MemberCfg>::iterator i = c.members.begin(); i != c.members.end(); i++ ) {
                
                ReplSetConfig::MemberCfg& m = *i;
                if( m.h.isSelf() ) {
                    me++;
                }
                
                if( reconf ) {
                    const Member *old = findById(m._id);
                    if( old ) {
                        nfound++;
                        verify( (int) old->id() == m._id );
                        if( old->config() != m ) {
                            additive = false;
                        }
                    }
                    else {
                        newOnes.push_back(&m);
                    }
                }
            }
            if( me == 0 ) { // we're not in the config -- we must have been removed
                if (state().shunned()) {
                    // already took note of our ejection from the set
                    // so just sit tight and poll again
                    return false;
                }

                _members.orphanAll();

                // kill off rsHealthPoll threads (because they Know Too Much about our past)
                endOldHealthTasks();

                // close sockets to force clients to re-evaluate this member
                MessagingPort::closeAllSockets(0);

                // take note of our ejection
                changeState(MemberState::RS_SHUNNED);

                // go into holding pattern
                log() << "replSet info self not present in the repl set configuration:" << rsLog;
                log() << c.toString() << rsLog;

                loadConfig();  // redo config from scratch
                // if we were shunned, and had to wait for a new config,
                // reset the state to either arbiter or secondary, based on
                // config options
                //
                // I don't know of a better way to do this, unfortunately
                // The problem is I don't want the background sync threads to
                // handle the state transitions, as those might become racy
                // with maintenanceMode. So doing it here
                if (iAmArbiterOnly()) {
                    changeState(MemberState::RS_ARBITER);
                }
                else if (_maintenanceMode > 0 || _blockSync) {
                    changeState(MemberState::RS_RECOVERING);
                }
                else {
                    changeState(MemberState::RS_SECONDARY);
                }
                return false; 
            }
            uassert( 13302, "replSet error self appears twice in the repl set configuration", me<=1 );

            // if we found different members that the original config, reload everything
            if( reconf && config().members.size() != nfound )
                additive = false;
        }

        // If we are changing chaining rules, we don't want this to be an additive reconfig so that
        // the primary can step down and the sync targets change.
        // TODO: This can be removed once SERVER-5208 is fixed.
        if (reconf && config().chainingAllowed() != c.chainingAllowed()) {
            additive = false;
        }

        _cfg = new ReplSetConfig(c);
        dassert( &config() == _cfg ); // config() is same thing but const, so we use that when we can for clarity below
        verify( config().ok() );
        verify( _name.empty() || _name == config()._id );
        _name = config()._id;
        verify( !_name.empty() );
        // this is a shortcut for simple changes
        if( additive ) {
            log() << "replSet info : additive change to configuration" << rsLog;
            for( list<ReplSetConfig::MemberCfg*>::const_iterator i = newOnes.begin(); i != newOnes.end(); i++ ) {
                ReplSetConfig::MemberCfg *m = *i;
                Member *mi = new Member(m->h, m->_id, m, false);

                /** we will indicate that new members are up() initially so that we don't relinquish our
                    primary state because we can't (transiently) see a majority.  they should be up as we
                    check that new members are up before getting here on reconfig anyway.
                    */
                mi->get_hbinfo().health = 0.1;

                _members.push(mi);
                startHealthTaskFor(mi);
            }

            // if we aren't creating new members, we may have to update the
            // groups for the current ones
            _cfg->updateMembers(_members);

            return true;
        }

        // start with no members.  if this is a reconfig, drop the old ones.
        _members.orphanAll();

        endOldHealthTasks();
        
        // Clear out our memory of who might have been syncing from us.
        // Any incoming handshake connections after this point will be newly registered.
        ghost->clearCache();

        int oldPrimaryId = -1;
        {
            const Member *p = box.getPrimary();
            if( p )
                oldPrimaryId = p->id();
        }
        
        if( box.getState().primary() ) {
            // If we are here, that means we must be doing
            // a reconfig, and must have been called by
            // haveNewConfig
            verify(reconf);
            // don't start replication in the middle of a config
            // haveNewConfig will take care of restarting
            // replication if we exit this as a secondary
            relinquish(false);
        }
        else {
            box.setOtherPrimary(0);
        }

        // not setting _self to 0 as other threads use _self w/o locking
        int me = 0;

        // For logging
        string members = "";

        for( vector<ReplSetConfig::MemberCfg>::const_iterator i = config().members.begin(); i != config().members.end(); i++ ) {
            const ReplSetConfig::MemberCfg& m = *i;
            Member *mi;
            members += ( members == "" ? "" : ", " ) + m.h.toString();
            if( m.h.isSelf() ) {
                verify( me++ == 0 );
                mi = new Member(m.h, m._id, &m, true);
                if (!reconf) {
                    log() << "replSet I am " << m.h.toString() << rsLog;
                }
                setSelfTo(mi);

                if( (int)mi->id() == oldPrimaryId )
                    box.setSelfPrimary(mi);
            }
            else {
                mi = new Member(m.h, m._id, &m, false);
                _members.push(mi);
                if( (int)mi->id() == oldPrimaryId )
                    box.setOtherPrimary(mi);
            }
        }

        if( me == 0 ){
            log() << "replSet warning did not detect own host in full reconfig, members " << members << " config: " << c << rsLog;
        }
        else {
            // Do this after we've found ourselves, since _self needs
            // to be set before we can start the heartbeat tasks
            for( Member *mb = _members.head(); mb; mb=mb->next() ) {
                startHealthTaskFor( mb );
            }
        }
        return true;
    }

    // Our own config must be the first one.
    bool ReplSetImpl::_loadConfigFinish(vector<ReplSetConfig*>& cfgs) {
        int v = -1;
        ReplSetConfig *highest = 0;
        int myVersion = -2000;
        int n = 0;
        for( vector<ReplSetConfig*>::iterator i = cfgs.begin(); i != cfgs.end(); i++ ) {
            ReplSetConfig* cfg = *i;
            DEV { LOG(1) << n+1 << " config shows version " << cfg->version << rsLog; }
            if( ++n == 1 ) myVersion = cfg->version;
            if( cfg->ok() && cfg->version > v ) {
                highest = cfg;
                v = cfg->version;
            }
        }
        verify( highest );

        if( !initFromConfig(*highest) )
            return false;

        if( highest->version > myVersion && highest->version >= 0 ) {
            log() << "replSet got config version " << highest->version << " from a remote, saving locally" << rsLog;
            highest->saveConfigLocally(BSONObj());
        }
        return true;
    }

    void ReplSetImpl::loadConfig() {
        startupStatus = LOADINGCONFIG;
        startupStatusMsg.set("loading " + rsConfigNs + " config (LOADINGCONFIG)");
        LOG(1) << "loadConfig() " << rsConfigNs << endl;

        while( 1 ) {
            try {
                OwnedPointerVector<ReplSetConfig> configs;
                try {
                    configs.mutableVector().push_back(ReplSetConfig::makeDirect());
                }
                catch(DBException& e) {
                    log() << "replSet exception loading our local replset configuration object : " << e.toString() << rsLog;
                }
                for( vector<HostAndPort>::const_iterator i = _seeds->begin(); i != _seeds->end(); i++ ) {
                    try {
                        configs.mutableVector().push_back( ReplSetConfig::make(*i) );
                    }
                    catch( DBException& e ) {
                        log() << "replSet exception trying to load config from " << *i << " : " << e.toString() << rsLog;
                    }
                }
                {
                    scoped_lock lck( replSettings.discoveredSeeds_mx );
                    if( replSettings.discoveredSeeds.size() > 0 ) {
                        for (set<string>::iterator i = replSettings.discoveredSeeds.begin(); 
                             i != replSettings.discoveredSeeds.end(); 
                             i++) {
                            try {
                                configs.mutableVector().push_back( ReplSetConfig::make(HostAndPort(*i)) );
                            }
                            catch( DBException& ) {
                                LOG(1) << "replSet exception trying to load config from discovered seed " << *i << rsLog;
                                replSettings.discoveredSeeds.erase(*i);
                            }
                        }
                    }
                }

                if (!replSettings.reconfig.isEmpty()) {
                    try {
                        configs.mutableVector().push_back(ReplSetConfig::make(replSettings.reconfig,
                                                                       true));
                    }
                    catch( DBException& re) {
                        log() << "replSet couldn't load reconfig: " << re.what() << rsLog;
                        replSettings.reconfig = BSONObj();
                    }
                }

                int nok = 0;
                int nempty = 0;
                for( vector<ReplSetConfig*>::iterator i = configs.mutableVector().begin();
                     i != configs.mutableVector().end(); i++ ) {
                    if( (*i)->ok() )
                        nok++;
                    if( (*i)->empty() )
                        nempty++;
                }
                if( nok == 0 ) {

                    if( nempty == (int) configs.mutableVector().size() ) {
                        startupStatus = EMPTYCONFIG;
                        startupStatusMsg.set("can't get " + rsConfigNs + " config from self or any seed (EMPTYCONFIG)");
                        log() << "replSet can't get " << rsConfigNs << " config from self or any seed (EMPTYCONFIG)" << rsLog;
                        static unsigned once;
                        if( ++once == 1 ) {
                            log() << "replSet info you may need to run replSetInitiate -- rs.initiate() in the shell -- if that is not already done" << rsLog;
                        }
                        if( _seeds->size() == 0 ) {
                            LOG(1) << "replSet info no seed hosts were specified on the --replSet command line" << rsLog;
                        }
                    }
                    else {
                        startupStatus = EMPTYUNREACHABLE;
                        startupStatusMsg.set("can't currently get " + rsConfigNs + " config from self or any seed (EMPTYUNREACHABLE)");
                        log() << "replSet can't get " << rsConfigNs << " config from self or any seed (yet)" << rsLog;
                    }

                    sleepsecs(10);
                    continue;
                }

                if( !_loadConfigFinish(configs.mutableVector()) ) {
                    log() << "replSet info Couldn't load config yet. Sleeping 20sec and will try again." << rsLog;
                    sleepsecs(20);
                    continue;
                }
            }
            catch(DBException& e) {
                startupStatus = BADCONFIG;
                startupStatusMsg.set("replSet error loading set config (BADCONFIG)");
                log() << "replSet error loading configurations " << e.toString() << rsLog;
                log() << "replSet error replication will not start" << rsLog;
                sethbmsg("error loading set config");
                _fatal();
                throw;
            }
            break;
        }
        startupStatusMsg.set("? started");
        startupStatus = STARTED;
    }

    void ReplSetImpl::_fatal() {
        box.set(MemberState::RS_FATAL, 0);
        log() << "replSet error fatal, stopping replication" << rsLog;
    }

    void ReplSet::haveNewConfig(ReplSetConfig& newConfig, bool addComment) {
        bo comment;
        if( addComment )
            comment = BSON( "msg" << "Reconfig set" << "version" << newConfig.version );

        newConfig.saveConfigLocally(comment);

        try {
            boost::unique_lock<boost::mutex> lock(stateChangeMutex);
            log() << "stopping replication because we have a new config" << endl;
            stopReplication();
            log() << "stopped replication because we have a new config" << endl;
            // going into this function, we know there is no replication running
            RSBase::lock lk(this);
            // if we are fatal, we will just fall into the catch block below
            massert(16803, "we are fatal, cannot create a new config", !state().fatal());
            if (initFromConfig(newConfig, true)) {
                log() << "replSet replSetReconfig new config saved locally" << rsLog;
            }
            // if necessary, restart replication
            if (isSecondary()) {
                log() << "starting replication because we have a new config" << endl;
                startReplication();
                log() << "started replication because we have a new config" << endl;
            }
            else {
                log() << "NOT starting replication because we have a new config" << endl;
            }
        }
        catch(DBException& e) {
            log() << "replSet error unexpected exception in haveNewConfig() : " << e.toString() << rsLog;
            _fatal();
        }
        catch(...) {
            log() << "replSet error unexpected exception in haveNewConfig()" << rsLog;
            _fatal();
        }
    }

    void Manager::msgReceivedNewConfig(BSONObj o) {
        log() << "replset msgReceivedNewConfig version: " << o["version"].toString() << rsLog;
        scoped_ptr<ReplSetConfig> config(ReplSetConfig::make(o));
        if( config->version > rs->config().version )
            theReplSet->haveNewConfig(*config, false);
        else {
            log() << "replSet info msgReceivedNewConfig but version isn't higher " <<
                  config->version << ' ' << rs->config().version << rsLog;
        }
    }

    /* forked as a thread during startup
       it can run quite a while looking for config.  but once found,
       a separate thread takes over as ReplSetImpl::Manager, and this thread
       terminates.
    */
    void startReplSets(ReplSetCmdline *replSetCmdline) {
        Client::initThread("rsStart");
        try {
            verify( theReplSet == 0 );
            if( replSetCmdline == 0 ) {
                verify(!replSet);
                return;
            }
            replLocalAuth();
            (theReplSet = ReplSet::make(*replSetCmdline))->go();
        }
        catch(std::exception& e) {
            log() << "replSet caught exception in startReplSets thread: " << e.what() << rsLog;
            if( theReplSet )
                theReplSet->fatal();
        }
        cc().shutdown();
    }

    void ReplSet::shutdown() {
        BackgroundSync::get()->shutdown();
        stopReplInfoThread();
    }

    void replLocalAuth() {
        if (!AuthorizationManager::isAuthEnabled())
            return;
        cc().getAuthorizationSession()->grantInternalAuthorization(
                UserName("_repl", "local"));
    }

    // for testing only
    void ReplSetImpl::setKeepOplogAlivePeriod(uint64_t val) {
        {
            boost::unique_lock<boost::mutex> lock(_keepOplogAliveMutex);
            _keepOplogPeriodMillis = val;
        }
        _keepOplogAliveCond.notify_all();
    }

    void ReplSetImpl::keepOplogAliveThread() {
        _replKeepOplogAliveRunning = true;
        GTID lastSeenGTID = gtidManager->getLiveState();
        Client::initThread("keepOplogAlive");
        replLocalAuth();
        while (_replBackgroundShouldRun) {
            // make it 10 minutes
            {
                boost::unique_lock<boost::mutex> lock(_keepOplogAliveMutex);
                _keepOplogAliveCond.timed_wait(
                    lock,
                    boost::posix_time::milliseconds(_keepOplogPeriodMillis)
                    );
            }
            GTID curr = gtidManager->getLiveState();
            RWLockRecursive::Shared lk(operationLock);
            if (_replBackgroundShouldRun && _isMaster() && GTID::cmp(curr, lastSeenGTID) == 0) {
                Client::Transaction txn (DB_SERIALIZABLE);
                OplogHelpers::logComment(BSON("comment" << "keepOplogAlive"));
                txn.commit(DB_TXN_NOSYNC);
                lastSeenGTID = gtidManager->getLiveState();
            }
            else {
                lastSeenGTID = curr;
            }
        }
        cc().shutdown();
        _replKeepOplogAliveRunning = false;
    }

    void ReplSetImpl::changeExpireOplog(uint64_t expireOplogDays, uint64_t expireOplogHours) {
        boost::unique_lock<boost::mutex> lock(_oplogPartitionMutex);
        replSettings.expireOplogDays = expireOplogDays;
        replSettings.expireOplogHours = expireOplogHours;
        _oplogPartitionCond.notify_all();
    }

    // responsible for adding and dropping partitions from the oplog
    void ReplSetImpl::oplogPartitionThread() {
        _replOplogPartitionRunning = true;
        Client::initThread("oplogPartitionThread");
        replLocalAuth();
        log() << "starting thread" << rsLog;
        while (_replBackgroundShouldRun) {
            const uint64_t currTime = curTimeMillis64();
            uint64_t expireMillis = 0;
            {
                boost::unique_lock<boost::mutex> lock(_oplogPartitionMutex);
                expireMillis = expireOplogMilliseconds();
            }
            // deal with add partition
            try {
                uint64_t lastAddTime = getLastPartitionAddTime();
                // if expireMillis is greater than a day (or 0), then we partition daily,
                // otherwise, we partition hourly
                uint64_t timeBetweenAdds = (expireMillis == 0 || expireMillis >= 24*60*60*1000) ? 24*60*60*1000 : 60*60*1000;
                LOG(2) << "lastAddTime: " << lastAddTime << 
                    " currTime: " << currTime <<
                    " timeBetweenAdds: " << timeBetweenAdds <<
                    "diff" << currTime - lastAddTime << rsLog;
                if (currTime > lastAddTime && ((currTime - lastAddTime) > timeBetweenAdds)) {
                    LOG(2) << "adding partition!" << rsLog;
                    addOplogPartitions();
                } 
                else {
                    LOG(2) << "not adding partition" << rsLog;
                }
            }
            catch(std::exception& e) {
                log() << "replSet caught oplog partition thread (when adding): " << e.what() << rsLog;
            }
            catch (...) {
                log() << "exception cought in oplog partition thread (when adding): " << rsLog;
            }

            // deal with possible drop partition
            if (expireMillis) {
                try {
                    // possibly drop partition
                    if (currTime > expireMillis) { // avoid overflow error
                        trimOplogWithTS(curTimeMillis64() - expireMillis);
                    }
                    else {
                        log() << "Not dropping partitions. expireMillis is too large. " <<
                            "currTime: " << currTime << " expireMillis: " << expireMillis << rsLog;
                    }
                }
                catch(std::exception& e) {
                    log() << "replSet caught oplog partition thread (when dropping): " << e.what() << rsLog;
                }
                catch (...) {
                    log() << "exception caught in oplog partition thread (when dropping): " << rsLog;
                }
            }

            // now sleep for 60 seconds. We basically run this loop once a minute
            {
                boost::unique_lock<boost::mutex> lock(_oplogPartitionMutex);
                LOG(2) << "sleeping" << rsLog;
                _oplogPartitionCond.timed_wait(
                    lock,
                    boost::posix_time::milliseconds(60*1000)
                    );
                LOG(2) << "woke up" << rsLog;
            }
            
        }
        log() << "ending thread" << rsLog;
        cc().shutdown();
        _replOplogPartitionRunning = false;
    }

    void ReplSetImpl::forceUpdateReplInfo() {
        boost::unique_lock<boost::mutex> lock(_replInfoMutex);
        GTID minUnappliedGTID;
        GTID minLiveGTID;
        verify(gtidManager != NULL);
        gtidManager->getMins(&minLiveGTID, &minUnappliedGTID);
        LOCK_REASON(lockReason, "repl: force updating repl info");
        Lock::DBRead lk("local", lockReason);
        Client::Transaction transaction(DB_SERIALIZABLE);
        logToReplInfo(minLiveGTID, minUnappliedGTID);
        transaction.commit();
    }

    void ReplSetImpl::updateReplInfoThread() {
        _replInfoUpdateRunning = true;
        GTID lastMinUnappliedGTID;
        GTID lastMinLiveGTID;
        Client::initThread("updateReplInfo");
        replLocalAuth();
        // not sure if this is correct, don't yet know how to ensure
        // that we don't have race conditions with shutdown
        while (_replBackgroundShouldRun) {
            if (theReplSet) {
                try {
                    boost::unique_lock<boost::mutex> lock(_replInfoMutex);
                    GTID minUnappliedGTID;
                    GTID minLiveGTID;
                    verify(gtidManager != NULL);
                    gtidManager->getMins(&minLiveGTID, &minUnappliedGTID);
                    // Note that these CANNOT be >, they must be !=. In the
                    // case of rollback, these values may go backwards, and this
                    // thread must capture that information.
                    if (GTID::cmp(lastMinLiveGTID, minLiveGTID) != 0 ||
                        GTID::cmp(lastMinUnappliedGTID, minUnappliedGTID) != 0
                        )
                    {
                        LOCK_REASON(lockReason, "repl: updating repl info");
                        Lock::DBRead lk("local", lockReason);
                        Client::Transaction transaction(DB_SERIALIZABLE);
                        logToReplInfo(minLiveGTID, minUnappliedGTID);
                        lastMinUnappliedGTID = minUnappliedGTID;
                        lastMinLiveGTID = minLiveGTID;
                        transaction.commit();
                    }
                }
                catch (...) {
                    log() << "exception cought in updateReplInfo thread: " << rsLog;
                }
            }
            sleepsecs(1);
        }
        cc().shutdown();
        _replInfoUpdateRunning = false;
    }

    void ReplSetImpl::registerSlave(const BSONObj& rid, const int memberId) {
        // To prevent race conditions with clearing the cache at reconfig time,
        // we lock the replset mutex here.
        lock lk(this);
        ghost->associateSlave(rid, memberId);
    }

    void ReplSetImpl::stopReplInfoThread() {        
        _replBackgroundShouldRun = false;
        log() << "waiting for updateReplInfo thread to end" << endl;
        while (_replInfoUpdateRunning) {
            sleepsecs(1);
            log() << "still waiting for updateReplInfo thread to end..." << endl;
        }
        {
            boost::unique_lock<boost::mutex> lock(_keepOplogAliveMutex);
            _keepOplogAliveCond.notify_all();
        }
        while (_replKeepOplogAliveRunning) {
            sleepsecs(1);
            log() << "still waiting for keep oplog alive thread to end..." << endl;
        }
        {
            boost::unique_lock<boost::mutex> lock(_oplogPartitionMutex);
            _oplogPartitionCond.notify_all();
        }
        while (_replOplogPartitionRunning) {
            sleepsecs(1);
            log() << "still waiting for oplog partition thread to end..." << endl;
        }
    }

    // look at comments in BackgroundSync::stopOpSyncThread for rules
    void ReplSetImpl::stopReplication() {
        // arbiters do not have these threads running, so only do this
        // if we are not an arbiter
        if (!iAmArbiterOnly()) {
            BackgroundSync::get()->stopOpSyncThread();
        }
    }

    // look at comments in BackgroundSync::startOpSyncThread for rules
    void ReplSetImpl::startReplication() {
        // arbiters do not have these threads running, so only do this
        // if we are not an arbiter
        if (!iAmArbiterOnly()) {
            BackgroundSync::get()->startOpSyncThread();
        }
    }

    class ReplIndexPrefetch : public ServerParameter {
    public:
        ReplIndexPrefetch()
            : ServerParameter( ServerParameterSet::getGlobal(), "replIndexPrefetch" ) {
        }

        virtual ~ReplIndexPrefetch() {
        }

        const char * _value() {
            if (!theReplSet)
                return "uninitialized";
            return "none";
        }

        virtual void append( BSONObjBuilder& b, const string& name ) {
            b.append( name, _value() );
        }

        virtual Status set( const BSONElement& newValueElement ) {
            if (!theReplSet) {
                return Status( ErrorCodes::BadValue, "replication is not enabled" );
            }

            std::string prefetch = newValueElement.valuestrsafe();
            return setFromString( prefetch );
        }

        virtual Status setFromString( const string& prefetch ) {
            return Status( ErrorCodes::IllegalOperation, "replIndexPrefetch is a deprecated parameter" );
        }

    } replIndexPrefetch;
}

