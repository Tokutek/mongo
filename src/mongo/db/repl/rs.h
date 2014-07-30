// /db/repl/rs.h

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

#pragma once

#include "mongo/db/commands.h"
#include "mongo/db/collection.h"
#include "mongo/db/oplog.h"
#include "mongo/db/oplogreader.h"
#include "mongo/db/repl/rs_config.h"
#include "mongo/db/repl/rs_exception.h"
#include "mongo/db/repl/rs_member.h"
#include "mongo/db/repl/rs_optime.h"
#include "mongo/db/repl/rs_sync.h"
#include "mongo/util/concurrency/list.h"
#include "mongo/util/concurrency/msg.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/concurrency/value.h"
#include "mongo/util/net/hostandport.h"

/**
 * Order of Events
 *
 * On startup, if the --replSet option is present, startReplSets is called.
 * startReplSets forks off a new thread for replica set activities.  It creates
 * the global theReplSet variable and calls go() on it.
 *
 * theReplSet's constructor changes the replica set's state to RS_STARTUP,
 * starts the replica set manager, and loads the config (if the replica set
 * has been initialized).
 */

namespace mongo {

    struct HowToFixUp;
    struct Target;
    class DBClientConnection;
    class ReplSetImpl;
    class OplogReader;
    extern bool replSet; // true if using repl sets
    extern class ReplSet *theReplSet; // null until initialized
    extern Tee *rsLog;

    /* member of a replica set */
    class Member : public List1<Member>::Base {
    private:
        ~Member(); // intentionally unimplemented as should never be called -- see List1<>::Base.
        Member(const Member&); 
    public:
        Member(HostAndPort h, unsigned ord, const ReplSetConfig::MemberCfg *c, bool self);

        string fullName() const { return h().toString(); }
        const ReplSetConfig::MemberCfg& config() const { return _config; }
        ReplSetConfig::MemberCfg& configw() { return _config; }
        const HeartbeatInfo& hbinfo() const { return _hbinfo; }
        HeartbeatInfo& get_hbinfo() { return _hbinfo; }
        string lhb() const { return _hbinfo.lastHeartbeatMsg; }
        MemberState state() const { return _hbinfo.hbstate; }
        const HostAndPort& h() const { return _h; }
        unsigned id() const { return _hbinfo.id(); }

        bool potentiallyHot() const { return _config.potentiallyHot(); } // not arbiter, not priority 0
        void summarizeMember(stringstream& s) const;
        // If we could sync from this member.  This doesn't tell us anything about the quality of
        // this member, just if they are a possible sync target.
        bool syncable() const;
        void recvHeartbeat() {
            _lastHeartbeatRecv = time(0);
        }
        time_t getLastRecvHeartbeat() {
            return _lastHeartbeatRecv;
        }

    private:
        friend class ReplSetImpl;
        ReplSetConfig::MemberCfg _config;
        const HostAndPort _h;
        HeartbeatInfo _hbinfo;
        // This is the last time we got a heartbeat request from a given member.
        time_t _lastHeartbeatRecv;
    };

    class Manager : public task::Server {
        ReplSetImpl *rs;
        bool busyWithElectSelf;
        int _primary;

        /** @param two - if true two primaries were seen.  this can happen transiently, in addition to our
                         polling being only occasional.  in this case null is returned, but the caller should
                         not assume primary itself in that situation.
        */
        const Member* findOtherPrimary(bool& two);

        void noteARemoteIsPrimary(const Member *);
        void checkElectableSet();
        // returns true if auth issue, false otherwise
        bool checkAuth();
        virtual void starting();
    public:
        Manager(ReplSetImpl *rs);
        virtual ~Manager();
        void msgReceivedNewConfig(BSONObj);
        void msgCheckNewState();
    };

    class GhostSync : public task::Server {
        struct GhostSlave : boost::noncopyable {
            GhostSlave() : slave(0), init(false) { }
            OplogReader reader;
            GTID lastGTID;
            Member* slave;
            bool init;
        };
        /**
         * This is a cache of ghost slaves
         */
        typedef map< mongo::OID,shared_ptr<GhostSlave> > MAP;
        MAP _ghostCache;
        RWLock _lock; // protects _ghostCache
        ReplSetImpl *rs;
        virtual void starting();
    public:
        GhostSync(ReplSetImpl *_rs) : task::Server("rsGhostSync"), _lock("GhostSync"), rs(_rs) {}
        ~GhostSync() {
            log() << "~GhostSync() called" << rsLog;
        }

        /**
         * Replica sets can sync in a hierarchical fashion, which throws off w
         * calculation on the master.  percolate() faux-syncs from an upstream
         * node so that the primary will know what the slaves are up to.
         *
         * We can't just directly sync to the primary because it could be
         * unreachable, e.g., S1--->S2--->S3--->P.  S2 should ghost sync from S3
         * and S3 can ghost sync from the primary.
         *
         * Say we have an S1--->S2--->P situation and this node is S2.  rid
         * would refer to S1.  S2 would create a ghost slave of S1 and connect
         * it to P (_currentSyncTarget). Then it would use this connection to
         * pretend to be S1, replicating off of P.
         */
        void percolate(const BSONObj& rid, const GTID& lastGTID);
        void associateSlave(const BSONObj& rid, const int memberId);
        void updateSlave(const mongo::OID& id, const GTID& lastGTID);
        void clearCache();
    };

    struct Target;

    class PersistHighestVotedForPrimary {
        boost::mutex _mutex;
        uint64_t _lastPersistedVal;
    public:
        PersistHighestVotedForPrimary() : _lastPersistedVal(0) { }

        void persist(uint64_t val, bool persist) {
            boost::unique_lock<boost::mutex> lock(_mutex);
            if (val > _lastPersistedVal) {
                {
                    LOCK_REASON(lockReason, "repl: force updating repl info");
                    Client::ReadContext ctx(rsOplogRefs , lockReason);
                    Client::Transaction transaction(DB_SERIALIZABLE);
                    logHighestVotedForPrimary(val);
                    transaction.commit(persist ? 0 : DB_TXN_NOSYNC); // make sure we sync transaction
                }
                _lastPersistedVal = val;
            }
        }
    };
    
    class Consensus {
        ReplSetImpl &rs;
        PersistHighestVotedForPrimary persistVote;
        unsigned yea(unsigned memberId);
        void _electSelf();
        bool weAreFreshest(bool& allUp, int& nTies, uint64_t& highestKnownPrimary);
        bool sleptLast; // slept last elect() pass
    public:
        Consensus(ReplSetImpl *t) : rs(*t), sleptLast(false), steppedDown(0) {
        }

        /* if we've stepped down, this is when we are allowed to try to elect ourself again.
           todo: handle possible weirdnesses at clock skews etc.
        */
        time_t steppedDown;

        int totalVotes() const;
        bool aMajoritySeemsToBeUp() const;
        bool shouldRelinquish() const;
        void electSelf();
        bool mayElectCmdReceived(BSONObj& cmdObj, string& errmsg, BSONObjBuilder& result);
        void electCmdReceived(BSONObj, BSONObjBuilder*);
        void multiCommand(BSONObj cmd, list<Target>& L);
        void newPrimaryNotificationReceived(BSONObj cmd);
    };

    /**
     * most operations on a ReplSet object should be done while locked. that
     * logic implemented here.
     *
     * Order of locking: lock the replica set, then take a rwlock.
     */
    class RSBase : boost::noncopyable {
    public:
        const unsigned magic;
        void assertValid() { verify( magic == 0x12345677 ); }
    private:
        mongo::mutex m;
        int _locked;
        ThreadLocalValue<bool> _lockedByMe;
    protected:
        RSBase() : magic(0x12345677), m("RSBase"), _locked(0) { }
        ~RSBase() {
            /* this can happen if we throw in the constructor; otherwise never happens.  thus we log it as it is quite unusual. */
            log() << "replSet ~RSBase called" << rsLog;
        }

    public:
        class lock {
            RSBase& rsbase;
            auto_ptr<scoped_lock> sl;
        public:
            lock(RSBase* b) : rsbase(*b) {
                if( rsbase._lockedByMe.get() )
                    return; // recursive is ok...

                sl.reset( new scoped_lock(rsbase.m) );
                DEV verify(rsbase._locked == 0);
                rsbase._locked++;
                rsbase._lockedByMe.set(true);
            }
            ~lock() {
                if( sl.get() ) {
                    verify( rsbase._lockedByMe.get() );
                    DEV verify(rsbase._locked == 1);
                    rsbase._lockedByMe.set(false);
                    rsbase._locked--;
                }
            }
        };

        /* for asserts */
        bool locked() const { return _locked != 0; }

        /* if true, is locked, and was locked by this thread. note if false, it could be in the lock or not for another
           just for asserts & such so we can make the contracts clear on who locks what when.
           we don't use these locks that frequently, so the little bit of overhead is fine.
        */
        bool lockedByMe() { return _lockedByMe.get(); }
    };

    class ReplSetHealthPollTask;

    /* safe container for our state that keeps member pointer and state variables always aligned */
    class StateBox : boost::noncopyable {
    public:
        struct SP { // SP is like pair<MemberState,const Member *> but nicer
            SP() : state(MemberState::RS_STARTUP), primary(0) { }
            MemberState state;
            const Member *primary;
        };
        const SP get() {
            rwlock lk(m, false);
            return sp;
        }
        MemberState getState() const {
            rwlock lk(m, false);
            return sp.state;
        }
        const Member* getPrimary() const {
            rwlock lk(m, false);
            return sp.primary;
        }
        void change(MemberState s, const Member *self) {
            rwlock lk(m, true);
            if( sp.state != s ) {
                log() << "replSet " << s.toString() << rsLog;
            }
            sp.state = s;
            if( s.primary() ) {
                sp.primary = self;
            }
            else {
                if( self == sp.primary )
                    sp.primary = 0;
            }
        }
        void set(MemberState s, const Member *p) {
            rwlock lk(m, true);
            sp.state = s;
            sp.primary = p;
        }
        void setSelfPrimary(const Member *self) { change(MemberState::RS_PRIMARY, self); }
        void setOtherPrimary(const Member *mem) {
            rwlock lk(m, true);
            verify( !sp.state.primary() );
            sp.primary = mem;
        }
        void noteRemoteIsPrimary(const Member *remote) {
            rwlock lk(m, true);
            verify(!sp.state.primary());
            sp.primary = remote;
        }
        StateBox() : m("StateBox") { }
    private:
        RWLock m;
        SP sp;
    };

    void parseReplsetCmdLine(const std::string& cfgString,
                             string& setname,
                             vector<HostAndPort>& seeds,
                             set<HostAndPort>& seedSet);

    /** Parameter given to the --replSet command line option (parsed).
        Syntax is "<setname>/<seedhost1>,<seedhost2>"
        where setname is a name and seedhost is "<host>[:<port>]" */
    class ReplSetCmdline {
    public:
        ReplSetCmdline(const std::string& cfgString) { parseReplsetCmdLine(cfgString, setname, seeds, seedSet); }
        string setname;
        vector<HostAndPort> seeds;
        set<HostAndPort> seedSet;
    };

    /* information about the entire repl set, such as the various servers in the set, and their state */
    /* note: We currently do not free mem when the set goes away - it is assumed the replset is a
             singleton and long lived.
    */
    class ReplSetImpl : protected RSBase {
    public:
        /** info on our state if the replset isn't yet "up".  for example, if we are pre-initiation. */
        enum StartupStatus {
            PRESTART=0, LOADINGCONFIG=1, BADCONFIG=2, EMPTYCONFIG=3,
            EMPTYUNREACHABLE=4, STARTED=5, SOON=6
        };
        static StartupStatus startupStatus;
        static DiagStr startupStatusMsg;
        static string stateAsHtml(MemberState state);

        /* todo thread */
        void msgUpdateHBInfo(HeartbeatInfo);

        StateBox box;

        shared_ptr<GTIDManager> gtidManager;
        // this lock protects the _blockSync variable and the _maintenanceMode
        // variable. It must be taken before the rslock. It protects state changes
        // that depend on those variables, meaning RS_SECONDARY, RS_PRIMARY,
        // and RS_RECOVERING. Because one stops the opsync thread with this
        // lock held, and stopping the opsync thread may take seconds, this
        // lock may be held for a long time and should be taken before the
        // rslock.
        boost::mutex stateChangeMutex;
        bool forceSyncFrom(const string& host, string& errmsg, BSONObjBuilder& result);
        // Check if the current sync target is suboptimal. This must be called while holding a mutex
        // that prevents the sync source from changing.
        bool shouldChangeSyncTarget(const uint64_t& target) const;

        /**
         * Find the closest member (using ping time) with a higher latest GTID.
         */
        const Member* getMemberToSyncTo();
        void veto(const string& host, unsigned secs=10);
        bool gotForceSync();
        void goToRollbackState();
        void leaveRollbackState();
    private:
        // for replInfoUpdate
        boost::mutex _replInfoMutex;
        bool _replInfoUpdateRunning;
        // for oplogPartitionThread
        boost::mutex _oplogPartitionMutex;
        boost::condition_variable _oplogPartitionCond;
        bool _replOplogPartitionRunning;
        // for keepOplogAlive
        bool _replKeepOplogAliveRunning;
        uint64_t _keepOplogPeriodMillis;
        boost::mutex _keepOplogAliveMutex;
        boost::condition_variable _keepOplogAliveCond;

        // mutex protecting access to _highestKnownPrimaryAcrossReplSet
        boost::mutex _hkpAcrossReplSetMutex;
        // this stores the highestKnownPrimary we know of
        // that has been communicated by any machine, through heartbeats,
        // within the replica set. This value does NOT control what
        // this machine can or cannot vote for in an election. That is managed
        // by the GTIDManager. This value is used to notify partially
        // disconnected primaries that another election with a higher value
        // has taken place, and therefore should step down. See
        // Consensus::shouldRelinquish
        uint64_t _highestKnownPrimaryAcrossReplSet;

        bool _replBackgroundShouldRun;
        
        set<ReplSetHealthPollTask*> healthTasks;
        void endOldHealthTasks();
        void startHealthTaskFor(Member *m);
        // implemented in heartbeat.cpp to have access to ReplSetHealthPollTask
        // organization and file layout of these classes needs serious refactoring
        void forceHeartbeat(const Member* m);

        Consensus elect;
        void relinquish(bool startReplication = true);
    protected:
        bool _stepDown(int secs);
        bool _freeze(int secs);
    private:
        bool assumePrimary(uint64_t primaryToUse);
        void loadGTIDManager();
        void changeState(MemberState s);

        Member* _forceSyncTarget;

        bool _blockSync;
        void blockSync(bool block);

        // set of electable members' _ids
        set<unsigned> _electableSet;
    protected:
        // "heartbeat message"
        // sent in requestHeartbeat respond in field "hbm"
        char _hbmsg[256]; // we change this unlocked, thus not an stl::string
        time_t _hbmsgTime; // when it was logged
    public:
        void sethbmsg(const std::string& s, int logLevel = 0);

        /**
         * Election with Priorities
         *
         * Each node (n) keeps a set of nodes that could be elected primary.
         * Each node in this set:
         *
         *  1. can connect to a majority of the set
         *  2. has a priority greater than 0
         *  3. has an optime within 10 seconds of the most up-to-date node
         *     that n can reach
         *
         * If a node fails to meet one or more of these criteria, it is removed
         * from the list.  This list is updated whenever the node receives a
         * heartbeat.
         *
         * When a node sends an "am I freshest?" query, the node receiving the
         * query checks their electable list to make sure that no one else is
         * electable AND higher priority.  If this check passes, the node will
         * return an "ok" response, if not, it will veto.
         *
         * If a node is primary and there is another node with higher priority
         * on the electable list (i.e., it must be synced to within 10 seconds
         * of the current primary), the node (or nodes) with connections to both
         * the primary and the secondary with higher priority will issue
         * replSetStepDown requests to the primary to allow the higher-priority
         * node to take over.  
         */
        void addToElectable(const unsigned m) { lock lk(this); _electableSet.insert(m); }
        void rmFromElectable(const unsigned m) { lock lk(this); _electableSet.erase(m); }
        bool iAmElectable() { lock lk(this); return _electableSet.find(_self->id()) != _electableSet.end(); }
        bool isElectable(const unsigned id) { lock lk(this); return _electableSet.find(id) != _electableSet.end(); }
        Member* getMostElectable();
        bool handleHighestKnownPrimaryOfMember(uint64_t hkp);
        uint64_t getHighestKnownPrimaryAcrossSet();
    protected:
        /**
         * Load a new config as the replica set's main config.
         *
         * If there is a "simple" change (just adding a node), this shortcuts
         * the config. Returns true if the config was changed.  Returns false
         * if the config doesn't include a this node.  Throws an exception if
         * something goes very wrong.
         *
         * Behavior to note:
         *  - locks this
         *  - intentionally leaks the old _cfg and any old _members (if the
         *    change isn't strictly additive)
         */
        bool initFromConfig(ReplSetConfig& c, bool reconf=false); 
        void _fillIsMaster(BSONObjBuilder&);
        void _fillIsMasterHost(const Member*, vector<string>&, vector<string>&, vector<string>&);
        const ReplSetConfig& config() { return *_cfg; }
        string name() const { return _name; } /* @return replica set's logical name */
        MemberState state() const { return box.getState(); }
        void _fatal();
        void _getOplogDiagsAsHtml(unsigned server_id, stringstream& ss) const;
        void _summarizeAsHtml(stringstream&) const;
        void _summarizeStatus(BSONObjBuilder&) const; // for replSetGetStatus command

        /* call afer constructing to start - returns fairly quickly after launching its threads */
        void _go();

    private:
        string _name;
        const vector<HostAndPort> *_seeds;
        ReplSetConfig *_cfg;

        /**
         * Finds the configuration with the highest version number and attempts
         * load it.
         */
        bool _loadConfigFinish(vector<ReplSetConfig*>& v);
        /**
         * Gather all possible configs (from command line seeds, our own config
         * doc, and any hosts listed therein) and try to initiate from the most
         * recent config we find.
         */
        void loadConfig();

        list<HostAndPort> memberHostnames() const;
        bool iAmArbiterOnly() const { return myConfig().arbiterOnly; }
        bool iAmPotentiallyHot() const {
          return myConfig().potentiallyHot() && // not an arbiter
            elect.steppedDown <= time(0) && // not stepped down/frozen
            state() == MemberState::RS_SECONDARY; // not stale
        }
    protected:
        Member *_self;
        bool _buildIndexes;       // = _self->config().buildIndexes

        ReplSetImpl();
        /* throws exception if a problem initializing. */
        void init(ReplSetCmdline&);

        void setSelfTo(Member *); // use this as it sets buildIndexes var
    private:
        List1<Member> _members; // all members of the set EXCEPT _self.
        ReplSetConfig::MemberCfg _config; // config of _self
        unsigned _id; // _id of _self

        int _maintenanceMode; // if we should stay in recovering state
    public:
        unsigned selfId() const { return _id; }
        Manager *mgr;
        GhostSync *ghost;
        /**
         * This forces a secondary to go into recovering state and stay there
         * until this is called again, passing in "false".  Multiple threads can
         * call this and it will leave maintenance mode once all of the callers
         * have called it again, passing in false.
         */
        bool setMaintenanceMode(const bool inc, string& errmsg);
        bool inMaintenanceMode();
        // Records a new slave's id in the GhostSlave map, at handshake time.
        void registerSlave(const BSONObj& rid, const int memberId);
    private:
        Member* head() const { return _members.head(); }
    public:
        const Member* findById(unsigned id) const;
        void stopReplInfoThread();
        Member* findByName(const std::string& hostname) const;
        // for testing
        void setKeepOplogAlivePeriod(uint64_t val);
        void changeExpireOplog(uint64_t expireOplogDays, uint64_t expireOplogHours);
    private:
        void _getTargets(list<Target>&, int &configVersion);
        void getTargets(list<Target>&, int &configVersion);
        void startThreads();
        void keepOplogAliveThread();
        void updateReplInfoThread();
        void oplogPartitionThread();
        friend class FeedbackThread;
        friend class CmdReplSetFresh;
        friend class CmdReplSetElect;
        friend class CmdReplAssumedPrimary;
        friend class Member;
        friend class Manager;
        friend class GhostSync;
        friend class Consensus;

    private:
        bool _syncDoInitialSync_clone( const char *master, const list<string>& dbs, shared_ptr<DBClientConnection> conn);
        void _fillGaps(OplogReader* r); // helper function for initial sync
        void _applyMissingOpsDuringInitialSync(); // helper function for initial sync
        bool _syncDoInitialSync();
        void syncDoInitialSync();

        // keep a list of hosts that we've tried recently that didn't work
        map<string,time_t> _veto;

    public:
        static const int maxSyncSourceLagSecs;

        const ReplSetConfig::MemberCfg& myConfig() const { return _config; }
        void tryToGoLiveAsASecondary(); // readlocks
        const uint64_t lastOtherOpTime() const;
        const GTID lastOtherGTID() const;

        void stopReplication();
        void startReplication();
        void forceUpdateReplInfo();
        
        int oplogVersion;
    };

    class ReplSet : public ReplSetImpl {
    public:
        static ReplSet* make(ReplSetCmdline& replSetCmdline);
        virtual ~ReplSet() {}

        // for the replSetStepDown command
        bool stepDown(int secs) { return _stepDown(secs); }

        // for the replSetFreeze command
        bool freeze(int secs) { return _freeze(secs); }

        string selfFullName() {
            verify( _self );
            return _self->fullName();
        }

        virtual bool buildIndexes() const { return _buildIndexes; }

        /* call after constructing to start - returns fairly quickly after launching its threads */
        void go() { _go(); }
        void shutdown();

        void fatal() { _fatal(); }
        virtual bool isPrimary() { return box.getState().primary(); }
        virtual bool isSecondary() {  return box.getState().secondary(); }
        MemberState state() const { return ReplSetImpl::state(); }
        string name() const { return ReplSetImpl::name(); }
        virtual const ReplSetConfig& config() { return ReplSetImpl::config(); }
        void getOplogDiagsAsHtml(unsigned server_id, stringstream& ss) const { _getOplogDiagsAsHtml(server_id,ss); }
        void summarizeAsHtml(stringstream& ss) const { _summarizeAsHtml(ss); }
        void summarizeStatus(BSONObjBuilder& b) const  { _summarizeStatus(b); }
        void fillIsMaster(BSONObjBuilder& b) { _fillIsMaster(b); }

        /**
         * We have a new config (reconfig) - apply it.
         * @param comment write a no-op comment to the oplog about it.  only
         * makes sense if one is primary and initiating the reconf.
         *
         * The slaves are updated when they get a heartbeat indicating the new
         * config.  The comment is a no-op.
         */
        void haveNewConfig(ReplSetConfig& c, bool comment);

        /**
         * Pointer assignment isn't necessarily atomic, so this needs to assure
         * locking, even though we don't delete old configs.
         */
        const ReplSetConfig& getConfig() { return config(); }

        bool lockedByMe() { return RSBase::lockedByMe(); }

        // heartbeat msg to send to others; descriptive diagnostic info
        string hbmsg() const {
            if( time(0)-_hbmsgTime > 120 ) return "";
            return _hbmsg;
        }

    protected:
        ReplSet();
    };

    /**
     * Base class for repl set commands.  Checks basic things such if we're in
     * rs mode before the command does its real work.
     */
    class ReplSetCommand : public Command {
    protected:
        ReplSetCommand(const char * s, bool show=false) : Command(s, show) { }
        virtual bool slaveOk() const { return true; }
        virtual bool requiresShardedOperationScope() const { return false; }
        virtual bool adminOnly() const { return true; }
        virtual bool logTheOp() { return false; }
        virtual LockType locktype() const { return NONE; }
        virtual bool needsTxn() const { return false; }
        virtual int txnFlags() const { return noTxnFlags(); }
        virtual bool requiresSync() const { return false; }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual void help( stringstream &help ) const { help << "internal"; }

        bool check(string& errmsg, BSONObjBuilder& result) {
            if( !replSet ) {
                errmsg = "not running with --replSet";
                if( cmdLine.configsvr ) { 
                    result.append("info", "configsvr"); // for shell prompt
                }
                return false;
            }

            if( theReplSet == 0 || theReplSet->gtidManager == NULL) {
                result.append("startupStatus", ReplSet::startupStatus);
                string s;
                errmsg = ReplSet::startupStatusMsg.empty() ? "replset unknown error 2" : ReplSet::startupStatusMsg.get();
                if( ReplSet::startupStatus == 3 )
                    result.append("info", "run rs.initiate(...) if not yet done for the set");
                return false;
            }

            return true;
        }
    };

    /**
     * does local authentication
     * directly authorizes against AuthenticationInfo
     */
    void replLocalAuth();

    /** inlines ----------------- */

    inline Member::Member(HostAndPort h, unsigned ord, const ReplSetConfig::MemberCfg *c, bool self) :
        _config(*c), _h(h), _hbinfo(ord), _lastHeartbeatRecv(0){
        verify(c);
        if( self )
            _hbinfo.health = 1.0;
    }

    inline bool ignoreUniqueIndex(IndexDetails& idx) {
        if (!idx.unique()) {
            return false;
        }
        if (!theReplSet) {
            return false;
        }
        // see SERVER-6671
        MemberState ms = theReplSet->state();
        if (! ((ms == MemberState::RS_STARTUP2) ||
               (ms == MemberState::RS_RECOVERING) ||
               (ms == MemberState::RS_ROLLBACK))) {
            return false;
        }
        // 2 is the oldest oplog version where operations
        // are fully idempotent.
        if (theReplSet->oplogVersion < 2) {
            return false;
        }
        // Never ignore _id index
        if (idx.isIdIndex()) {
            return false;
        }
        
        return true;
    }

    inline BSONObj getLastEntryInOplog() {
        BSONObj o;
        LOCK_REASON(lockReason, "repl: getting last entry in oplog");
        Client::ReadContext lk(rsoplog, lockReason);
        Collection *cl = getCollection(rsoplog);
        shared_ptr<Cursor> c(Cursor::make(cl, -1));
        return c->ok() ? c->current().copy() : BSONObj();
    }

    // meant to be run during startup
    inline uint64_t getSavedHighestVotedForPrimary() {
        BSONObj o;
        LOCK_REASON(lockReason, "repl: getting highestVote");
        Client::WriteContext lk(rsVoteInfo, lockReason);
        Client::Transaction txn(DB_SERIALIZABLE);
        Collection* voteInfo = getCollection(rsVoteInfo);
        if (!voteInfo) {
            log () << "voteInfo collection does not exist, creating it" << rsLog;
            string err;
            bool ret = userCreateNS(rsVoteInfo, BSONObj(), err, false);
            verify(ret);
        }

        const bool found = Collection::findOne(rsVoteInfo, BSON("_id" << "highestVote"), o);
        if (found) {
            return o["val"].numberLong();
        }
        log () << "Could not find highestVote in replInfo, returning 0" << rsLog;
        txn.commit();
        return 0;
    }
}
