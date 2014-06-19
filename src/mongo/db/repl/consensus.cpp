/**
*    Copyright (C) 2010 10gen Inc.
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
#include "../commands.h"
#include "rs.h"
#include "multicmd.h"

namespace mongo {

    bool shouldVeto(const uint32_t id, const int config, string& errmsg) {
        const Member* primary = theReplSet->box.getPrimary();
        const Member* hopeful = theReplSet->findById(id);
        const Member *highestPriority = theReplSet->getMostElectable();
    
        if( !hopeful ) {
            errmsg = str::stream() << "replSet couldn't find member with id " << id;
            return true;
        }
        else if( theReplSet->config().version > config ) {
            errmsg = str::stream() << "replSet member " << id << " is not yet aware its cfg version " << config << " is stale";
            return true;
        }
        else if( theReplSet->isPrimary() )
        {
            // hbinfo is not updated, so we have to check the primary's last GTID separately
            errmsg = str::stream() << "I am already primary, " << hopeful->fullName() <<
                " can try again once I've stepped down";
            return true;
        }
        else if( primary ) 
        {
            // other members might be aware of more up-to-date nodes
            errmsg = str::stream() << hopeful->fullName() << " is trying to elect itself but " <<
                primary->fullName() << " is already primary and more up-to-date";
            return true;
        }
        else if( highestPriority && highestPriority->config().priority > hopeful->config().priority) {
            errmsg = str::stream() << hopeful->fullName() << " has lower priority than " << highestPriority->fullName();
            return true;
        }
    
        return false;
    }

    /** the first cmd called by a node seeking election and it's a basic sanity 
        test: do any of the nodes it can reach know that it can't be the primary?
        */
    class CmdReplSetFresh : public ReplSetCommand {
    public:
        CmdReplSetFresh() : ReplSetCommand("replSetFresh") { }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetFresh);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
    private:

        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( !check(errmsg, result) ) {
                return false;
            }
            return theReplSet->elect.mayElectCmdReceived(cmdObj, errmsg, result);
        }
    } cmdReplSetFresh;

    class CmdReplSetElect : public ReplSetCommand {
    public:
        CmdReplSetElect() : ReplSetCommand("replSetElect") { }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replSetElect);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
    private:
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( !check(errmsg, result) )
                return false;
            theReplSet->elect.electCmdReceived(cmdObj, &result);
            return true;
        }
    } cmdReplSetElect;

    class CmdReplAssumedPrimary : public ReplSetCommand {
    public:
        CmdReplAssumedPrimary() : ReplSetCommand("replAssumedPrimary") { }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::replAssumedPrimary);
            out->push_back(Privilege(AuthorizationManager::SERVER_RESOURCE_NAME, actions));
        }
    private:
        virtual bool run(const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            if( !check(errmsg, result) ) {
                return false;
            }
            theReplSet->elect.newPrimaryNotificationReceived(cmdObj);
            result.append("ok", 1);
            return true;
        }
    } cmdReplAssumedPrimary;

    int Consensus::totalVotes() const {
        static int complain = 0;
        int vTot = rs._self->config().votes;
        for( Member *m = rs.head(); m; m=m->next() )
            vTot += m->config().votes;
        if( vTot % 2 == 0 && vTot && complain++ == 0 )
            log() << "replSet " /*buildbot! warning */ "total number of votes is even - add arbiter or give one member an extra vote" << rsLog;
        return vTot;
    }

    bool Consensus::aMajoritySeemsToBeUp() const {
        int vUp = rs._self->config().votes;
        for( Member *m = rs.head(); m; m=m->next() )
            vUp += m->hbinfo().up() ? m->config().votes : 0;
        return vUp * 2 > totalVotes();
    }

    bool Consensus::shouldRelinquish() const {
        GTID ourLiveState = theReplSet->gtidManager->getLiveState();
        uint64_t ourHighestKnownPrimary = theReplSet->gtidManager->getHighestKnownPrimary();
        int vUp = rs._self->config().votes;
        for( Member *m = rs.head(); m; m=m->next() ) {
            if (GTID::cmp(ourLiveState, m->hbinfo().gtid) < 0) {
                log() << "our GTID is" << ourLiveState.toString() << \
                    ", " << m->fullName() << " has GTID " << m->hbinfo().gtid.toString() << \
                    ", relinquishing primary" << rsLog;
                return true;
            }
            uint64_t otherHighestKnownPrimary = m->hbinfo().highestKnownPrimaryInSet;
            if (ourHighestKnownPrimary < otherHighestKnownPrimary) {
                log() << "our highestKnownPrimary " << ourHighestKnownPrimary << \
                    ", " << m->fullName() << " has highestKnownPrimary " << otherHighestKnownPrimary << \
                    ", relinquishing primary" << rsLog;
                return true;
            }
            if (m->hbinfo().up()) {
                vUp += m->config().votes;
            }
        }

        // the manager will handle calling stepdown if another node should be
        // primary due to priority
        if (!( vUp * 2 > totalVotes())) {
            log() << "can't see a majority of the set, relinquishing primary" << rsLog;
            return true;
        }

        return false;
    }

    unsigned Consensus::yea(unsigned memberId) {
        return rs._self->config().votes;
    }

    // called when a remote is trying to gauge whether it should
    // run an election to make itself primary. The task of this function
    // is to give the remote as good an indicator as possible whether
    // we will vote yea or not, so that the remote has a good sense
    // of whether to bother running.
    bool Consensus::mayElectCmdReceived(BSONObj& cmdObj, string& errmsg, BSONObjBuilder& result) {
        GTIDManager* gtidMgr = theReplSet->gtidManager.get();
        if( cmdObj["set"].String() != theReplSet->name() ) {
            errmsg = "wrong repl set name";
            return false;
        }
        string who = cmdObj["who"].String();
        int cfgver = cmdObj["cfgver"].Int();
        uint32_t id = cmdObj["id"].Int();
        GTID remoteGTID = getGTIDFromBSON("GTID", cmdObj);
        GTID ourGTID = gtidMgr->getLiveState();
        
        bool weAreFresher = false;
        // check not only our own GTID, but any other member we can reach
        if (GTID::cmp(remoteGTID, ourGTID) < 0 ||
                 GTID::cmp(remoteGTID, theReplSet->lastOtherGTID()) < 0) {                
            log() << "we are fresher! remoteGTID" << remoteGTID.toString() << " ourGTID " << ourGTID.toString() << " lastOther " << theReplSet->lastOtherGTID() << " " << rsLog;
            weAreFresher = true;
        }
        addGTIDToBSON("GTID", ourGTID, result);
        result.append("fresher", weAreFresher);
        bool veto = shouldVeto(id, cfgver, errmsg);
        // have this check here because once we get to the second phase of the election,
        // we don't want this to be a reason for an election failure
        // ignoreElectable is a test hook that is only used in js tests, like consensus.js
        if (!veto && !cmdObj["ignoreElectable"].ok()) {
            if (!theReplSet->isElectable(id)) {
                errmsg = str::stream() << "I don't think " << theReplSet->findById(id)->fullName() <<
                    " is electable";
                veto = true;
            }
        }
        result.append("veto", veto);
        if (veto) {
            result.append("errmsg", errmsg);
        }
        // stands for "highest known primary"
        uint64_t highestKnownPrimaryToUse = std::max(gtidMgr->getHighestKnownPrimary(),
            theReplSet->getHighestKnownPrimaryAcrossSet());
        result.append("hkp", highestKnownPrimaryToUse);
        result.append("vote", rs._self->config().votes);
        return true;
    }

    /* todo: threading **************** !!!!!!!!!!!!!!!! */
    void Consensus::electCmdReceived(BSONObj cmd, BSONObjBuilder* _b) {
        BSONObjBuilder& b = *_b;
        DEV log() << "replSet received elect msg " << cmd.toString() << rsLog;
        else LOG(2) << "replSet received elect msg " << cmd.toString() << rsLog;
        string set = cmd["set"].String();
        unsigned whoid = cmd["whoid"].Int();
        int cfgver = cmd["cfgver"].Int();
        OID round = cmd["round"].OID();
        int myver = rs.config().version;

        const Member* hopeful = rs.findById(whoid);

        int vote = 0;
        string errmsg;
        if( set != rs.name() ) {
            log() << "replSet error received an elect request for '" << set << "' but our set name is '" << rs.name() << "'" << rsLog;
        }
        else if( myver < cfgver ) {
            // we are stale.  don't vote
        }
        else if (shouldVeto(whoid, cfgver, errmsg)) {
            log() << "Election vetoed with: " << errmsg << rsLog;
            vote = -10000;
        }
        else {
            uint64_t primaryToUse = 0;
            GTIDManager* gtidMgr = theReplSet->gtidManager.get();
            PRIMARY_VOTE voteVal;
            if (cmd["primaryToUse"].ok()) {
                GTID remoteGTID = getGTIDFromBSON("gtid", cmd);
                primaryToUse = cmd["primaryToUse"].numberLong();
                voteVal = gtidMgr->acceptPossiblePrimary(primaryToUse, remoteGTID);
                if (voteVal == VOTE_YES) {
                    rs.handleHighestKnownPrimaryOfMember(primaryToUse);
                }
            }
            else {
                // it's 1.5 machine, with the older protocol
                voteVal = VOTE_YES;
            }
            if (voteVal == VOTE_YES) {
                vote = yea(whoid);
                dassert( hopeful->id() == whoid );
                log() << "replSet info voting yea for " <<  hopeful->fullName() << " (" << whoid << ')' << rsLog;
                if (vote > 0 && primaryToUse > 0) { // second part of if-clause is for backward compat, old members won't have this value
                    // sync this to disk, so that if we crash, we remember that
                    // we voted for this primary, avoiding the possibility of
                    // voting twice for the same primary, once before a crash
                    // and once after
                    persistVote.persist(primaryToUse, true);
                }
            }
            else {
                verify(voteVal == VOTE_NO || voteVal == VOTE_VETO);
                if (voteVal == VOTE_VETO) {
                    vote = -10000;
                }
                log() << "Due to bad possible primary, replSet did NOT vote yea for " <<  hopeful->fullName() << " (" << whoid << ')' << rsLog;
            }
        }

        b.append("vote", vote);
        b.append("round", round);
    }

    // This method is called when a newly elected primary sends a
    // replAssumedPrimary command to notify the this member that
    // a primary was just elected. The purpose of this method
    // is to induce an immediete heartbeat with this newly elected primary,
    // so that the likelihood of another primary being elected is decreased.
    //
    // Having this method succeed is not necessary for the election algorithm
    // to work, because even if another primary is elected in addition to this one
    // (which is possible), the manager will properly handle it. The purpose of this 
    // method is to help make that annoying scenario much less likely.
    void Consensus::newPrimaryNotificationReceived(BSONObj cmd) {
        string set = cmd["set"].String();
        unsigned whoid = cmd["whoid"].Int();
        int cfgver = cmd["cfgver"].Int();
        int myver = rs.config().version;

        const Member* hopeful = rs.findById(whoid);

        if( set != rs.name() ) {
            log() << "replSet error received primary notification for '" << set << "' but our set name is '" << rs.name() << "'" << rsLog;
            return;
        }
        else if (hopeful == NULL) {
            log() << "Could not find hopeful, returning" << rsLog;
        }
        else if( myver < cfgver ) {
            return;
        }
        // request an immediete heartbeat
        // the reason we request a heartbeat and not have this command
        // relay the heartbeat information is that currently, heartbeats have a nice
        // invariant that they happen on a single thread. Having this command
        // relay heartbeat information would break that. We could likely get that working,
        // but it adds a bit of complexity. Instead, at the cost of an additional round trip,
        // we simply force a heartbeat to occur
        rs.forceHeartbeat(hopeful);
    }

    void ReplSetImpl::_getTargets(list<Target>& L, int& configVersion) {
        configVersion = config().version;
        for( Member *m = head(); m; m=m->next() )
            if( m->hbinfo().maybeUp() )
                L.push_back( Target(m->fullName()) );
    }

    /* config version is returned as it is ok to use this unlocked.  BUT, if unlocked, you would need
       to check later that the config didn't change. */
    void ReplSetImpl::getTargets(list<Target>& L, int& configVersion) {
        if( lockedByMe() ) {
            _getTargets(L, configVersion);
            return;
        }
        lock lk(this);
        _getTargets(L, configVersion);
    }

    /* Do we have the newest data of them all?
       @param allUp - set to true if all members are up.  Only set if true returned.
       @return true if we are freshest.  Note we may tie.
    */
    bool Consensus::weAreFreshest(bool& allUp, int& nTies, uint64_t& highestKnownPrimary) {
        int prelimVote = rs._self->config().votes;
        // we will disregard the prelim vote if we find a member
        // that does not report it
        bool disregardPrelimVote = false;
        nTies = 0;
        GTID ourGTID = rs.gtidManager->getLiveState();
        highestKnownPrimary = rs.gtidManager->getHighestKnownPrimary();
        BSONObjBuilder cmdBuilder;
        cmdBuilder.append("replSetFresh", 1);
        cmdBuilder.append("set", rs.name());
        addGTIDToBSON("GTID", ourGTID, cmdBuilder);
        cmdBuilder.append("who", rs._self->fullName());
        cmdBuilder.append("cfgver", rs._cfg->version);
        cmdBuilder.append("id", rs._self->id());
        BSONObj cmd = cmdBuilder.done();

        list<Target> L;
        int ver;
        /* the following queries arbiters, even though they are never fresh.  wonder if that makes sense.
           it doesn't, but it could, if they "know" what freshness it one day.  so consider removing
           arbiters from getTargets() here.  although getTargets is used elsewhere for elections; there
           arbiters are certainly targets - so a "includeArbs" bool would be necessary if we want to make
           not fetching them herein happen.
           */
        rs.getTargets(L, ver);
        multiCommand(cmd, L);
        int nok = 0;
        allUp = true;
        for( list<Target>::iterator i = L.begin(); i != L.end(); i++ ) {
            if( i->ok ) {
                nok++;
                if( i->result["fresher"].trueValue() ) {
                    log() << "not electing self, we are not freshest" << rsLog;
                    return false;
                }
                GTID remoteGTID = getGTIDFromBSON("GTID", i->result);
                if( GTID::cmp(remoteGTID, ourGTID) == 0 ) {
                    nTies++;
                }
                verify( GTID::cmp(remoteGTID, ourGTID) <= 0 );

                if( i->result["veto"].trueValue() ) {
                    BSONElement msg = i->result["errmsg"];
                    if (!msg.eoo()) {
                        log() << "not electing self, " << i->toHost << " would veto with '" <<
                            msg.String() << "'" << rsLog;
                    }
                    else {
                        log() << "not electing self, " << i->toHost << " would veto" << rsLog;
                    }
                    return false;
                }
                // 1.5 members won't be sending these
                if ( i->result["hkp"].ok()) {
                    uint64_t memHighestKnownPrimary = i->result["hkp"].numberLong();
                    if (memHighestKnownPrimary > highestKnownPrimary) {
                        highestKnownPrimary = memHighestKnownPrimary;
                    }
                }
                if (i->result["vote"].ok()) {
                    prelimVote += i->result["vote"].Int();
                }
                else {
                    disregardPrelimVote = true;
                }
            }
            else {
                DEV log() << "replSet freshest returns " << i->result.toString() << rsLog;
                allUp = false;
            }
        }
        int total = totalVotes();
        if (!disregardPrelimVote && prelimVote*2 <= total) {
            log () << "not electing self, only have " << prelimVote << " out of " << total << " votes."<< rsLog;
            return false;
        }
        LOG(1) << "replSet dev we are freshest of up nodes, nok:" << nok << " nTies:" << nTies << rsLog;
        return true;
    }

    extern time_t started;

    void Consensus::multiCommand(BSONObj cmd, list<Target>& L) {
        verify( !rs.lockedByMe() );
        mongo::multiCommand(cmd, L);
    }

    // Method a member calls to try to elect oneself as primary
    // The election protocol has the following steps:
    // Step 1: ask all members whether I would make a good primary
    //  In this step, if any other member has sees a reason to not
    //  make this member a primary, it will say so. Note that one of these possible reasons
    //  is "I see another primary, so don't bother". Also, in doing so,
    //  other members relay what election ID (highestKnownPrimary) so
    //  that this member can pick a high enough value that will result
    //  in a successful election.
    // Step 2: Tell members "I plan on being primary, give me official votes".
    //  This should only be done if the first step gives us a really good indication
    //  that the election will be successful. In fact, the only reason this election should
    //  not be successful is if something unexpected happened, like connections
    //  between other members got either broken or restored after step 1.
    //  While accumulating the votes, any member that votes "yes" will stop acknowledging
    //  writes that have a GTID with a lower primary value. See Consensus::electCmdReceived.
    // Step 3: If a majority vote total was accumulated, become primary.
    // Step 4: Send a message to all members saying that we have become primary,
    //  and that those members should send us a heartbeat request to get updated
    //  information. Note that this step is not necessary for correctness, it is a nice to have.
    //  This step can do nothing to undo our ascencion to primary in step 3. All this does is
    //  speed up the time other members get notified that we have become primary, to
    //  hopefully avoid a situation where another member also elects itself as primary.
    //
    void Consensus::_electSelf() {
        bool sleepThisRound = !sleptLast;
        sleptLast = false;
        if( time(0) < steppedDown ) {
            return;
        }
        
        bool allUp;
        int nTies;
        uint64_t highestKnownPrimary = 0;
        if( !weAreFreshest(allUp, nTies, highestKnownPrimary) ) {
            return;
        }

        rs.sethbmsg("",9);

        if( !allUp && time(0) - started < 60 * 5 ) {
            /* the idea here is that if a bunch of nodes bounce all at once, we don't want to drop data
               if we don't have to -- we'd rather be offline and wait a little longer instead
               todo: make this configurable.
               */
            rs.sethbmsg("not electing self, not all members up and we have been up less than 5 minutes");
            return;
        }

        Member& me = *rs._self;

        if( nTies ) {
            /* tie?  we then randomly sleep to try to not collide on our voting. */
            /* todo: smarter. */
            // also, vanilla MongoDB does not sleep if id is 0, because theoretically
            // one guy doesn't ever need to sleep. 
            if (me.id() != 0 && sleepThisRound) {
                verify( !rs.lockedByMe() ); // bad to go to sleep locked
                unsigned ms = ((unsigned) rand()) % 1000 + 50;
                log() << "replSet tie " << nTies << " sleeping a little " << ms << "ms" << rsLog;
                sleptLast = true;
                sleepmillis(ms);
                throw RetryAfterSleepException();
            }
        }

        time_t start = time(0);
        unsigned meid = me.id();
        int tally = yea( meid );
        bool success = false;
        try {
            log() << "replSet info electSelf " << meid << rsLog;
            uint64_t primaryToUse = highestKnownPrimary+1;
            BSONObjBuilder b;
            b.append("replSetElect", 1);
            b.append("set", rs.name());
            b.append("who", me.fullName());
            b.append("whoid", me.hbinfo().id());
            b.append("cfgver", rs._cfg->version);
            b.append("round", OID::gen());
            b.append("primaryToUse", primaryToUse);
            b.append("gtid", theReplSet->gtidManager->getLiveState());
            BSONObj electCmd = b.obj();

            int configVersion;
            list<Target> L;
            rs.getTargets(L, configVersion);
            multiCommand(electCmd, L);

            {
                for( list<Target>::iterator i = L.begin(); i != L.end(); i++ ) {
                    DEV log() << "replSet elect res: " << i->result.toString() << rsLog;
                    if( i->ok ) {
                        int v = i->result["vote"].Int();
                        tally += v;
                    }
                }
                if( tally*2 <= totalVotes() ) {
                    log() << "replSet couldn't elect self, only received " << tally << " votes" << rsLog;
                }
                else if( time(0) - start > 30 ) {
                    // defensive; should never happen as we have timeouts on connection and operation for our conn
                    log() << "replSet too much time passed during our election, ignoring result" << rsLog;
                }
                else if( configVersion != rs.config().version ) {
                    log() << "replSet config version changed during our election, ignoring result" << rsLog;
                }
                else if (theReplSet->gtidManager->acceptPossiblePrimary(primaryToUse, theReplSet->gtidManager->getLiveState()) != VOTE_YES) {
                    log() << "Could not accept " << primaryToUse << " as a primary GTID value, another election likely snuck in"<< rsLog;
                }
                else {
                    /* succeeded. */
                    LOG(1) << "replSet election succeeded, assuming primary role" << rsLog;
                    theReplSet->handleHighestKnownPrimaryOfMember(primaryToUse);
                    // persist the fact that we have voted for self
                    // it does not need to be synced to disk right now, it just
                    // needs to make it before the first write that this guy does
                    // as primary, hence we can pass false for second parameter
                    persistVote.persist(primaryToUse, false);
                    success = rs.assumePrimary(primaryToUse);
                    if (!success) {
                        log() << "tried to assume primary and failed" << rsLog;
                    }
                    else {                        
                        BSONObj assumedPrimaryCmd = BSON(
                                               "replAssumedPrimary" << 1 <<
                                               "set" << rs.name() <<
                                               "who" << me.fullName() <<
                                               "whoid" << me.hbinfo().id() <<
                                               "cfgver" << rs._cfg->version 
                                               );
                        
                        list<Target> L;
                        int ver;
                        rs.getTargets(L, ver);
                        multiCommand(assumedPrimaryCmd, L);
                        // no need to check return values
                        // This is meant to be a helpful notification that the
                        // primary election has succeeded. It is not necessary
                        // for correctness. If all of these commands fail,
                        // future heartbeats will relay the information
                        // that this command could not
                    }
                }
            }
        }
        catch( std::exception& ) {
            throw;
        }
    }

    void Consensus::electSelf() {
        verify( !rs.lockedByMe() );
        verify( !rs.myConfig().arbiterOnly );
        verify( rs.myConfig().slaveDelay == 0 );
        try {
            _electSelf();
        }
        catch(RetryAfterSleepException&) {
            throw;
        }
        catch(DBException& e) {
            log() << "replSet warning caught unexpected exception in electSelf() " << e.toString() << rsLog;
        }
        catch(...) {
            log() << "replSet warning caught unexpected exception in electSelf()" << rsLog;
        }
    }

}
