// cloner.cpp - copy a database (export/import basically)

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

#include "mongo/pch.h"
#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/builder.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/client/remote_transaction.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/client.h"
#include "mongo/db/cloner.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/rename_collection.h"
#include "mongo/db/instance.h"
#include "mongo/db/kill_current_op.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/repl.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/db/database.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/storage/exception.h"

namespace mongo {

    BSONElement getErrField(const BSONObj& o);

    bool replAuthenticate(DBClientBase *, bool);

    void mayInterrupt( bool mayBeInterrupted ) {
        if ( mayBeInterrupted ) {
            killCurrentOp.checkForInterrupt();   
        }
    }

    bool masterSameProcess(const char *masterHost) {
        stringstream a,b;
        a << "localhost:" << cmdLine.port;
        b << "127.0.0.1:" << cmdLine.port;
        return (a.str() == masterHost || b.str() == masterHost);
    }

    bool checkSelfClone(
        const char *masterHost,
        string fromDB,
        string& errmsg
        ) 
    {
        string todb = cc().database()->name();
        bool same = masterSameProcess(masterHost);
        if ( same ) {
            if ( fromDB == todb && cc().database()->path() == dbpath ) {
                // guard against an "infinite" loop
                // if you are replicating, the local.sources config may be wrong if you get this
                errmsg = "can't clone from self (localhost).";
                return false;
            }
        }
        return true;
    }

    shared_ptr<DBClientConnection> makeConnection(
        const char *masterHost,
        string& errmsg
        ) 
    {
        verify(!masterSameProcess(masterHost));
        ConnectionString cs = ConnectionString::parse(masterHost, errmsg);
        shared_ptr<DBClientConnection> conn(static_cast<DBClientConnection *>(cs.connect(errmsg)));
        if (!replAuthenticate(conn.get(), false)) {
            errmsg = "can't authenticate replication";
            conn.reset();
        }
        return conn;
    }

    class Cloner: boost::noncopyable {
        shared_ptr<DBClientBase> conn;
        void copy(
            const char *from_ns, 
            const char *to_ns, 
            bool isindex, 
            bool logForRepl,
            bool slaveOk, 
            bool mayBeInterrupted, 
            bool isCapped,
            Query q = Query()
            );
        struct Fun;
    public:
        Cloner(shared_ptr<DBClientBase> &c) : conn(c) {}

        /* slaveOk     - if true it is ok if the source of the data is !ismaster.
           useReplAuth - use the credentials we normally use as a replication slave for the cloning
           snapshot    - use $snapshot mode for copying collections.  note this should not be used when it isn't required, as it will be slower.
                         for example repairDatabase need not use it.
        */

        /** copy the entire database */
        bool go(
            const char *masterHost, 
            string& errmsg, 
            const string& fromdb, 
            bool logForRepl, 
            bool slaveOk, 
            bool useReplAuth, 
            bool mayBeInterrupted, 
            int *errCode = 0
            );
        
        bool go(
            const char *masterHost, 
            const CloneOptions& opts, 
            set<string>& clonedColls, 
            string& errmsg, 
            int *errCode = 0
            );

        bool copyCollection(
            const string& ns , 
            const BSONObj& query , 
            string& errmsg , 
            bool copyIndexes
            );
        
        void copyCollectionData(
            const string& ns, 
            const BSONObj& query,
            bool copyIndexes,
            bool logForRepl
            ); 
    };

    /* for index info object:
         { "name" : "name_1" , "ns" : "foo.index3" , "key" :  { "name" : 1.0 } }
       we need to fix up the value in the "ns" parameter so that the name prefix is correct on a
       copy to a new name.
    */
    static BSONObj fixindex(BSONObj o, const string &dbname) {
        BSONObjBuilder b;
        BSONObjIterator i(o);
        while ( i.moreWithEOO() ) {
            BSONElement e = i.next();
            if ( e.eoo() )
                break;

            // for now, skip the "v" field so that v:0 indexes will be upgraded to v:1
            if ( string("v") == e.fieldName() ) {
                continue;
            }

            if ( string("ns") == e.fieldName() ) {
                uassert( 
                    10024 , 
                    "bad ns field for index during dbcopy", 
                    e.type() == String
                    );
                const char *p = strchr(e.valuestr(), '.');
                uassert( 10025 , "bad ns field for index during dbcopy [2]", p);
                string newname = dbname + p;
                b.append("ns", newname);
            }
            else
                b.append(e);
        }
        BSONObj res= b.obj();
        return res;
    }

    struct Cloner::Fun {
        Fun() : lastLog(0) { }
        time_t lastLog;
        void operator()(DBClientCursorBatchIterator &i) {
            const string to_dbname = nsToDatabase(to_collection);
            while (i.moreInCurrentBatch()) {
                if (n % 128 == 127) {
                    time_t now = time(0);
                    if (now - lastLog >= 60) { 
                        // report progress
                        if (lastLog) {
                            log() << "clone " << to_collection << ' ' << n << endl;
                        }
                        lastLog = now;
                    }
                    mayInterrupt(_mayBeInterrupted);
                }

                BSONObj js = i.nextSafe();
                ++n;

                if (isindex) {
                    verify(nsToCollectionSubstring(from_collection) == "system.indexes");
                    storedForLater->push_back(fixindex(js, to_dbname).getOwned());
                }
                else {
                    try {
                        Client::ReadContext ctx(to_collection);
                        if (_isCapped) {
                            NamespaceDetails *d = nsdetails(to_collection);
                            verify(d->isCapped());
                            BSONObj pk = js["$_"].Obj();
                            BSONObjBuilder rowBuilder;                        
                            BSONObjIterator it(js);
                            while (it.moreWithEOO()) {
                                BSONElement e = it.next();
                                if (e.eoo()) {
                                    break;
                                }
                                if (!mongoutils::str::equals(e.fieldName(), "$_")) {
                                    rowBuilder.append(e);
                                }
                            }
                            BSONObj row = rowBuilder.obj();
                            d->insertObjectIntoCappedWithPK(pk, row, NamespaceDetails::NO_LOCKTREE);
                        }
                        else {
                            insertObject(to_collection, js, 0, logForRepl);
                        }
                    }
                    catch (UserException& e) {
                        error() << "error: exception cloning object in " << from_collection << ' ' << e.what() << " obj:" << js.toString() << '\n';
                        throw;
                    }

                    RARELY if ( time( 0 ) - saveLast > 60 ) {
                        log() << n << " objects cloned so far from collection " << from_collection << endl;
                        saveLast = time( 0 );
                    }
                }
            }
        }
        int n;
        bool isindex;
        const char *from_collection;
        const char *to_collection;
        time_t saveLast;
        list<BSONObj> *storedForLater;
        bool logForRepl;
        bool _mayBeInterrupted;
        bool _isCapped;
    };

    /* copy the specified collection
       isindex - if true, this is system.indexes collection, in which we do some transformation when copying.
    */
    void Cloner::copy(
        const char *from_collection, 
        const char *to_collection, 
        bool isindex, 
        bool logForRepl, 
        bool slaveOk, 
        bool mayBeInterrupted,
        bool isCapped,
        Query query
        ) 
    {
        list<BSONObj> storedForLater;

        LOG(2) << "\t\tcloning collection " << from_collection << " to " << to_collection << " on " << conn->getServerAddress() << " with filter " << query.toString() << endl;

        Fun f;
        f.n = 0;
        f.isindex = isindex;
        f.from_collection = from_collection;
        f.to_collection = to_collection;
        f.saveLast = time( 0 );
        f.storedForLater = &storedForLater;
        f.logForRepl = logForRepl;
        f._mayBeInterrupted = mayBeInterrupted;
        f._isCapped = isCapped;

        int options = QueryOption_NoCursorTimeout | QueryOption_AddHiddenPK |
            ( slaveOk ? QueryOption_SlaveOk : 0 );

        mayInterrupt( mayBeInterrupted );
        conn->query(boost::function<void(DBClientCursorBatchIterator &)>(f), from_collection, query, 0, options);

        for ( list<BSONObj>::iterator i = storedForLater.begin(); i!=storedForLater.end(); i++ ) {
            BSONObj js = *i;
            try {
                Client::WriteContext ctx(js.getStringField("ns"));
                insertObject(to_collection, js, 0, logForRepl);
            }
            catch( UserException& e ) {
                error() << "error: exception cloning object in " << from_collection << ' ' << e.what() << " obj:" << js.toString() << '\n';
                throw;
            }
        }
    }

    void Cloner::copyCollectionData(
        const string& ns, 
        const BSONObj& query,
        bool copyIndexes,
        bool logForRepl
        ) 
    {
        // main data
        copy(
            ns.c_str(), 
            ns.c_str(), 
            false, // isindex
            logForRepl, //logForRepl
            true,
            false, //maybeInterrupted
            false, // in this path, we don't set isCapped, so hidden PKs not copied
            Query(query)
            );

        if( copyIndexes ) {
            // indexes
            string dbname = getSisterNS(ns, "system.indexes");
            copy(
                dbname.c_str(),
                dbname.c_str(),
                true, //isindex
                logForRepl, //logForRepl
                true,
                false, // mayBeInterrupted
                false, // isCapped
                BSON( "ns" << ns )
                );
        }
    }

    bool Cloner::copyCollection(
        const string& ns, 
        const BSONObj& query,
        string& errmsg,
        bool copyIndexes
        ) 
    {
        {
            // config
            string temp = getSisterNS(cc().database()->name(), "system.namespaces");
            BSONObj config = conn->findOne( temp , BSON( "name" << ns ) );
            if ( config["options"].isABSONObj() ) {
                if ( !userCreateNS(
                        ns.c_str(), 
                        config["options"].Obj(), 
                        errmsg, 
                        true // logForRepl
                        ) 
                    ) 
                {
                    return false;
                }
            }
        }
        copyCollectionData(ns, query, copyIndexes, true);
        return true;
    }

    bool Cloner::go(
        const char *masterHost, 
        string& errmsg, 
        const string& fromdb, 
        bool logForRepl, 
        bool slaveOk, 
        bool useReplAuth, 
        bool mayBeInterrupted, 
        int *errCode
        )
    {

        CloneOptions opts;

        opts.fromDB = fromdb;
        opts.logForRepl = logForRepl;
        opts.slaveOk = slaveOk;
        opts.useReplAuth = useReplAuth;
        opts.mayBeInterrupted = mayBeInterrupted;

        set<string> clonedColls;
        return go( masterHost, opts, clonedColls, errmsg, errCode );

    }

    static bool checkCollectionsExist(DBClientBase &conn, const string &dbname, const vector<string> &collnames, string &errmsg);

    bool Cloner::go(
        const char *masterHost,
        const CloneOptions& opts,
        set<string>& clonedColls,
        string& errmsg,
        int* errCode
        )
    {
        if ( errCode ) {
            *errCode = 0;
        }
        massert( 10289 ,  "useReplAuth is not written to replication log", !opts.useReplAuth || !opts.logForRepl );

        string todb = cc().database()->name();
        verify(conn.get());

        /* todo: we can put these releases inside dbclient or a dbclient specialization.
           or just wait until we get rid of global lock anyway.
           */
        string ns = getSisterNS(opts.fromDB, "system.namespaces");
        list<BSONObj> toClone;
        vector<string> toCloneNames;
        clonedColls.clear();
        if ( opts.syncData ) {
            mayInterrupt( opts.mayBeInterrupted );

            // just using exhaust for collection copying right now
            auto_ptr<DBClientCursor> c = conn->query( 
                ns.c_str(), 
                BSONObj(), 
                0, 
                0, 
                0, 
                opts.slaveOk ? QueryOption_SlaveOk : 0 
                );

            if ( c.get() == 0 ) {
                errmsg = "query failed " + ns;
                return false;
            }

            if ( c->more() ) {
                BSONObj first = c->next();
                if( !getErrField(first).eoo() ) {
                    if ( errCode ) {
                        *errCode = first.getIntField("code");
                    }
                    errmsg = "query failed " + ns;
                    return false;
                }
                c->putBack( first );
            }

            while ( c->more() ) {
                BSONObj collection = c->next();

                LOG(2) << "\t cloner got " << collection << endl;

                BSONElement e = collection.getField("name");
                if ( e.eoo() ) {
                    string s = "bad system.namespaces object " + collection.toString();
                    massert( 10290 , s.c_str(), false);
                }
                verify( !e.eoo() );
                verify( e.type() == String );
                StringData from_name = e.Stringdata();

                if( NamespaceString::isSystem(from_name) ) {
                    // system.users and s.js is cloned -- but nothing else from system.
                    // * system.indexes is handled specially at the end
                    if( legalClientSystemNS( from_name , true ) == 0 ) {
                        LOG(2) << "\t\t not cloning because system collection" << endl;
                        continue;
                    }
                }
                if( ! NamespaceString::normal( from_name ) ) {
                    LOG(2) << "\t\t not cloning because has $ " << endl;
                    continue;
                }

                string from_name_str = from_name.toString();
                if( opts.collsToIgnore.find( from_name_str ) != opts.collsToIgnore.end() ){
                    LOG(2) << "\t\t ignoring collection " << from_name << endl;
                    continue;
                }
                else {
                    LOG(2) << "\t\t not ignoring collection " << from_name << endl;
                }

                clonedColls.insert( from_name_str );
                toClone.push_back( collection.getOwned() );
                toCloneNames.push_back( from_name_str );
            }
        }

        for ( list<BSONObj>::iterator i=toClone.begin(); i != toClone.end(); i++ ) {
            mayInterrupt( opts.mayBeInterrupted );
            if (!checkCollectionsExist(*conn, opts.fromDB, toCloneNames, errmsg)) {
                return false;
            }
            BSONObj collection = *i;
            LOG(2) << "  really will clone: " << collection << endl;
            const char * from_name = collection["name"].valuestr();
            BSONObj options = collection.getObjectField("options");

            /* change name "<fromdb>.collection" -> <todb>.collection */
            const char *p = strchr(from_name, '.');
            verify(p);
            string to_name = todb + p;
            bool isCapped = options["capped"].trueValue();

            {
                string err;
                const char *toname = to_name.c_str();
                userCreateNS(toname, options, err, opts.logForRepl);
            }
            LOG(1) << "\t\t cloning " << from_name << " -> " << to_name << endl;
            Query q;
            copy(
                from_name, 
                to_name.c_str(), 
                false, 
                opts.logForRepl, 
                opts.slaveOk, 
                opts.mayBeInterrupted, 
                isCapped,
                q
                );
        }

        // check that they still exists before syncing indexes
        if (!checkCollectionsExist(*conn, opts.fromDB, toCloneNames, errmsg)) {
            return false;
        }

        // now build the indexes
        
        if ( opts.syncIndexes ) {
            string system_indexes_from = getSisterNS(opts.fromDB, "system.indexes");
            string system_indexes_to = getSisterNS(todb, "system.indexes");
            
            // [dm]: is the ID index sometimes not called "_id_"?  There is 
            // other code in the system that looks for a "_id" prefix
            // rather than this exact value.  we should standardize.  
            // OR, remove names - which is in the bugdb.  Anyway, this
            // is dubious here at the moment.
            
            // build a $nin query filter for the collections we *don't* want
            BSONArrayBuilder barr;
            barr.append( opts.collsToIgnore );
            BSONArray arr = barr.arr();
            
            // Also don't copy the _id_ index
            BSONObj query = BSON("name" << NE << "_id_" << "ns" << NIN << arr);
            
            copy(
                system_indexes_from.c_str(),
                system_indexes_to.c_str(),
                true,
                opts.logForRepl,
                opts.slaveOk,
                opts.mayBeInterrupted,
                false, //isCapped
                query
                );
        }

        // check one more time at the end to make sure we got everything
        if (!checkCollectionsExist(*conn, opts.fromDB, toCloneNames, errmsg)) {
            return false;
        }
        return true;
    }

    /**
       During an initial sync, we need to periodically check whether the collections we're cloning have been dropped.
       There is also the possibility that a collection could be dropped and re-created before we get to it.
       Therefore we also try to create a cursor and check for "dictionary too new" errors.
       This command returns true iff all the collections exist and have not been created since the transaction started.
       This is only really a useful property when run in a multi-statement transaction.
     */
    class CmdCollectionsExist : public QueryCommand {
      public:
        CmdCollectionsExist() : QueryCommand("_collectionsExist") {}
        virtual void help(stringstream &h) const { h << "internal use only"; }
        virtual bool slaveOk() const { return true; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::collectionsExist);
            out->push_back(Privilege(dbname, actions));
        }
        virtual bool run(const string &dbname, BSONObj &jsobj, int, string &errmsg, BSONObjBuilder &result, bool) {
            BSONElement arrElt = jsobj["_collectionsExist"];
            if (!arrElt.ok() || arrElt.type() != Array) {
                errmsg = "argument must be an array";
                return false;
            }
            vector<BSONElement> v = arrElt.Array();
            for (vector<BSONElement>::const_iterator it = v.begin(); it != v.end(); ++it) {
                const BSONElement &e = *it;
                if (!e.ok() || e.type() != String) {
                    errmsg = "each collection name must be a string";
                    return false;
                }
                StringData ns = e.Stringdata();
                if (nsToDatabaseSubstring(ns) != dbname) {
                    errmsg = "cannot check collection in another db";
                    return false;
                }
                if (!checkCollection(ns, errmsg)) {
                    return false;
                }
            }
            return true;
        }
      private:
        bool checkCollection(const StringData &ns, string &errmsg) {
            NamespaceDetails *d;
            try {
                d = nsdetails(ns);
            }
            catch (storage::SystemException::Enoent &e) {
                d = NULL;
            }
            if (d == NULL) {
                errmsg = mongoutils::str::stream() << "collection " << ns << " was dropped";
                return false;
            }
            try {
                shared_ptr<Cursor> c(BasicCursor::make(d));
            }
            catch (storage::RetryableException::MvccDictionaryTooNew &e) {
                errmsg = mongoutils::str::stream() << "collection " << ns << " was dropped and re-created";
                return false;
            }
            return true;
        }
    } collectionsExistCommand;

    // Utility wrapper for the _collectionsExist command.
    static bool checkCollectionsExist(DBClientBase &conn, const string &dbname, const vector<string> &collnames, string &errmsg) {
        BSONObj res;
        bool ok = conn.runCommand(dbname, BSON("_collectionsExist" << collnames), res);
        if (!ok) {
            errmsg = res["errmsg"].String();
            LOG(1) << errmsg << endl;
        }
        return ok;
    }

    bool cloneFrom( 
        const string& masterHost , 
        const CloneOptions& options , 
        shared_ptr<DBClientBase> conn,
        string& errmsg /* out */
        ) 
    {
        set<string> clonedCollections;
        
        Cloner c(conn);
        return c.go(
            masterHost.c_str(),
            options,
            clonedCollections,
            errmsg,
            NULL
            );
    }

    void cloneCollectionData(
        shared_ptr<DBClientBase> conn,
        const string& ns, 
        const BSONObj& query,
        bool copyIndexes,
        bool logForRepl
        ) 
    {
        Cloner c(conn);
        c.copyCollectionData(
            ns,
            query,
            copyIndexes,
            logForRepl
            );
    }

    /* Usage:
       mydb.$cmd.findOne( { clone: "fromhost" } );
       Note: doesn't work with authentication enabled, except as internal operation or for
       old-style users for backwards compatibility.
    */
    class CmdClone : public Command {
    public:
        virtual bool slaveOk() const {
            return false;
        }
        virtual LockType locktype() const { return WRITE; }
        virtual bool needsTxn() const { return true; }
        virtual int txnFlags() const { return DB_SERIALIZABLE; }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual bool requiresSync() const { return true; }
        virtual void help( stringstream &help ) const {
            help << "clone this database from an instance of the db on another host\n";
            help << "{ clone : \"host13\" }";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            // Note: privileges required are currently only granted to old-style users for backwards
            // compatibility, and to internal connections (used in movePrimary).
            ActionSet actions;
            actions.addAction(ActionType::clone);
            out->push_back(Privilege(dbname, actions));
        }
        CmdClone() : Command("clone") { }
        
        virtual bool run(
            const string& dbname,
            BSONObj& cmdObj, 
            int, 
            string& errmsg,
            BSONObjBuilder& result,
            bool fromRepl
            ) 
        {
            verify(!fromRepl);
            string from = cmdObj.getStringField("clone");
            if ( from.empty() )
                return false;

            CloneOptions opts;
            opts.fromDB = dbname;
            opts.logForRepl = true;

            // See if there's any collections we should ignore
            if( cmdObj["collsToIgnore"].type() == Array ){
                BSONObjIterator it( cmdObj["collsToIgnore"].Obj() );

                while( it.more() ){
                    BSONElement e = it.next();
                    if( e.type() == String ){
                        opts.collsToIgnore.insert( e.String() );
                    }
                }
            }

            // check if the input parameters are asking for a self-clone
            // if so, gracefully exit
            if (!checkSelfClone(from.c_str(), dbname, errmsg)) {
                return false;
            }

            shared_ptr<DBClientBase> sconn;
            scoped_ptr<RemoteTransaction> rtxn;
            if (masterSameProcess(from.c_str())) {
                sconn = boost::make_shared<DBDirectClient>();
            }
            else {
                sconn = makeConnection(from.c_str(), errmsg);
                if (!sconn) {
                    return false;
                }
                // since this is a remote connection, we should
                // create a multi statement transaction for the work
                try {
                    rtxn.reset(new RemoteTransaction(*sconn, "mvcc"));
                }
                catch (DBException &e) {
                    errmsg = "unable to begin transaction over connection";
                    return false;
                }
            }
            verify(sconn);
            Cloner c(sconn);

            set<string> clonedColls;
            bool rval = c.go( from.c_str(), opts, clonedColls, errmsg );

            if (rval && rtxn) {
                bool ok = rtxn->commit();
                verify(ok);
            }            

            BSONArrayBuilder barr;
            barr.append( clonedColls );

            result.append( "clonedColls", barr.arr() );

            return rval;

        }
    } cmdClone;

    class CmdCloneCollection : public Command {
    public:
        virtual bool slaveOk() const {
            return false;
        }
        virtual LockType locktype() const { return OPLOCK; }
        virtual bool needsTxn() const { return false; }
        virtual int txnFlags() const { return noTxnFlags(); }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual bool requiresSync() const { return true; }
        CmdCloneCollection() : Command("cloneCollection") { }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            // Will fail if source instance has auth on.
            string collection = cmdObj.getStringField("cloneCollection");
            uassert(16709, "bad 'cloneCollection' value", !collection.empty());

            ActionSet actions;
            actions.addAction(ActionType::cloneCollectionTarget);
            out->push_back(Privilege(collection, actions));
        }
        virtual void help( stringstream &help ) const {
            help << "{ cloneCollection: <collection>, from: <host> [,query: <query_filter>] [,copyIndexes:<bool>] }"
                 "\nCopies a collection from one server to another. Do not use on a single server as the destination "
                 "is placed at the same db.collection (namespace) as the source.\n"
                 ;
        }
        virtual bool run(
            const string& dbname,
            BSONObj& cmdObj,
            int,
            string& errmsg,
            BSONObjBuilder& result,
            bool fromRepl
            )
        {
            verify(!fromRepl);
            string fromhost = cmdObj.getStringField("from");
            if ( fromhost.empty() ) {
                errmsg = "missing 'from' parameter";
                return false;
            }
            {
                HostAndPort h(fromhost);
                if( h.isSelf() ) {
                    errmsg = "can't cloneCollection from self";
                    return false;
                }
            }
            string collection = cmdObj.getStringField("cloneCollection");
            if ( collection.empty() ) {
                errmsg = "bad 'cloneCollection' value";
                return false;
            }
            BSONObj query = cmdObj.getObjectField("query");
            if ( query.isEmpty() ) {
                query = BSONObj();
            }

            BSONElement copyIndexesSpec = cmdObj.getField("copyindexes");
            bool copyIndexes = copyIndexesSpec.isBoolean() ? 
                copyIndexesSpec.boolean() : 
                true;

            log() << "cloneCollection.  db:" << dbname << " collection:" <<
                collection << " from: " << fromhost << " query: " << query <<
                " " << ( copyIndexes ? "" : ", not copying indexes" ) <<
                endl;

            shared_ptr<DBClientConnection> myconn = boost::make_shared<DBClientConnection>();
            if (!myconn->connect(fromhost, errmsg)) {
                return false;
            }
            shared_ptr<DBClientBase> conn = myconn;
            
            RemoteTransaction rtxn(*conn, "mvcc");

            Client::WriteContext ctx(collection);
            Client::Transaction txn(DB_SERIALIZABLE);
            
            Cloner c(conn);
            bool retval = c.copyCollection(
                collection,
                query,
                errmsg,
                copyIndexes
                );

            if (retval) {
                txn.commit();
                bool ok = rtxn.commit();
                verify(ok);
            }

            return retval;
        }
    } cmdCloneCollection;


    /* Usage:
     admindb.$cmd.findOne( { copydbgetnonce: 1, fromhost: <hostname> } );
     */
    class CmdCopyDbGetNonce : public Command {
    public:
        CmdCopyDbGetNonce() : Command("copydbgetnonce") { }
        virtual bool adminOnly() const {
            return true;
        }
        virtual bool slaveOk() const {
            return false;
        }
        virtual LockType locktype() const { return NONE; }
        virtual bool requiresSync() const { return false; }
        virtual bool needsTxn() const { return false; }
        virtual int txnFlags() const { return noTxnFlags(); }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {} // No auth required
        virtual void help( stringstream &help ) const {
            help << "get a nonce for subsequent copy db request from secure server\n";
            help << "usage: {copydbgetnonce: 1, fromhost: <hostname>}";
        }
        virtual bool run(
            const string&,
            BSONObj& cmdObj,
            int,
            string& errmsg,
            BSONObjBuilder& result,
            bool fromRepl
            )
        {
            verify(!fromRepl);
            string fromhost = cmdObj.getStringField("fromhost");
            if ( fromhost.empty() ) {
                /* copy from self */
                stringstream ss;
                ss << "localhost:" << cmdLine.port;
                fromhost = ss.str();
            }
            shared_ptr<DBClientConnection> newConn( new DBClientConnection() );
            cc().setAuthConn(newConn);
            
            BSONObj ret;
            if (!cc().authConn()->connect(fromhost, errmsg)) {
                return false;
            }
            if (!cc().authConn()->runCommand("admin", BSON("getnonce" << 1), ret)) {
                errmsg = "couldn't get nonce " + ret.toString();
                return false;
            }
            result.appendElements( ret );
            return true;
        }
    } cmdCopyDBGetNonce;

    /* Usage:
       admindb.$cmd.findOne( { copydb: 1, fromhost: <hostname>, fromdb: <db>, todb: <db>[, username: <username>, nonce: <nonce>, key: <key>] } );
       Note: doesn't work with authentication enabled, except as old-style users.
    */
    class CmdCopyDb : public Command {
    public:
        CmdCopyDb() : Command("copydb") { }
        virtual bool adminOnly() const {
            return true;
        }
        virtual bool slaveOk() const {
            return false;
        }
        virtual LockType locktype() const { return OPLOCK; }
        virtual bool needsTxn() const { return false; }
        virtual int txnFlags() const { return noTxnFlags(); }
        virtual bool canRunInMultiStmtTxn() const { return true; }
        virtual bool requiresSync() const { return true; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            // Note: privileges required are currently only granted to old-style users for backwards
            // compatibility, since we can't properly handle auth checking for the read from the
            // source DB.
            ActionSet actions;
            actions.addAction(ActionType::copyDBTarget);
            out->push_back(Privilege(dbname, actions)); // NOTE: dbname is always admin
        }
        virtual void help( stringstream &help ) const {
            help << "copy a database from another host to this host\n";
            help << "usage: {copydb: 1, fromhost: <hostname>, fromdb: <db>, todb: <db>[, slaveOk: <bool>, username: <username>, nonce: <nonce>, key: <key>]}";
        }
        virtual bool run(
            const string& dbname,
            BSONObj& cmdObj,
            int,
            string& errmsg,
            BSONObjBuilder& result,
            bool fromRepl
            ) 
        {
            // clone command should not be logged,
            // and therefore, fromRepl should be false.
            verify(!fromRepl);
            bool slaveOk = cmdObj["slaveOk"].trueValue();
            string fromhost = cmdObj.getStringField("fromhost");
            bool fromSelf = fromhost.empty();
            if ( fromSelf ) {
                /* copy from self */
                stringstream ss;
                ss << "localhost:" << cmdLine.port;
                fromhost = ss.str();
            }
            string fromdb = cmdObj.getStringField("fromdb");
            string todb = cmdObj.getStringField("todb");
            if ( fromhost.empty() || todb.empty() || fromdb.empty() ) {
                errmsg = "parms missing - {copydb: 1, fromhost: <hostname>, fromdb: <db>, todb: <db>}";
                return false;
            }

            scoped_ptr<Lock::ScopedLock> lk;
            shared_ptr<DBClientBase> conn;
            scoped_ptr<RemoteTransaction> rtxn;
            string username = cmdObj.getStringField( "username" );
            string nonce = cmdObj.getStringField( "nonce" );
            string key = cmdObj.getStringField( "key" );
            if ( !username.empty() && !nonce.empty() && !key.empty() ) {
                uassert( 13008, "must call copydbgetnonce first", cc().authConn().get() );
                BSONObj ret;
                BSONObj command = BSON( 
                    "authenticate" << 1 << 
                    "user" << username << 
                    "nonce" << nonce << 
                    "key" << key
                                        );
                if ( !cc().authConn()->runCommand( fromdb, command, ret ) ) {
                    errmsg = "unable to login " + ret.toString();
                    return false;
                }

                lk.reset(static_cast<Lock::ScopedLock *>(new Lock::DBWrite(todb)));
                conn = cc().authConn();
                // we are not using a direct client, so we should
                // create a multi statement transaction for the work
                try {
                    rtxn.reset(new RemoteTransaction(*conn, "mvcc"));
                }
                catch (DBException &e) {
                    errmsg = "unable to begin transaction over connection";
                    return false;
                }
            }
            else {
                {
                    Client::ReadContext rctx(todb); // this is annoying, checkSelfClone needs cc().database()
                    // check if the input parameters are asking for a self-clone
                    // if so, gracefully exit
                    if (!checkSelfClone(fromhost.c_str(), fromdb, errmsg)) {
                        return false;
                    }
                }

                if (masterSameProcess(fromhost.c_str())) {
                    // SERVER-4328 todo lock just the two db's not everything for the fromself case
                    lk.reset(static_cast<Lock::ScopedLock *>(new Lock::GlobalWrite()));
                    conn = boost::make_shared<DBDirectClient>();
                }
                else {
                    lk.reset(static_cast<Lock::ScopedLock *>(new Lock::DBWrite(todb)));
                    conn = makeConnection(fromhost.c_str(), errmsg);
                    if (!conn) {
                        // errmsg should be set
                        return false;
                    }
                    // we are not using a direct client, so we should
                    // create a multi statement transaction for the work
                    try {
                        rtxn.reset(new RemoteTransaction(*conn, "mvcc"));
                    }
                    catch (DBException &e) {
                        errmsg = "unable to begin transaction over connection";
                        return false;
                    }
                }
            }
            Client::Context tc(todb);
            Client::Transaction txn(DB_SERIALIZABLE);
            verify(conn);
            Cloner c(conn);
            bool res = c.go(
                fromhost.c_str(),
                errmsg,
                fromdb,
                true, /*logForReplication=*/
                slaveOk,
                false, /*replauth*/
                false /*mayBeInterrupted*/
                );
            if (res) {
                if (rtxn) {
                    rtxn->commit();
                }
                txn.commit();
            }
            shared_ptr<DBClientConnection> emptyConn;
            cc().setAuthConn(emptyConn);
            return res;
        }
    } cmdCopyDB;

} // namespace mongo
