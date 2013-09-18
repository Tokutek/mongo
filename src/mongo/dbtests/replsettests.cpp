// replsettests.cpp : Unit tests for replica sets
//

/**
 *    Copyright (C) 2009 10gen Inc.
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
#include "mongo/db/repl.h"
#include "mongo/db/instance.h"
#include "mongo/db/json.h"
#include "mongo/db/oplog.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/repl/bgsync.h"
#include "mongo/db/repl/replication_server_status.h"  // replSettings
#include "mongo/db/repl/rs.h"
#include "mongo/dbtests/dbtests.h"

namespace mongo {
    void createOplog();
}

namespace ReplSetTests {
    const int replWriterThreadCount(32);
    const int replPrefetcherThreadCount(32);
    class ReplSetTest : public ReplSet {
        ReplSetConfig *_config;
        ReplSetConfig::MemberCfg *_myConfig;
        //ckgroundSyncInterface *_syncTail;
    public:
        static const int replWriterThreadCount;
        static const int replPrefetcherThreadCount;
        static ReplSetTest* make() {
            auto_ptr<ReplSetTest> ret(new ReplSetTest());
            ret->init();
            return ret.release();
        }
        virtual ~ReplSetTest() {
            delete _myConfig;
            delete _config;
        }
        virtual bool isSecondary() {
            return true;
        }
        virtual bool isPrimary() {
            return false;
        }
        virtual bool tryToGoLiveAsASecondary() {
            return false;
        }
        virtual const ReplSetConfig& config() {
            return *_config;
        }
        virtual const ReplSetConfig::MemberCfg& myConfig() {
            return *_myConfig;
        }
        virtual bool buildIndexes() const {
            return true;
        }
        /*
        void setSyncTail(BackgroundSyncInterface *syncTail) {
            _syncTail = syncTail;
        }
        */
    private:
        ReplSetTest()
            {
        }
        void init() {
            BSONArrayBuilder members;
            members.append(BSON("_id" << 0 << "host" << "host1"));
            _config = ReplSetConfig::make(BSON("_id" << "foo" << "members" << members.arr()));
            _myConfig = new ReplSetConfig::MemberCfg();
        }
    };

    /*
    class BackgroundSyncTest : public BackgroundSyncInterface {
        std::queue<BSONObj> _queue;
    public:
        BackgroundSyncTest() {}
        virtual ~BackgroundSyncTest() {}
        virtual bool peek(BSONObj* op) {
            if (_queue.empty()) {
                return false;
            }
            *op = _queue.front();
            return true;
        }
        virtual void consume() {
            _queue.pop();
        }
        virtual Member* getSyncTarget() {
            return 0;
        }
        void addDoc(BSONObj doc) {
            _queue.push(doc.getOwned());
        }
        virtual void waitForMore() {
            return;
        }
    };
*/

    class Base {
    private:
        static DBDirectClient client_;
    protected:
        //BackgroundSyncTest* _bgsync;
        //SyncTail* _tailer;
    public:
        Base() {
            replSettings.replSet = "foo";
            createOplog();
            setup();
        }
        ~Base() {
            //delete _bgsync;
            //delete _tailer;
        }

        static const char *ns() {
            return "unittests.repltests";
        }

        DBDirectClient *client() const { return &client_; }

        static void insert( const BSONObj &o, bool god = false ) {
            Lock::DBWrite lk(ns(), mongo::unittest::EMPTY_STRING);
            Client::Context ctx( ns() );
            insertObject( ns(), o );
        }

        BSONObj findOne( const BSONObj &query = BSONObj() ) const {
            return client()->findOne( ns(), query );
        }

        void drop() {
            Client::WriteContext c(ns(), mongo::unittest::EMPTY_STRING);
            string errmsg;
            BSONObjBuilder result;

            Collection *d = getCollection(ns());
            if (d != NULL) {
                d->drop(errmsg, result);
                return;
            }
        }
        void setup() {
            replSettings.replSet = "foo";

            // setup background sync instance
            //_bgsync = new BackgroundSyncTest();

            // setup tail
            //_tailer = new SyncTail(_bgsync);

            // setup theReplSet
            ReplSetTest *rst = ReplSetTest::make();
            //rst->setSyncTail(_bgsync);
            delete theReplSet;
            theReplSet = rst;
        }
    };

    DBDirectClient Base::client_;

    class CappedInitialSync : public Base {
        string _cappedNs;
        Lock::DBWrite _lk;

        string spec() const {
            return "{\"capped\":true,\"size\":512}";
        }

        void create() {
            Client::Context c(_cappedNs);
            string err;
            ASSERT(userCreateNS( _cappedNs.c_str(), fromjson( spec() ), err, false ));
        }

        void dropCapped() {
            Client::Context c(_cappedNs);
            Collection *d = getCollection(_cappedNs);
            if (d != NULL) {
                string errmsg;
                BSONObjBuilder result;
                d->drop(errmsg, result);
                return;
            }
        }

        BSONObj updateFail() {
            BSONObjBuilder b;
            {
                mongo::mutex::scoped_lock lk2(OpTime::m);
                b.appendTimestamp("ts", OpTime::now(lk2).asLL());
            }
            b.append("op", "u");
            b.append("o", BSON("$set" << BSON("x" << 456)));
            b.append("o2", BSON("_id" << 123 << "x" << 123));
            b.append("ns", _cappedNs);
            BSONObj o = b.obj();

            verify(!apply(o));
            return o;
        }
    public:
        CappedInitialSync() : _cappedNs("unittests.foo.bar"), _lk(_cappedNs, mongo::unittest::EMPTY_STRING) {
            dropCapped();
            create();
        }
        virtual ~CappedInitialSync() {
            dropCapped();
        }

        string& cappedNs() {
            return _cappedNs;
        }

        // returns true on success, false on failure
        bool apply(const BSONObj& op) {
            Client::Context ctx( _cappedNs );
            ::abort();
            // in an annoying twist of api, returns true on failure
            return true;
        }

        void run() {
            Lock::DBWrite lk(_cappedNs, mongo::unittest::EMPTY_STRING);

            BSONObj op = updateFail();

            //Sync s("");
            // TODO: decide if this ought to be restored one day
            //verify(!s.shouldRetry(op));
        }
    };

    class CappedUpdate : public CappedInitialSync {
        void updateSucceed() {
            BSONObjBuilder b;
            {
                mongo::mutex::scoped_lock lk2(OpTime::m);
                b.appendTimestamp("ts", OpTime::now(lk2).asLL());
            }
            b.append("op", "u");
            b.append("o", BSON("$set" << BSON("x" << 789)));
            b.append("o2", BSON("x" << 456));
            b.append("ns", cappedNs());

            verify(apply(b.obj()));
        }

        void insert() {
            Client::Context ctx( cappedNs() );
            BSONObj o = BSON(GENOID << "x" << 456);
            insertObject( cappedNs().c_str(), o );
        }
    public:
        virtual ~CappedUpdate() {}
        void run() {
            // RARELY shoud be once/128x
            for (int i=0; i<150; i++) {
                insert();
                updateSucceed();
            }

            DBDirectClient client;
            int count = (int) client.count(cappedNs(), BSONObj());
            verify(count > 1);

            // check _id index created
            Client::Context ctx(cappedNs());
            Collection *nsd = getCollection(cappedNs());
            verify(nsd->findIdIndex() > -1);
        }
    };

    class CappedInsert : public CappedInitialSync {
        void insertSucceed() {
            BSONObjBuilder b;
            {
                mongo::mutex::scoped_lock lk2(OpTime::m);
                b.appendTimestamp("ts", OpTime::now(lk2).asLL());
            }
            b.append("op", "i");
            b.append("o", BSON("_id" << 123 << "x" << 456));
            b.append("ns", cappedNs());
            verify(apply(b.obj()));
        }
    public:
        virtual ~CappedInsert() {}
        void run() {
            // This will succeed, but not insert anything because they are changed to upserts
            for (int i=0; i<150; i++) {
                insertSucceed();
            }

            // this changed in 2.1.2
            // we now have indexes on capped collections
            Client::Context ctx(cappedNs());
            Collection *nsd = getCollection(cappedNs());
            verify(nsd->findIdIndex() >= 0);
        }
    };

    class TestRSSync : public Base {

        void addOp(const string& op, BSONObj o, BSONObj* o2 = NULL, const char* coll = NULL, 
                   int version = 0) {
            OpTime ts;
            {
                Lock::GlobalWrite lk(mongo::unittest::EMPTY_STRING);
                ts = OpTime::_now();
            }

            BSONObjBuilder b;
            b.appendTimestamp("ts", ts.asLL());
            if (version != 0) {
                b.append("v", version);
            }
            b.append("op", op);
            b.append("o", o);

            if (o2) {
                b.append("o2", *o2);
            }

            if (coll) {
                b.append("ns", coll);
            }
            else {
                b.append("ns", ns());
            }

            //_bgsync->addDoc(b.done());
        }

        void addInserts(int expected) {
            for (int i=0; i<expected; i++) {
                addOp("i", BSON("_id" << i << "x" << 789));
            }
        }

        void addVersionedInserts(int expected) {
            for (int i=0; i < expected; i++) {
                addOp("i", BSON("_id" << i << "x" << 789), NULL, NULL, i);
            }
        }
            
        void addUpdates() {
            BSONObj id = BSON("_id" << "123456something");
            addOp("i", id);

            addOp("u", BSON("$set" << BSON("requests.1000001_2" << BSON(
                    "id" << "1000001_2" <<
                    "timestamp" << 1334813340))), &id);

            addOp("u", BSON("$set" << BSON("requests.1000002_2" << BSON(
                    "id" << "1000002_2" <<
                    "timestamp" << 1334813368))), &id);

            addOp("u", BSON("$set" << BSON("requests.100002_1" << BSON(
                    "id" << "100002_1" <<
                    "timestamp" << 1334810820))), &id);
        }

        void addConflictingUpdates() {
            BSONObj first = BSON("_id" << "asdfasdfasdf");
            addOp("i", first);

            BSONObj filter = BSON("_id" << "asdfasdfasdf" << "sp" << BSON("$size" << 2));
            // Test an op with no version, op is ignored and replication continues (code assumes 
            // version 1)
            addOp("u", BSON("$push" << BSON("sp" << 42)), &filter, NULL, 0);
            // The following line generates an fassert because it's version 2
            //addOp("u", BSON("$push" << BSON("sp" << 42)), &filter, NULL, 2);
        }

        void addUniqueIndex() {
            addOp("i", BSON("ns" << ns() << "key" << BSON("x" << 1) << "name" << "x1" << "unique" << true), 0, "unittests.system.indexes");
            addInserts(2);
        }

        void applyOplog() {
            //_tailer->oplogApplication();
        }
    public:
        void run() {
            const int expected = 100;

            drop();
            addInserts(100);
            applyOplog();

            ASSERT_EQUALS(expected, static_cast<int>(client()->count(ns())));

            drop();
            addVersionedInserts(100);
            applyOplog();

            ASSERT_EQUALS(expected, static_cast<int>(client()->count(ns())));

            drop();
            addUpdates();
            applyOplog();

            BSONObj obj = findOne();

            ASSERT_EQUALS(1334813340, obj["requests"]["1000001_2"]["timestamp"].number());
            ASSERT_EQUALS(1334813368, obj["requests"]["1000002_2"]["timestamp"].number());
            ASSERT_EQUALS(1334810820, obj["requests"]["100002_1"]["timestamp"].number());

            drop();
            
            // test converting updates to upserts but only for version 2.2.1 and greater,
            // which means oplog version 2 and greater.
            addConflictingUpdates();
            applyOplog();

            drop();

        }
    };

    class All : public Suite {
    public:
        All() : Suite( "replset" ) {
        }

        void setupTests() {
            LOG(0) << "replication tests disabled" << endl;
#if 0
            add< TestInitApplyOp >();
            add< TestInitApplyOp2 >();
            add< CappedInitialSync >();
            add< CappedUpdate >();
            add< CappedInsert >();
            add< TestRSSync >();
#endif
        }
    } myall;
}
