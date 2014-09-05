// dump.cpp

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

#include <fcntl.h>
#include <map>
#include <fstream>

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/convenience.hpp>

#include "mongo/client/dbclientcursor.h"
#include "mongo/db/namespacestring.h"
#include "mongo/tools/mongodump_options.h"
#include "mongo/tools/tool.h"
#include "mongo/tools/tool_options.h"
#include "mongo/util/options_parser/option_section.h"

using namespace mongo;

class Dump : public Tool {
    class FilePtr : boost::noncopyable {
    public:
        /*implicit*/ FilePtr(FILE* f) : _f(f) {}
        ~FilePtr() { fclose(_f); }
        operator FILE*() { return _f; }
    private:
        FILE* _f;
    };
public:
    Dump() : Tool() { }

    virtual void printHelp(ostream& out) {
        printMongoDumpHelp(&out);
    }

    // This is a functor that writes a BSONObj to a file
    struct Writer {
        Writer(FILE* out, ProgressMeter* m) :_out(out), _m(m) {}

        void operator () (const BSONObj& obj) {
            size_t toWrite = obj.objsize();
            size_t written = 0;

            while (toWrite) {
                size_t ret = fwrite( obj.objdata()+written, 1, toWrite, _out );
                uassert(14035, errnoWithPrefix("couldn't write to file"), ret);
                toWrite -= ret;
                written += ret;
            }

            // if there's a progress bar, hit it
            if (_m) {
                _m->hit();
            }
        }

        FILE* _out;
        ProgressMeter* _m;
    };

    void doCollection( const string coll , FILE* out , ProgressMeter *m ) {
        Query q = _query;

        int queryOptions = QueryOption_SlaveOk | QueryOption_NoCursorTimeout;
        if (startsWith(coll.c_str(), "local.oplog.")) {
            queryOptions |= QueryOption_OplogReplay;
        }
        
        DBClientBase& connBase = conn(true);
        Writer writer(out, m);

        // use low-latency "exhaust" mode if going over the network
        if (!_usingMongos && typeid(connBase) == typeid(DBClientConnection&)) {
            DBClientConnection& conn = static_cast<DBClientConnection&>(connBase);
            boost::function<void(const BSONObj&)> castedWriter(writer); // needed for overload resolution
            conn.query( castedWriter, coll.c_str() , q , NULL, queryOptions | QueryOption_Exhaust);
        }
        else {
            //This branch should only be taken with DBDirectClient or mongos which doesn't support exhaust mode
            scoped_ptr<DBClientCursor> cursor(connBase.query( coll.c_str() , q , 0 , 0 , 0 , queryOptions ));
            while ( cursor->more() ) {
                writer(cursor->next());
            }
        }
    }

    void writeCollectionFile( const string coll , boost::filesystem::path outputFile ) {
        toolInfoLog() << "\t" << coll << " to " << outputFile.string() << std::endl;

        FilePtr f (fopen(outputFile.string().c_str(), "wb"));
        uassert(10262, errnoWithPrefix("couldn't open file"), f);

        ProgressMeter m(conn(true).count(coll.c_str(), BSONObj(), QueryOption_SlaveOk));
        m.setName("Collection File Writing Progress");
        m.setUnits("objects");

        doCollection(coll, f, &m);

        toolInfoLog() << "\t\t " << m.done() << " objects" << std::endl;
    }

    void writeMetadataFile( const string coll, boost::filesystem::path outputFile, 
                            map<string, BSONObj> options, multimap<string, BSONObj> indexes, map<string, BSONObj> partitionInfo) {
        toolInfoLog() << "\tMetadata for " << coll << " to " << outputFile.string() << std::endl;

        bool hasOptions = options.count(coll) > 0;
        bool hasIndexes = indexes.count(coll) > 0;
        bool hasPartitionInfo = partitionInfo.count(coll) > 0;

        BSONObjBuilder metadata;

        if (hasOptions) {
            metadata << "options" << options.find(coll)->second;
        }

        if (hasIndexes) {
            BSONArrayBuilder indexesOutput (metadata.subarrayStart("indexes"));

            // I'd kill for C++11 auto here...
            const pair<multimap<string, BSONObj>::iterator, multimap<string, BSONObj>::iterator>
                range = indexes.equal_range(coll);

            for (multimap<string, BSONObj>::iterator it=range.first; it!=range.second; ++it) {
                 indexesOutput << it->second;
            }

            indexesOutput.done();
        }
        if (hasPartitionInfo) {
            metadata << "partitionInfo" << partitionInfo.find(coll)->second;
        }

        ofstream file (outputFile.string().c_str());
        uassert(15933, "Couldn't open file: " + outputFile.string(), file.is_open());
        file << metadata.done().jsonString();
    }



    void writeCollectionStdout( const string coll ) {
        doCollection(coll, stdout, NULL);
    }

    void go( const string db , const boost::filesystem::path outdir ) {
        toolInfoLog() << "DATABASE: " << db << "\t to \t" << outdir.string() << std::endl;

        boost::filesystem::create_directories( outdir );

        map <string, BSONObj> collectionOptions;
        map <string, BSONObj> partitionInfo;
        multimap <string, BSONObj> indexes;
        vector <string> collections;

        // Save indexes for database
        string ins = db + ".system.indexes";
        auto_ptr<DBClientCursor> cursor = conn( true ).query( ins.c_str() , Query() , 0 , 0 , 0 , QueryOption_SlaveOk | QueryOption_NoCursorTimeout );
        while ( cursor->more() ) {
            BSONObj obj = cursor->nextSafe();
            const string name = obj.getField( "ns" ).valuestr();
            indexes.insert( pair<string, BSONObj> (name, obj.getOwned()) );
        }

        string sns = db + ".system.namespaces";
        cursor = conn( true ).query( sns.c_str() , Query() , 0 , 0 , 0 , QueryOption_SlaveOk | QueryOption_NoCursorTimeout );
        while ( cursor->more() ) {
            BSONObj obj = cursor->nextSafe();
            const string name = obj.getField( "name" ).valuestr();
            if (obj.hasField("options")) {
                BSONObj options = obj.getField("options").embeddedObject().getOwned();
                if (options["partitioned"].trueValue()) {
                    BSONObj res;
                    StringData collectionName = nsToCollectionSubstring(name);
                    bool ok = conn(true).runCommand(db, BSON("getPartitionInfo" << collectionName), res);
                    uassert(17354, str::stream() << "Could not get partition information for " << name, ok);
                    partitionInfo[name] = res;
                }
                collectionOptions[name] = options;
            }

            // skip namespaces with $ in them only if we don't specify a collection to dump
            if (toolGlobalParams.coll.empty() && name.find(".$") != string::npos) {
                if (logger::globalLogDomain()->shouldLog(logger::LogSeverity::Debug(1))) {
                    toolInfoLog() << "\tskipping collection: " << name << std::endl;
                }
                continue;
            }

            const string filename = name.substr( db.size() + 1 );

            //if a particular collections is specified, and it's not this one, skip it
            if (toolGlobalParams.coll.empty() &&
                db + "." + toolGlobalParams.coll != name &&
                toolGlobalParams.coll != name) {
                continue;
            }

            // raise error before writing collection with non-permitted filename chars in the name
            size_t hasBadChars = name.find_first_of("/\0");
            if (hasBadChars != string::npos){
                toolError() << "Cannot dump "  << name
                          << ". Collection has '/' or null in the collection name." << std::endl;
                continue;
            }

            if (nsToCollectionSubstring(name) == "system.indexes") {
              // Create system.indexes.bson for compatibility with pre 2.2 mongorestore
              writeCollectionFile( name.c_str() , outdir / ( filename + ".bson" ) );
              // Don't dump indexes as *.metadata.json
              continue;
            }
            
            collections.push_back(name);
        }
        
        for (vector<string>::iterator it = collections.begin(); it != collections.end(); ++it) {
            string name = *it;
            const string filename = name.substr( db.size() + 1 );
            writeCollectionFile( name , outdir / ( filename + ".bson" ) );
            writeMetadataFile( name, outdir / (filename + ".metadata.json"), collectionOptions, indexes, partitionInfo);
        }

    }

    int repair() {
        if (toolGlobalParams.dbpath.empty()) {
            toolInfoLog() << "repair mode only works with --dbpath" << std::endl;
            return -1;
        }

        if (toolGlobalParams.db.empty()) {
            toolInfoLog() << "repair mode only works on 1 db at a time right now" << std::endl;
            return -1;
        }

        toolInfoLog() << "going to try and recover data from: " << toolGlobalParams.db << std::endl;

        return _repair(toolGlobalParams.db);
    }    

    int _repair( string dbname ) {
        return 0;
    }

    int run() {
        if (mongoDumpGlobalParams.repair) {
            toolError() << "repair is a work in progress" << std::endl;
            return repair();
        }

        {
            if (mongoDumpGlobalParams.query.size()) {
                _query = fromjson(mongoDumpGlobalParams.query);
            }
        }

        string opLogName = "";
        unsigned long long opLogStart = 0;
        if (mongoDumpGlobalParams.useOplog) {

            BSONObj isMaster;
            conn("true").simpleCommand("admin", &isMaster, "isMaster");

            if (isMaster.hasField("hosts")) { // if connected to replica set member
                opLogName = "local.oplog.rs";
            }
            else {
                opLogName = "local.oplog.$main";
                if ( ! isMaster["ismaster"].trueValue() ) {
                    toolError() << "oplog mode is only supported on master or replica set member"
                              << std::endl;
                    return -1;
                }
            }

            BSONObj op = conn(true).findOne(opLogName, Query().sort("$natural", -1), 0, QueryOption_SlaveOk);
            if (op.isEmpty()) {
                toolError() << "No operations in oplog. Please ensure you are connecting to a "
                            << "master." << std::endl;
                return -1;
            }

            opLogStart = op["ts"]._numberLong();
        }

        // check if we're outputting to stdout
        if (mongoDumpGlobalParams.outputFile == "-") {
            if (toolGlobalParams.db != "" && toolGlobalParams.coll != "") {
                writeCollectionStdout(toolGlobalParams.db + "." + toolGlobalParams.coll);
                return 0;
            }
            else {
                toolError() << "You must specify database and collection to print to stdout"
                          << std::endl;
                return -1;
            }
        }

        _usingMongos = isMongos();

        boost::filesystem::path root(mongoDumpGlobalParams.outputFile);

        if (toolGlobalParams.db == "") {
            if (toolGlobalParams.coll != "") {
                toolError() << "--db must be specified with --collection" << std::endl;
                return -1;
            }

            toolInfoLog() << "all dbs" << std::endl;

            BSONObj res = conn( true ).findOne( "admin.$cmd" , BSON( "listDatabases" << 1 ) );
            if ( ! res["databases"].isABSONObj() ) {
                toolError() << "output of listDatabases isn't what we expected, no 'databases' "
                          << "field:\n" << res << std::endl;
                return -2;
            }
            BSONObj dbs = res["databases"].embeddedObjectUserCheck();
            set<string> keys;
            dbs.getFieldNames( keys );
            for ( set<string>::iterator i = keys.begin() ; i != keys.end() ; i++ ) {
                string key = *i;
                
                if ( ! dbs[key].isABSONObj() ) {
                    toolError() << "database field not an object key: " << key << " value: "
                              << dbs[key] << std::endl;
                    return -3;
                }

                BSONObj dbobj = dbs[key].embeddedObjectUserCheck();

                const char * dbName = dbobj.getField( "name" ).valuestr();
                if ( (string)dbName == "local" )
                    continue;

                go ( dbName , root / dbName );
            }
        }
        else {
            go(toolGlobalParams.db, root / toolGlobalParams.db);
        }

        if (!opLogName.empty()) {
            BSONObjBuilder b;
            b.appendDate("$gt", opLogStart);

            _query = BSON("ts" << b.obj());

            writeCollectionFile( opLogName , root / "oplog.bson" );
        }

        return 0;
    }

    bool _usingMongos;
    BSONObj _query;
};

REGISTER_MONGO_TOOL(Dump);
