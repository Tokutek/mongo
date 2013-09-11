// @file restore.cpp

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

#include <boost/filesystem/convenience.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/scoped_ptr.hpp>
#include <fcntl.h>
#include <fstream>
#include <set>

#include "mongo/client/dbclientcursor.h"
#include "mongo/client/remote_loader.h"
#include "mongo/db/json.h"
#include "mongo/db/namespacestring.h"
#include "mongo/tools/mongorestore_options.h"
#include "mongo/tools/tool.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/stringutils.h"

using namespace mongo;

class Restore : public BSONTool {
public:

    string _curns;
    string _curdb;
    string _curcoll;
    set<string> _users; // For restoring users with --drop

    Restore() : BSONTool() { }

    virtual void printHelp(ostream& out) {
        printMongoRestoreHelp(toolsOptions, &out);
    }

    virtual int doRun() {

        boost::filesystem::path root = mongoRestoreGlobalParams.restoreDirectory;

        // check if we're actually talking to a machine that can write
        if (!isMaster()) {
            return -1;
        }

        if (isMongos() && toolGlobalParams.db == "" && exists(root / "config")) {
            log() << "Cannot do a full restore on a sharded system" << endl;
            return -1;
        }

        /* If toolGlobalParams.db is not "" then the user specified a db name to restore as.
         *
         * In that case we better be given either a root directory that
         * contains only .bson files or a single .bson file  (a db).
         *
         * In the case where a collection name is specified we better be
         * given either a root directory that contains only a single
         * .bson file, or a single .bson file itself (a collection).
         */
        drillDown(root, !toolGlobalParams.db.empty(), !toolGlobalParams.coll.empty(), true);
        string err = conn().getLastError(toolGlobalParams.db.empty() ? "admin" : toolGlobalParams.db);
        if (!err.empty()) {
            error() << err;
        }

        return EXIT_CLEAN;
    }

    void drillDown( boost::filesystem::path root, bool use_db, bool use_coll, bool top_level=false ) {
        LOG(2) << "drillDown: " << root.string() << endl;

        // skip hidden files and directories
        if (root.leaf().string()[0] == '.' && root.leaf().string() != ".")
            return;

        if ( is_directory( root ) ) {
            boost::filesystem::directory_iterator end;
            boost::filesystem::directory_iterator i(root);
            while ( i != end ) {
                boost::filesystem::path p = *i;
                i++;

                if (use_db) {
                    if (boost::filesystem::is_directory(p)) {
                        error() << "ERROR: root directory must be a dump of a single database" << endl;
                        error() << "       when specifying a db name with --db" << endl;
                        printHelp(cout);
                        return;
                    }
                }

                if (use_coll) {
                    if (boost::filesystem::is_directory(p) || i != end) {
                        error() << "ERROR: root directory must be a dump of a single collection" << endl;
                        error() << "       when specifying a collection name with --collection" << endl;
                        printHelp(cout);
                        return;
                    }
                }

                // don't insert oplog
                if (top_level && !use_db && p.leaf() == "oplog.bson")
                    continue;

                // Only restore indexes from a corresponding .metadata.json file.
                if ( p.leaf() != "system.indexes.bson" ) {
                    drillDown(p, use_db, use_coll);
                }
            }

            return;
        }

        if ( endsWith( root.string().c_str() , ".metadata.json" ) ) {
            // Metadata files are handled when the corresponding .bson file is handled
            return;
        }

        if ( ! ( endsWith( root.string().c_str() , ".bson" ) ||
                 endsWith( root.string().c_str() , ".bin" ) ) ) {
            error() << "don't know what to do with file [" << root.string() << "]" << endl;
            return;
        }

        log() << root.string() << endl;

        if ( root.leaf() == "system.profile.bson" ) {
            log() << "\t skipping" << endl;
            return;
        }

        string ns;
        if (use_db) {
            ns += toolGlobalParams.db;
        }
        else {
            ns = root.parent_path().filename().string();
            if (ns.empty())
                ns = "test";
        }

        verify( ns.size() );

        string oldCollName = root.leaf().string(); // Name of the collection that was dumped from
        oldCollName = oldCollName.substr( 0 , oldCollName.find_last_of( "." ) );
        if (use_coll) {
            ns += "." + toolGlobalParams.coll;
        }
        else {
            ns += "." + oldCollName;
        }

        log() << "\tgoing into namespace [" << ns << "]" << endl;

        if (mongoRestoreGlobalParams.drop) {
            if (root.leaf() != "system.users.bson" ) {
                log() << "\t dropping" << endl;
                conn().dropCollection( ns );
            } else {
                // Create map of the users currently in the DB
                BSONObj fields = BSON("user" << 1);
                scoped_ptr<DBClientCursor> cursor(conn().query(ns, Query(), 0, 0, &fields));
                while (cursor->more()) {
                    BSONObj user = cursor->next();
                    _users.insert(user["user"].String());
                }
            }
        }

        BSONObj metadataObject;
        if (mongoRestoreGlobalParams.restoreOptions || mongoRestoreGlobalParams.restoreIndexes) {
            boost::filesystem::path metadataFile = (root.branch_path() / (oldCollName + ".metadata.json"));
            if (!boost::filesystem::exists(metadataFile.string())) {
                // This is fine because dumps from before 2.1 won't have a metadata file, just print a warning.
                // System collections shouldn't have metadata so don't warn if that file is missing.
                if (!startsWith(metadataFile.leaf().string(), "system.")) {
                    log() << metadataFile.string() << " not found. Skipping." << endl;
                }
            } else {
                metadataObject = parseMetadataFile(metadataFile.string());
            }
        }

        _curns = ns.c_str();
        NamespaceString nss(_curns);
        _curdb = nss.db;
        _curcoll = nss.coll;

        // If drop is not used, warn if the collection exists.
        if (!mongoRestoreGlobalParams.drop) {
            scoped_ptr<DBClientCursor> cursor(conn().query(_curdb + ".system.namespaces",
                                                            Query(BSON("name" << ns))));
            if (cursor->more()) {
                // collection already exists show warning
                warning() << "Restoring to " << ns << " without dropping. Restored data "
                             "will be inserted without raising errors; check your server log"
                             << endl;
            }
        }

        vector<BSONObj> indexes;
        if (mongoRestoreGlobalParams.restoreIndexes && metadataObject.hasField("indexes")) {
            const vector<BSONElement> indexElements = metadataObject["indexes"].Array();
            for (vector<BSONElement>::const_iterator it = indexElements.begin(); it != indexElements.end(); ++it) {
                // Need to make sure the ns field gets updated to
                // the proper _curdb + _curns value, if we're
                // restoring to a different database.
                // Also need to update the options with any defaults specified on the command line
                indexes.push_back(updateOptions(renameIndexNs(it->Obj())));
            }
        }
        const BSONObj options = updateOptions(mongoRestoreGlobalParams.restoreOptions && metadataObject.hasField("options")
                                              ? metadataObject["options"].Obj()
                                              : BSONObj());

        if (mongoRestoreGlobalParams.doBulkLoad && !options["partitioned"].trueValue()) {
            RemoteLoader loader(conn(), _curdb, _curcoll, indexes, options);
            processFile( root );
            BSONObj res;
            bool ok = loader.commit(&res);
            if (!ok) {
                error() << "Error committing load for " << _curdb << "." << _curcoll << ": " << res << endl;
            }
        } else {
            // No bulk load. Create collection and indexes manually.
            if (!options.isEmpty()) {
                createCollectionWithOptions(options, metadataObject);
            }
            // Build indexes last - it's a little faster.
            processFile( root );
            for (vector<BSONObj>::iterator it = indexes.begin(); it != indexes.end(); ++it) {
                createIndex(*it);
            }
        }

        if (mongoRestoreGlobalParams.drop && root.leaf() == "system.users.bson") {
            // Delete any users that used to exist but weren't in the dump file
            for (set<string>::iterator it = _users.begin(); it != _users.end(); ++it) {
                BSONObj userMatch = BSON("user" << *it);
                conn().remove(ns, Query(userMatch));
            }
            _users.clear();
        }
    }

    virtual void gotObject( const BSONObj& obj ) {
        StringData collstr = nsToCollectionSubstring(_curns);
        massert( 16910, "Shouldn't be inserting into system.indexes directly",
                        collstr != "system.indexes" );
        if (mongoRestoreGlobalParams.drop && collstr == "system.users" && _users.count(obj["user"].String())) {
            // Since system collections can't be dropped, we have to manually
            // replace the contents of the system.users collection
            BSONObj userMatch = BSON("user" << obj["user"].String());
            conn().update(_curns, Query(userMatch), obj);
            _users.erase(obj["user"].String());
        } else {
            conn().insert( _curns , obj );

            // wait for insert to propagate to "w" nodes (doesn't warn if w used without replset)
            if (mongoRestoreGlobalParams.w > 0) {
                string err = conn().getLastError(_curdb, false, false, mongoRestoreGlobalParams.w);
                if (!err.empty()) {
                    error() << err << endl;
                }
            }
        }
    }

private:

    BSONObj updateOptions(const BSONObj &originalOptions) {
        BSONObjBuilder newOptsBuilder;
        bool compressionSpecified = false;
        bool pageSizeSpecified = false;
        bool readPageSizeSpecified = false;
        for (BSONObjIterator optsIter(originalOptions); optsIter.more(); ++optsIter) {
            BSONElement opt = *optsIter;
            StringData optname(opt.fieldName());
            if (optname == "compression") {
                compressionSpecified = true;
            } else if (optname == "pageSize") {
                pageSizeSpecified = true;
            } else if (optname == "readPageSize") {
                readPageSizeSpecified = true;
            }
            newOptsBuilder.append(opt);
        }
        if (!compressionSpecified && !mongoRestoreGlobalParams.defaultCompression.empty()) {
            newOptsBuilder.append("compression", mongoRestoreGlobalParams.defaultCompression);
        }
        if (!pageSizeSpecified && mongoRestoreGlobalParams.defaultPageSize != 0) {
            newOptsBuilder.append("pageSize", mongoRestoreGlobalParams.defaultPageSize);
        }
        if (!readPageSizeSpecified && mongoRestoreGlobalParams.defaultReadPageSize != 0) {
            newOptsBuilder.append("readPageSize", mongoRestoreGlobalParams.defaultReadPageSize);
        }
        return newOptsBuilder.obj();
    }

    BSONObj parseMetadataFile(string filePath) {
        long long fileSize = boost::filesystem::file_size(filePath);
        ifstream file(filePath.c_str(), ios_base::in);

        scoped_ptr<char> buf(new char[fileSize]);
        file.read(buf.get(), fileSize);
        int objSize;
        BSONObj obj;
        obj = fromjson (buf.get(), &objSize);
        return obj;
    }

    // Compares 2 BSONObj representing collection options. Returns true if the objects
    // represent different options. Ignores the "create" field.
    bool optionsSame(BSONObj obj1, BSONObj obj2) {
        int nfields = 0;
        BSONObjIterator i(obj1);
        while ( i.more() ) {
            BSONElement e = i.next();
            if (!obj2.hasField(e.fieldName())) {
                if (strcmp(e.fieldName(), "create") == 0) {
                    continue;
                } else {
                    return false;
                }
            }
            nfields++;
            if (e != obj2[e.fieldName()]) {
                return false;
            }
        }
        return nfields == obj2.nFields();
    }

    void createCollectionWithOptions(BSONObj obj, BSONObj metadataObject) {
        BSONObjIterator i(obj);

        // Rebuild obj as a command object for the "create" command.
        // - {create: <name>} comes first, where <name> is the new name for the collection
        // - elements with type Undefined get skipped over
        BSONObjBuilder bo;
        bo.append("create", _curcoll);
        while (i.more()) {
            BSONElement e = i.next();

            if (strcmp(e.fieldName(), "create") == 0) {
                continue;
            }

            if (e.type() == Undefined) {
                log() << _curns << ": skipping undefined field: " << e.fieldName() << endl;
                continue;
            }

            bo.append(e);
        }
        obj = bo.obj();

        BSONObj fields = BSON("options" << 1);
        scoped_ptr<DBClientCursor> cursor(conn().query(_curdb + ".system.namespaces", Query(BSON("name" << _curns)), 0, 0, &fields));

        bool createColl = true;
        if (cursor->more()) {
            createColl = false;
            if (metadataObject["partitioned"].trueValue()) {
                log() << "Collection " << _curns << " already exists, so we will not be creating the automatic partitions" << endl;
            }
            BSONObj nsObj = cursor->next();
            if (!nsObj.hasField("options") || !optionsSame(obj, nsObj["options"].Obj())) {
                    log() << "WARNING: collection " << _curns << " exists with different options than are in the metadata.json file and not using --drop. Options in the metadata file will be ignored." << endl;
            }
        }

        if (!createColl) {
            return;
        }

        BSONObj info;
        if (!conn().runCommand(_curdb, obj, info)) {
            uasserted(15936, "Creating collection " + _curns + " failed. Errmsg: " + info["errmsg"].String());
        } else {
            log() << "\tCreated collection " << _curns << " with options: " << obj.jsonString() << endl;
            if (metadataObject["partitionInfo"].trueValue()) {
                BSONObj res;
                BSONObjBuilder b;
                b.append("clonePartitionInfo", obj["create"].String());
                BSONObj pInfo = metadataObject["partitionInfo"].Obj();
                b.appendAs(pInfo["partitions"], "info");
                BSONObj o = b.obj();
                log() << "the obj, " << o << endl;
                bool ok = conn().runCommand(_curdb, o, info);
                log() << "ok: " << ok << "info: " << info << endl;
            }
        }
    }

    BSONObj renameIndexNs(const BSONObj &orig) {
        BSONObjBuilder bo;
        BSONObjIterator i(orig);
        while ( i.more() ) {
            BSONElement e = i.next();
            if (strcmp(e.fieldName(), "ns") == 0) {
                string s = _curdb + "." + _curcoll;
                bo.append("ns", s);
            }
            else if (strcmp(e.fieldName(), "v") != 0) { // Remove index version number
                bo.append(e);
            }
        }
        return bo.obj();
    }

    /* We must handle if the dbname or collection name is different at restore time than what was dumped.
     */
    void createIndex(BSONObj indexObj) {
        LOG(0) << "\tCreating index: " << indexObj << endl;
        conn().insert( _curdb + ".system.indexes" ,  indexObj );

        // We're stricter about errors for indexes than for regular data
        BSONObj err = conn().getLastErrorDetailed(_curdb, false, false, mongoRestoreGlobalParams.w);

        if (err.hasField("err") && !err["err"].isNull()) {
            if (err["err"].str() == "norepl" && mongoRestoreGlobalParams.w > 1) {
                error() << "Cannot specify write concern for non-replicas" << endl;
            }
            else {
                string errCode;

                if (err.hasField("code")) {
                    errCode = str::stream() << err["code"].numberInt();
                }

                error() << "Error creating index " << indexObj["ns"].String() << ": "
                        << errCode << " " << err["err"] << endl;
            }

            ::abort();
        }

        massert(16441, str::stream() << "Error calling getLastError: " << err["errmsg"],
                err["ok"].trueValue());
    }
};

REGISTER_MONGO_TOOL(Restore);
