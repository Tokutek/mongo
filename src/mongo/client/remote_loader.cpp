/** @file remote_loader.cpp */

/**
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

#include "remote_loader.h"

#include "mongo/pch.h"

#include "mongo/client/connpool.h"
#include "mongo/db/jsobj.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    RemoteLoader::RemoteLoader(DBClientWithCommands &conn, const string &db, const BSONObj &obj) : _conn(&conn), _db(db), _rtxn(conn, "serializable"), _usingLoader(false), _commandObj() {
        begin(obj);
    }

    RemoteLoader::RemoteLoader(DBClientWithCommands &conn, const string &db,
                 const string &ns, const vector<BSONObj> &indexes, const BSONObj &options)
            : _conn(&conn), _db(db), _rtxn(conn, "serializable"), _usingLoader(false), _commandObj() {
        BSONObjBuilder b;
        beginLoadCmd(ns, indexes, options, b);
        begin(b.done());
    }

    void RemoteLoader::beginLoadCmd(const string &ns, const vector<BSONObj> &indexes, const BSONObj &options,
                                    BSONObjBuilder &b) {
        b.append("beginLoad", 1);
        b.append("ns", ns);
        {
            BSONArrayBuilder ab(b.subarrayStart("indexes"));
            for (vector<BSONObj>::const_iterator it = indexes.begin(); it != indexes.end(); ++it) {
                ab.append(*it);
            }
            ab.doneFast();
        }
        b.append("options", options);
    }

    void RemoteLoader::begin(const BSONObj &obj) {
        _commandObj = obj;
        BSONObj res;
        bool ok = _conn->runCommand(_db, obj, res);
        if (ok) {
            _usingLoader = true;
        } else {
            LOG(0) << "RemoteLoader failed to beginLoad: " << res
                   << ". Falling back to normal inserts." << endl;
            BSONObjBuilder cb;
            BSONElement nsElt = obj["ns"];
            uassert(16923, mongoutils::str::stream() << "invalid bulkLoad obj: " << obj,
                    nsElt.ok() && nsElt.type() == String);
            cb.append("create", nsElt.Stringdata());
            BSONElement optsElt = obj["options"];
            if (optsElt.ok()) {
                BSONObjIterator it(optsElt.Obj());
                while (it.more()) {
                    BSONElement e = it.next();
                    cb.append(e);
                }
            }
            ok = _conn->runCommand(_db, cb.done(), res);
            uassert(16924, mongoutils::str::stream() << "error creating collection: " << res, ok);
        }
    }

    RemoteLoader::~RemoteLoader() {
        if (_conn) {
            try {
                abort();
            }
            catch (DBException &e) {
                LOG(1) << "error aborting RemoteLoader" << endl;
                // not much else we can do
            }
        }
    }

    bool RemoteLoader::commit(BSONObj *res) {
        bool ok = true;
        if (_usingLoader) {
            ok = _conn->simpleCommand(_db, res, "commitLoad");
        } else {
            // Need to pick apart the obj here, in case the user passed one in using the 3-parameter
            // constructor.
            BSONElement indexesElt = _commandObj["indexes"];
            if (indexesElt.ok()) {
                uassert(16925, mongoutils::str::stream() << "invalid beginLoad command object: " << _commandObj,
                        indexesElt.type() == Array);
                const vector<BSONElement> indexes = indexesElt.Array();
                stringstream nss;
                nss << _db << ".system.indexes";
                string ns = nss.str();
                for (vector<BSONElement>::const_iterator it = indexes.begin(); it != indexes.end(); ++it) {
                    _conn->insert(ns, it->Obj());
                    string le = _conn->getLastError(_db);
                    ok = le.empty();
                    uassert(16926, mongoutils::str::stream() << "error ensuring index: " << le, ok);
                }
            }
        }

        if (ok) {
            _conn = NULL;
            ok = _rtxn.commit(res);
        }
        return ok;
    }

    bool RemoteLoader::abort(BSONObj *res) {
        bool ok = true;
        if (_usingLoader) {
            ok = _conn->simpleCommand(_db, res, "abortLoad");
        }

        if (ok) {
            _conn = NULL;
            ok = _rtxn.rollback(res);
        }
        return ok;
    }

} // namespace mongo
