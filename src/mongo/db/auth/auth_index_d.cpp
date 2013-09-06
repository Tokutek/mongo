/**
*    Copyright (C) 2012 10gen Inc.
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
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#include "mongo/db/auth/auth_index_d.h"

#include "mongo/base/init.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/client.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/index_update.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_details.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

namespace mongo {
namespace authindex {

namespace {
    BSONObj oldSystemUsersKeyPattern;
    BSONObj extendedSystemUsersKeyPattern;
    std::string extendedSystemUsersIndexName;

    MONGO_INITIALIZER(AuthIndexKeyPatterns)(InitializerContext*) {
        oldSystemUsersKeyPattern = BSON(AuthorizationManager::USER_NAME_FIELD_NAME << 1);
        extendedSystemUsersKeyPattern = BSON(AuthorizationManager::USER_NAME_FIELD_NAME << 1 <<
                                             AuthorizationManager::USER_SOURCE_FIELD_NAME << 1);
        extendedSystemUsersIndexName = std::string(str::stream() <<
                                                   AuthorizationManager::USER_NAME_FIELD_NAME <<
                                                   "_1_" <<
                                                   AuthorizationManager::USER_SOURCE_FIELD_NAME <<
                                                   "_1");
        return Status::OK();
    }

    void configureSystemUsersIndexes(const StringData& dbname) {
        std::string systemUsers = dbname.toString() + ".system.users";
        Client::WriteContext wctx(systemUsers);

        NamespaceString systemUsersNS( systemUsers );
        createSystemIndexes(systemUsersNS);

        NamespaceDetails* nsd = nsdetails(systemUsers);
        if (nsd == NULL)
            return;

        NamespaceDetails::IndexIterator indexIter = nsd->ii();
        std::vector<std::string> namedIndexesToDrop;

        while (indexIter.more()) {
            IndexDetails& idetails = indexIter.next();
            if (idetails.keyPattern() == oldSystemUsersKeyPattern)
                namedIndexesToDrop.push_back(idetails.indexName());
        }
        for (size_t i = 0; i < namedIndexesToDrop.size(); ++i) {
            std::string errmsg;
            BSONObjBuilder infoBuilder;

            if (dropIndexes(nsd,
                            systemUsers.c_str(),
                            namedIndexesToDrop[i].c_str(),
                            errmsg,
                            infoBuilder,
                            false)) {
                log() << "Dropped index " << namedIndexesToDrop[i] << " with key pattern " <<
                    oldSystemUsersKeyPattern << " from " << systemUsers <<
                    " because it is incompatible with extended form privilege documents." << endl;
            }
            else {
                // Only reason should be orphaned index, which dropIndexes logged.
            }
        }
    }
}  // namespace

    void configureSystemIndexes(const StringData& dbname) {
        configureSystemUsersIndexes(dbname);
    }

    void createSystemIndexes(const NamespaceString& ns) {
        if (ns.coll() == "system.users") {
            try {
                Helpers::ensureIndex(ns.ns().c_str(),
                                     extendedSystemUsersKeyPattern,
                                     true,  // unique
                                     extendedSystemUsersIndexName.c_str());
            } catch (const DBException& e) {
                if (e.getCode() == ASSERT_ID_DUPKEY) {
                    log() << "Duplicate key exception while trying to build unique index on " <<
                            ns << ".  You most likely have user documents with duplicate \"user\" "
                            "fields.  To resolve this, start up with a version of MongoDB prior to "
                            "2.4, drop the duplicate user documents, then start up again with the "
                            "current version." << endl;
                }
                throw;
            }
        }
    }

}  // namespace authindex
}  // namespace mongo
