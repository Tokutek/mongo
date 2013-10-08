// introspect.cpp

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

#include "mongo/bson/util/builder.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/principal_set.h"
#include "mongo/db/curop.h"
#include "mongo/db/database.h"
#include "mongo/db/databaseholder.h"
#include "mongo/db/introspect.h"
#include "mongo/db/curop.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/ops/insert.h"
#include "mongo/util/goodies.h"

namespace {
    const size_t MAX_PROFILE_DOC_SIZE_BYTES = 100*1024;
}

namespace mongo {

namespace {
    void _appendUserInfo(const Client& c, BSONObjBuilder& builder, AuthorizationManager* authManager) {
        PrincipalSet::NameIterator nameIter = authManager->getAuthenticatedPrincipalNames();

        PrincipalName bestUser;
        if (nameIter.more())
            bestUser = *nameIter;

        StringData opdb( nsToDatabaseSubstring( c.ns() ) );

        BSONArrayBuilder allUsers(builder.subarrayStart("allUsers"));
        for ( ; nameIter.more(); nameIter.next()) {
            BSONObjBuilder nextUser(allUsers.subobjStart());
            nextUser.append(AuthorizationManager::USER_NAME_FIELD_NAME, nameIter->getUser());
            nextUser.append(AuthorizationManager::USER_SOURCE_FIELD_NAME, nameIter->getDB());
            nextUser.doneFast();

            if (nameIter->getDB() == opdb) {
                bestUser = *nameIter;
            }
        }
        allUsers.doneFast();

        builder.append("user", bestUser.getUser().empty() ? "" : bestUser.getFullName());

    }
} // namespace

    static void _profile(const Client& c, CurOp& currentOp, BufBuilder& profileBufBuilder) {
        Database *db = c.database();
        DEV verify( db );

        // build object
        BSONObjBuilder b(profileBufBuilder);

        const bool isQueryObjTooBig = !currentOp.debug().append(currentOp, b,
                MAX_PROFILE_DOC_SIZE_BYTES);

        b.appendDate("ts", jsTime());
        b.append("client", c.clientAddress());

        AuthorizationManager* authManager = c.getAuthorizationManager();
        _appendUserInfo(c, b, authManager);

        BSONObj p = b.done();

        if (static_cast<size_t>(p.objsize()) > MAX_PROFILE_DOC_SIZE_BYTES || isQueryObjTooBig) {
            string small = p.toString(/*isArray*/false, /*full*/false);

            warning() << "can't add full line to system.profile: " << small << endl;

            // rebuild with limited info
            BSONObjBuilder b(profileBufBuilder);
            b.appendDate("ts", jsTime());
            b.append("client", c.clientAddress() );
            _appendUserInfo(c, b, authManager);

            b.append("err", "profile line too large (max is 100KB)");

            // should be much smaller but if not don't break anything
            if (small.size() < MAX_PROFILE_DOC_SIZE_BYTES){
                b.append("abbreviated", small);
            }

            p = b.done();
        }

        // get or create the profiling collection
        NamespaceDetails *details = getOrCreateProfileCollection(db);
        if (details) {
            insertOneObject(details, p);
        }
    }

    void profile(const Client& c, int op, CurOp& currentOp) {
        Lock::assertAtLeastReadLocked(currentOp.getNS());

        // initialize with 1kb to start, to avoid realloc later
        // doing this outside the dblock to improve performance
        BufBuilder profileBufBuilder(1024);

        try {
            _profile(c, currentOp, profileBufBuilder);
        }
        catch (const AssertionException& assertionEx) {
            warning() << "Caught Assertion while trying to profile " << opToString(op)
                      << " against " << currentOp.getNS()
                      << ": " << assertionEx.toString() << endl;
        }
    }

    NamespaceDetails* getOrCreateProfileCollection(Database *db, bool force) {
        fassert(16372, db);
        const char* profileName = db->profileName().c_str();
        NamespaceDetails* details = nsdetails(profileName);
        if (!details && (cmdLine.defaultProfile || force)) {
            // system.profile namespace doesn't exist; create it
            log() << "creating profile collection: " << profileName << endl;
            string errmsg;
            if (!userCreateNS(profileName,
                              BSON("capped" << true << "size" << 1024 * 1024 << "autoIndexId" << false),
                              errmsg , false)) {
                log() << "could not create ns " << profileName << ": " << errmsg << endl;
                return NULL;
            }
            details = nsdetails(profileName);
        }
        if (!details) {
            // failed to get or create profile collection
            static time_t last = time(0) - 10;  // warn the first time
            if( time(0) > last+10 ) {
                log() << "profile: warning ns " << profileName << " does not exist" << endl;
                last = time(0);
            }
        }
        return details;
    }

} // namespace mongo
