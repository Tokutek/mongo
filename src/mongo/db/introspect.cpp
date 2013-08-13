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
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/user_set.h"
#include "mongo/db/curop.h"
#include "mongo/db/database.h"
#include "mongo/db/databaseholder.h"
#include "mongo/db/introspect.h"
#include "mongo/db/curop.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/ops/insert.h"
#include "mongo/util/goodies.h"

namespace mongo {

namespace {
    void _appendUserInfo(const Client& c,
                         BSONObjBuilder& builder,
                         AuthorizationSession* authSession) {
        UserSet::NameIterator nameIter = authSession->getAuthenticatedUserNames();

        UserName bestUser;
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
        b.appendDate("ts", jsTime());
        currentOp.debug().append( currentOp , b );
        b.append("client", c.clientAddress());

        AuthorizationSession * authSession = c.getAuthorizationSession();
        _appendUserInfo(c, b, authSession);

        BSONObj p = b.done();

        if (p.objsize() > 100*1024){
            string small = p.toString(/*isArray*/false, /*full*/false);

            warning() << "can't add full line to system.profile: " << small;

            // rebuild with limited info
            BSONObjBuilder b(profileBufBuilder);
            b.appendDate("ts", jsTime());
            b.append("client", c.clientAddress() );
            _appendUserInfo(c, b, authSession);

            b.append("err", "profile line too large (max is 100KB)");
            if (small.size() < 100*1024){ // should be much smaller but if not don't break anything
                b.append("abbreviated", small);
            }

            p = b.done();
        }

        // get or create the profiling collection
        Collection *cl = getOrCreateProfileCollection(db);
        if (cl) {
            insertOneObject(cl, p);
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

    Collection *getOrCreateProfileCollection(Database *db, bool force) {
        fassert(16372, db);
        const char *profileName = db->profileName().c_str();
        Collection *cl = getCollection(profileName);
        if (!cl && (serverGlobalParams.defaultProfile || force)) {
            // system.profile collection doesn't exist; create it
            log() << "creating profile collection: " << profileName << endl;
            string errmsg;
            if (!userCreateNS(profileName,
                              BSON("capped" << true << "size" << 1024 * 1024 << "autoIndexId" << false),
                              errmsg , false)) {
                log() << "could not create ns " << profileName << ": " << errmsg << endl;
                return NULL;
            }
            cl = getCollection(profileName);
        }
        if (!cl) {
            // failed to get or create profile collection
            static time_t last = time(0) - 10;  // warn the first time
            if( time(0) > last+10 ) {
                log() << "profile: warning ns " << profileName << " does not exist" << endl;
                last = time(0);
            }
        }
        return cl;
    }

} // namespace mongo
