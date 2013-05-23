/*    Copyright 2012 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#pragma once

#include "mongo/base/disallow_copying.h"
#include "mongo/base/status.h"
#include "mongo/db/auth/auth_session_external_state_server_common.h"

namespace mongo {

    /**
     * The implementation of AuthSessionExternalState functionality for mongod.
     */
    class AuthSessionExternalStateMongod : public AuthSessionExternalStateServerCommon {
        MONGO_DISALLOW_COPYING(AuthSessionExternalStateMongod);

    public:
        AuthSessionExternalStateMongod();
        virtual ~AuthSessionExternalStateMongod();

        virtual bool shouldIgnoreAuthChecks() const;

        virtual void startRequest();

    protected:
        virtual bool _findUser(const string& usersNamespace,
                               const BSONObj& query,
                               BSONObj* result) const;
    };

} // namespace mongo
