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
*/

#pragma once

#include <string>
#include <vector>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/string_data.h"
#include "mongo/db/auth/principal.h"
#include "mongo/db/auth/principal_name.h"

namespace mongo {

    /**
     * A collection of authenticated principals.
     * This class does not do any locking/synchronization, the consumer will be responsible for
     * synchronizing access.
     */
    class PrincipalSet {
        MONGO_DISALLOW_COPYING(PrincipalSet);
    public:
        PrincipalSet();
        ~PrincipalSet();

        // If the principal is already present, this will replace the existing entry.
        // The PrincipalSet takes ownership of the passed-in principal and is responsible for
        // deleting it eventually
        void add(Principal* principal);

        // Removes all principals whose authentication credentials came from dbname.
        void removeByDBName(const StringData& dbname);

        // Returns the Principal with the given name, or NULL if not found.
        // Ownership of the returned Principal remains with the PrincipalSet.  The pointer
        // returned is only guaranteed to remain valid until the next non-const method is called
        // on the PrincipalSet.
        Principal* lookup(const PrincipalName& name) const;

        // Gets the principal whose authentication credentials came from dbname, or NULL if none
        // exist.  There should be at most one such principal.
        Principal* lookupByDBName(const StringData& dbname) const;

    private:
        // The PrincipalSet maintains ownership of the Principals in it, and is responsible for
        // deleting them when done with them.
        std::vector<Principal*> _principals;
    };

} // namespace mongo
