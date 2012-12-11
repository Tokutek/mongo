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

#include <boost/date_time/posix_time/posix_time.hpp>
#include <string>

#include "mongo/base/disallow_copying.h"
#include "mongo/db/auth/principal_name.h"

namespace mongo {

    /**
     * Represents an authenticated user.  Every principal has a name and a time that the user's
     * authentication expires.
     * This class does not do any locking/synchronization, the consumer will be responsible for
     * synchronizing access.
     */
    class Principal {
        MONGO_DISALLOW_COPYING(Principal);

    public:
        // No expiration is represented as boost::posix_time::pos_infin
        Principal(const PrincipalName& name,
                  const boost::posix_time::ptime& expirationTime);
        explicit Principal(const PrincipalName& name);
        ~Principal();

        const PrincipalName& getName() const { return _name; }
        const boost::posix_time::ptime& getExpirationTime() const { return _expirationTime; }

        void setExpirationTime(boost::posix_time::ptime& expiration);

    private:
        PrincipalName _name;
        boost::posix_time::ptime _expirationTime;
    };

} // namespace mongo
