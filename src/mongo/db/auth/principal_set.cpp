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

#include "mongo/pch.h"

#include "mongo/db/auth/principal_set.h"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <string>
#include <vector>

#include "mongo/db/auth/principal.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    PrincipalSet::PrincipalSet() {}
    PrincipalSet::~PrincipalSet() {
        for (std::vector<Principal*>::iterator it = _principals.begin();
                it != _principals.end(); ++it) {
            delete *it;
        }
    }

    void PrincipalSet::add(Principal* principal) {
        for (std::vector<Principal*>::iterator it = _principals.begin();
                it != _principals.end(); ++it) {
            Principal* current = *it;
            if (current->getName().getDB() == principal->getName().getDB()) {
                // There can be only one principal per database.
                delete current;
                *it = principal;
                return;
            }
        }
        _principals.push_back(principal);
    }

    void PrincipalSet::removeByDBName(const StringData& dbname) {
        for (std::vector<Principal*>::iterator it = _principals.begin();
                it != _principals.end(); ++it) {
            Principal* current = *it;
            if (current->getName().getDB() == dbname) {
                delete current;
                _principals.erase(it);
                break;
            }
        }
    }

    Principal* PrincipalSet::lookup(const PrincipalName& name) const {
        Principal* principal = lookupByDBName(name.getDB());
        if (principal && principal->getName() == name) {
            return principal;
        }
        return NULL;
    }

    Principal* PrincipalSet::lookupByDBName(const StringData& dbname) const {
        for (std::vector<Principal*>::const_iterator it = _principals.begin();
                it != _principals.end(); ++it) {
            Principal* current = *it;
            if (current->getName().getDB() == dbname) {
                return current;
            }
        }
        return NULL;
    }

} // namespace mongo
