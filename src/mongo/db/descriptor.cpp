/** @file descriptor.cpp */

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

#include "mongo/pch.h"
#include "mongo/db/descriptor.h"

namespace mongo {

    Descriptor::Descriptor(const Ordering &ordering) :
        _ordering(ordering) {
    }

    Descriptor::Descriptor(const char *data, const size_t size) :
        _ordering(*reinterpret_cast<const Ordering *>(data)) {
        verify(size == sizeof(Ordering));
    }

    bool Descriptor::operator==(const Descriptor &rhs) const {
        return memcmp(&_ordering, &rhs._ordering, sizeof(Ordering)) == 0;
    }

    DBT Descriptor::dbt() const {
        return storage::make_dbt(reinterpret_cast<const char *>(&_ordering), sizeof(Ordering));
    }

    int Descriptor::compareKeys(const storage::Key &key1, const storage::Key &key2) const {
        return key1.woCompare(key2, _ordering);
    }

} // namespace mongo
