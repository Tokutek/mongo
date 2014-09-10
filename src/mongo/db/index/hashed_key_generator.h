// keygenerator.h

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

#pragma once

#include <vector>

#include "mongo/pch.h"
#include "mongo/db/key_generator.h"
#include "mongo/db/hasher.h"
#include "mongo/db/jsobj.h"

namespace mongo {

    // Generates keys for a hashed index.
    class HashedKeyGenerator : public KeyGenerator {
    public:
        HashedKeyGenerator(const vector<const char *> fieldNames,
                           const HashSeed &seed,
                           const HashVersion &hashVersion,
                           const bool sparse) :
            KeyGenerator(fieldNames, sparse),
            _seed(seed),
            _hashVersion(hashVersion) {
        }
        virtual ~HashedKeyGenerator() { }

        virtual void getKeys(const BSONObj &obj, BSONObjSet &keys) const;

    private:
        static long long int makeSingleKey(const BSONElement &e,
                                           const HashSeed &seed,
                                           const HashVersion &v = 0);

        const HashSeed &_seed;
        const HashVersion &_hashVersion;

        friend class HashedIndex;
    };

} // namespace mongo
