/**
*    Copyright (C) 2013 10gen Inc.
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

#include "mongo/db/index_names.h"

#include "mongo/db/jsobj.h"

namespace mongo {
    const string IndexNames::GEO_2D = "2d";
    const string IndexNames::GEO_HAYSTACK = "geoHaystack";
    const string IndexNames::GEO_2DSPHERE = "2dsphere";
    const string IndexNames::TEXT = "text";
    const string IndexNames::TEXT_INTERNAL = "_fts";
    const string IndexNames::HASHED = "hashed";

    // static
    string IndexNames::findPluginName(const BSONObj& keyPattern) {
        BSONObjIterator i(keyPattern);

        while (i.more()) {
            BSONElement e = i.next();
            if (String != e.type()) { continue; }
            return e.String();
        }

        return "";
    }

}  // namespace mongo
