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

#include <vector>

#include "mongo/pch.h"
#include "mongo/db/index/haystack_key_generator.h"

namespace mongo {

    HaystackKeyGenerator::HaystackKeyGenerator(const string &geoField,
                                               const vector<string> &otherFields,
                                               const double bucketSize,
                                               const bool sparse) :
        KeyGenerator(vector<const char *>(), sparse),
        _geoField(geoField),
        _otherFields(otherFields),
        _bucketSize(bucketSize) {
    }

    // Build a new BSONObj with root in it.  If e is non-empty, append that to the key.  Insert
    // the BSONObj into keys.
    static void addKey(const string& root, const BSONElement& e, BSONObjSet& keys) {
        BSONObjBuilder buf;
        buf.append("", root);

        if (e.eoo())
            buf.appendNull("");
        else
            buf.appendAs(e, "");

        keys.insert(buf.obj());
    }

    // NOTE: these functions, hash and makeString, were taken verbatim
    //       from HashstackIndex and must be kept in sync with those
    //       implementations. We copied them to make linking simpler
    //       with respect to separating coredb and serveronly

    static int hash(double bucketSize, double d) {
        d += 180;
        d /= bucketSize;
        return static_cast<int>(d);
    }

    static int hash(double bucketSize, const BSONElement& e) {
        uassert(13322, "geo field is not a number", e.isNumber());
        return hash(bucketSize, e.numberDouble());
    }

    static string makeString(int hashedX, int hashedY) {
        stringstream ss;
        ss << hashedX << "_" << hashedY;
        return ss.str();
    }

    void HaystackKeyGenerator::getKeys(const BSONObj &obj, BSONObjSet &keys) const {
        BSONElement loc = obj.getFieldDotted(_geoField);
        if (loc.eoo())
            return;

        uassert(13323, "latlng not an array", loc.isABSONObj());
        string root;
        {
            BSONObjIterator i(loc.Obj());
            BSONElement x = i.next();
            BSONElement y = i.next();
            root = makeString(hash(_bucketSize, x), hash(_bucketSize, y));
        }

        verify(_otherFields.size() == 1);

        BSONElementSet all;

        // This is getFieldsDotted (plural not singular) since the object we're indexing
        // may be an array.
        obj.getFieldsDotted(_otherFields[0], all);

        if (all.size() == 0) {
            // We're indexing a document that doesn't have the secondary non-geo field present.
            // XXX: do we want to add this even if all.size() > 0?  result:empty search terms
            // match everything instead of only things w/empty search terms)
            addKey(root, BSONElement(), keys);
        } else {
            // Ex:If our secondary field is type: "foo" or type: {a:"foo", b:"bar"},
            // all.size()==1.  We can query on the complete field.
            // Ex: If our secondary field is type: ["A", "B"] all.size()==2 and all has values
            // "A" and "B".  The query looks for any of the fields in the array.
            for (BSONElementSet::iterator i = all.begin(); i != all.end(); ++i) {
                addKey(root, *i, keys);
            }
        }
    }

} // namespace mongo
