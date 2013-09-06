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

#include "mongo/db/field_ref.h"
#include "mongo/util/assert_util.h"

namespace mongo {

    void FieldRef::parse(const StringData& dottedField) {
        if (dottedField.size() == 0) {
            return;
        }

        // We guarantee that accesses through getPart() will be valid while 'this' is. So we
        // take a copy. We're going to be "chopping" up the copy into c-strings.
        _fieldBase.reset(new char[dottedField.size()+1]);
        dottedField.copyTo( _fieldBase.get(), true );

        // Separate the field parts using '.' as a delimiter.
        char* beg = _fieldBase.get();
        char* cur = beg;
        char* end = beg + dottedField.size();
        while (true) {
            if (cur != end && *cur != '.') {
                cur++;
                continue;
            }

            appendPart(StringData(beg, cur - beg));

            if (cur != end) {
                *cur = '\0';
                beg = ++cur;
                continue;
            }

            break;
        }
    }

    void FieldRef::setPart(size_t i, const StringData& part) {
        dassert(i < _size);

        if (_replacements.size() != _size) {
            _replacements.resize(_size);
        }

        _replacements[i] = part.toString();
        if (i < kReserveAhead) {
            _fixed[i] = _replacements[i];
        }
        else {
            _variable[getIndex(i)] = _replacements[i];
        }
    }

    size_t FieldRef::appendPart(const StringData& part) {
        if (_size < kReserveAhead) {
            _fixed[_size] = part;
        }
        else {
            _variable.push_back(part);
        }
        return ++_size;
    }

    StringData FieldRef::getPart(size_t i) const {
        dassert(i < _size);

        if (i < kReserveAhead) {
            return _fixed[i];
        }
        else {
            return _variable[getIndex(i)];
        }
    }

    void FieldRef::clear() {
        _size = 0;
        _variable.clear();
        _fieldBase.reset();
        _replacements.clear();
    }

    std::string FieldRef::dottedField() const {
        std::string res;
        if (_size == 0) {
            return res;
        }

        res.append(_fixed[0].rawData(), _fixed[0].size());
        for (size_t i=1; i<_size; i++) {
            res.append(1, '.');
            StringData part = getPart(i);
            res.append(part.rawData(), part.size());
        }
        return res;
    }

    size_t FieldRef::numReplaced() const {
        size_t res = 0;
        for (size_t i = 0; i < _replacements.size(); i++) {
            if (!_replacements[i].empty()) {
                res++;
            }
        }
        return res;
    }

} // namespace mongo
