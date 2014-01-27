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

#pragma once

#include "mongo/pch.h"

#include <istream>
#include <limits>
#include <string>

#include "mongo/base/parse_number.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/oid.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    /**
     * BytesQuantity can be treated like an integer, except that it can be initialized from a string
     * that ends in units (/[kKmMgG][bB]?/).  Can be used just like a number of type T by casting.
     */
    template <typename T>
    class BytesQuantity {
        T _value;

      public:
        static Status fromString(const StringData &s, T &result) {
            long long multiplier = 1;
            StringData numberPart = s;

            if (numberPart.endsWith("b") || numberPart.endsWith("B")) {
                numberPart = numberPart.substr(0, numberPart.size() - 1);
            }
            if (numberPart.size() < 1) {
                return Status(ErrorCodes::BadValue, mongoutils::str::stream() << "invalid number: " << s);
            }
            char c = numberPart[numberPart.size() - 1];
            switch (c) {
                case 't':
                case 'T':
                    multiplier <<= 10;
                case 'g':
                case 'G':
                    multiplier <<= 10;
                case 'm':
                case 'M':
                    multiplier <<= 10;
                case 'k':
                case 'K':
                    multiplier <<= 10;
                    numberPart = numberPart.substr(0, numberPart.size() - 1);
                    break;
                default:
                    if (c < '0' || c > '9') {
                        return Status(ErrorCodes::BadValue, mongoutils::str::stream() << "invalid number: " << s);
                    }
            }
            if (numberPart.empty() || numberPart == "-") {
                return Status(ErrorCodes::BadValue, mongoutils::str::stream() << "invalid number: " << s);
            }
            T value;
            Status status = parseNumberFromString(numberPart, &value);
            if (!status.isOK()) {
                return status;
            }
            
            typedef ::std::numeric_limits<T> limits;
            // The template is invalid for floating types because parseNumberFromString isn't
            // specialized for any floating types.  But if it were, we'd need to handle underflow
            // for those differently.
            if (limits::is_signed && value < 0) {
                if ((limits::min() / multiplier) > value) {
                    return Status(ErrorCodes::Overflow, mongoutils::str::stream() << "underflow when parsing " << s);
                }
            }
            else {
                if ((limits::max() / multiplier) < value) {
                    return Status(ErrorCodes::Overflow, mongoutils::str::stream() << "overflow when parsing " << s);
                }
            }
            result = value * multiplier;
            return Status::OK();
        }
        static T fromString(const StringData &s) {
            // Some compilers are not smart enough to realize that we can't return unless we've
            // assigned to this (because otherwise we would throw at massert).  Those compilers
            // think this is maybe-uninitialized unless we do this:
            T result(0);
            Status status = fromString(s, result);
            massert(16919, mongoutils::str::stream() << "error parsing " << s << ": " << status.codeString() << " " << status.reason(), status.isOK());
            return result;
        }

        BytesQuantity() : _value(0) {}
        BytesQuantity(T value) : _value(value) {}
        BytesQuantity(const StringData &s) : _value(fromString(s)) {}
        BytesQuantity(const BSONElement &e) : _value(0) {
            if (e.ok()) {
                if (e.type() == String) {
                    _value = fromString(e.Stringdata());
                } else {
                    uassert(17015, mongoutils::str::stream() << "element is not a number: " << e.wrap(), e.isNumber());
                    bool ok = e.coerce(&_value);
                    uassert(17316, mongoutils::str::stream() << "couldn't coerce to a number: " << e.wrap(), ok);
                }
            }
        }

        T value() const { return _value; }
        operator T() const { return value(); }

        friend inline std::istream &operator>>(std::istream &source, BytesQuantity &target) {
            // Ensure we're calling the StringData constructor above, and not the direct value constructor.
            string s;
            source >> s;
            target = BytesQuantity(s);
            return source;
        }
    };

} // namespace mongo
