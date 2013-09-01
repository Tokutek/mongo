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

#include <stdint.h>

#include <string>

#include "mongo/base/units.h"
#include "mongo/unittest/unittest.h"

#include "mongo/base/status.h"

namespace {

    using mongo::BytesQuantity;
    using mongo::Status;
    using std::string;
    using std::stringstream;

    typedef BytesQuantity<long> long_bytes;

    TEST(Construction, Empty) {
        long_bytes b;
        ASSERT_EQUALS(b.value(), 0L);
    }

    TEST(Construction, FromLong) {
        long l(19892843L);
        long_bytes b(l);
        ASSERT_EQUALS(b.value(), l);
    }

    TEST(Construction, FromStringNoPrefix) {
        string s("1432545235");
        long l(1432545235L);
        long_bytes b(s);
        ASSERT_EQUALS(b.value(), l);
    }

    TEST(Construction, FromStringWithPrefix) {
        string noprefix("13452");
        long l(13452 * 1024);
        string lowerPrefixes("kmgt");
        string upperPrefixes("KMGT");
        for (size_t idx = 0; idx < lowerPrefixes.size(); ++idx) {
            long l2(l);
            for (size_t i = 0; i < idx; ++i) {
                l2 *= 1024;
            }
            for (int upper = 0; upper <= 1; ++upper) {
                for (int hasb = 0; hasb <= 1; ++hasb) {
                    stringstream ss;
                    ss << noprefix;
                    ss << (upper ? upperPrefixes[idx] : lowerPrefixes[idx]);
                    if (hasb) {
                        ss << (upper ? "B" : "b");
                    }
                    long_bytes b(ss.str());
                    ASSERT_EQUALS(b.value(), l2);
                }
            }
        }
    }

    TEST(Construction, NegativeFromLong) {
        long l(-19892843L);
        long_bytes b(l);
        ASSERT_EQUALS(b.value(), l);
    }

    TEST(Construction, NegativeFromStringNoPrefix) {
        string s("-1432545235");
        long l(-1432545235L);
        long_bytes b(s);
        ASSERT_EQUALS(b.value(), l);
    }

    TEST(Construction, NegativeFromStringWithPrefix) {
        string noprefix("-13452");
        long l(-13452 * 1024);
        string lowerPrefixes("kmgt");
        string upperPrefixes("KMGT");
        for (size_t idx = 0; idx < lowerPrefixes.size(); ++idx) {
            long l2(l);
            for (size_t i = 0; i < idx; ++i) {
                l2 *= 1024;
            }
            for (int upper = 0; upper <= 1; ++upper) {
                for (int hasb = 0; hasb <= 1; ++hasb) {
                    stringstream ss;
                    ss << noprefix;
                    ss << (upper ? upperPrefixes[idx] : lowerPrefixes[idx]);
                    if (hasb) {
                        ss << (upper ? "B" : "b");
                    }
                    long_bytes b(ss.str());
                    ASSERT_EQUALS(b.value(), l2);
                }
            }
        }
    }

    TEST(Function, Empty) {
        string s("");
        long l(1234L);
        Status status = long_bytes::fromString(s, l);
        ASSERT_FALSE(status.isOK());
        ASSERT_EQUALS(l, 1234L);
    }

    TEST(Function, Malformed) {
        long l(1234L);
        ASSERT_FALSE(long_bytes::fromString("b", l).isOK());
        ASSERT_EQUALS(l, 1234L);
        ASSERT_FALSE(long_bytes::fromString("kb", l).isOK());
        ASSERT_EQUALS(l, 1234L);
        ASSERT_FALSE(long_bytes::fromString("m", l).isOK());
        ASSERT_EQUALS(l, 1234L);
        ASSERT_FALSE(long_bytes::fromString("-b", l).isOK());
        ASSERT_EQUALS(l, 1234L);
        ASSERT_FALSE(long_bytes::fromString("-KB", l).isOK());
        ASSERT_EQUALS(l, 1234L);
        ASSERT_FALSE(long_bytes::fromString("-G", l).isOK());
        ASSERT_EQUALS(l, 1234L);
        ASSERT_FALSE(long_bytes::fromString("eliot", l).isOK());
        ASSERT_EQUALS(l, 1234L);
    }

    TEST(Function, Overflow) {
        long l(1234L);
        ASSERT_FALSE(long_bytes::fromString("1073741824T", l).isOK());
        ASSERT_EQUALS(l, 1234L);
        uint32_t u = 1234U;
        ASSERT_FALSE(BytesQuantity<uint32_t>::fromString("4G", u).isOK());
        ASSERT_EQUALS(u, 1234U);
        ASSERT_FALSE(BytesQuantity<uint32_t>::fromString("4294967296", u).isOK());
        ASSERT_EQUALS(u, 1234U);
        ASSERT_FALSE(BytesQuantity<uint32_t>::fromString("-1", u).isOK());
        ASSERT_EQUALS(u, 1234U);
        int32_t i = 1234;
        ASSERT_FALSE(BytesQuantity<int32_t>::fromString("2G", i).isOK());
        ASSERT_EQUALS(i, 1234);
        ASSERT_FALSE(BytesQuantity<int32_t>::fromString("2147483648", i).isOK());
        ASSERT_EQUALS(i, 1234);
    }

}
