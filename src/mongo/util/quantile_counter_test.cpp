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

#include "mongo/unittest/unittest.h"
#include "mongo/util/quantile_counter.h"
#include "mongo/util/log.h"

namespace {

    using std::endl;

    TEST(QuantileCounter, TestBasic) {
        mongo::QuantileSet s;
        s.add(0.9, 0.00001);
        s.add(0.95, 0.000001);
        s.add(0.99, 0.00000001);
        mongo::QuantileCounter<unsigned long long> counter(s);

        for (int i = 0; i < 100000; ++i) {
            counter << rand();
        }

        mongo::tlog(0) << "80th: " << counter.quantile(0.8) << endl;
        mongo::tlog(0) << "90th: " << counter.quantile(0.9) << endl;
        mongo::tlog(0) << "95th: " << counter.quantile(0.95) << endl;
        mongo::tlog(0) << "99th: " << counter.quantile(0.99) << endl;
    }

}  // anonymous namespace
