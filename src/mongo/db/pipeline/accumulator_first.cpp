/**
 * Copyright (c) 2011 10gen Inc.
 * Copyright (C) 2013 Tokutek Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 */

#include "pch.h"
#include "accumulator.h"

#include "db/pipeline/value.h"

namespace mongo {

    Value AccumulatorFirst::evaluate(const Document& pDocument) const {
        verify(vpOperand.size() == 1);

        /* only remember the first value seen */
        if (!_haveFirst) {
            // can't use pValue.missing() since we want the first value even if missing
            _haveFirst = true;
            pValue = vpOperand[0]->evaluate(pDocument);
        }

        return pValue;
    }

    AccumulatorFirst::AccumulatorFirst()
        : AccumulatorSingleValue()
        , _haveFirst(false)
    {}

    intrusive_ptr<Accumulator> AccumulatorFirst::create(
        const intrusive_ptr<ExpressionContext> &pCtx) {
        intrusive_ptr<AccumulatorFirst> pAccumulator(
            new AccumulatorFirst());
        return pAccumulator;
    }

    const char *AccumulatorFirst::getOpName() const {
        return "$first";
    }
}
