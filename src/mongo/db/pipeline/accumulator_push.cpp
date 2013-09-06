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

#include "db/pipeline/expression_context.h"
#include "db/pipeline/value.h"

namespace mongo {
    Value AccumulatorPush::evaluate(const Document& pDocument) const {
        verify(vpOperand.size() == 1);
        Value prhs(vpOperand[0]->evaluate(pDocument));

        if (!pCtx->getDoingMerge()) {
            if (!prhs.missing()) {
                vpValue.push_back(prhs);
            }
        }
        else {
            /*
              If we're in the router, we need to take apart the arrays we
              receive and put their elements into the array we are collecting.
              If we didn't, then we'd get an array of arrays, with one array
              from each shard that responds.
             */
            verify(prhs.getType() == Array);
            
            const vector<Value>& vec = prhs.getArray();
            vpValue.insert(vpValue.end(), vec.begin(), vec.end());
        }

        return Value();
    }

    Value AccumulatorPush::getValue() const {
        return Value::createArray(vpValue);
    }

    AccumulatorPush::AccumulatorPush(
        const intrusive_ptr<ExpressionContext> &pTheCtx):
        Accumulator(),
        vpValue(),
        pCtx(pTheCtx) {
    }

    intrusive_ptr<Accumulator> AccumulatorPush::create(
        const intrusive_ptr<ExpressionContext> &pCtx) {
        intrusive_ptr<AccumulatorPush> pAccumulator(
            new AccumulatorPush(pCtx));
        return pAccumulator;
    }

    const char *AccumulatorPush::getOpName() const {
        return "$push";
    }
}
