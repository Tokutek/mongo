/* command_cursors.cpp
 */

/*    Copyright (C) 2013 Tokutek Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#include "mongo/db/command_cursors.h"

#include "mongo/pch.h"

#include "mongo/db/clientcursor.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/ops/query.h"
#include "mongo/util/assert_util.h"

namespace mongo {

    bool isCursorCommand(BSONObj cmdObj) {
        BSONElement cursorElem = cmdObj["cursor"];
        if (cursorElem.eoo())
            return false;

        uassert(16954, "cursor field must be missing or an object",
                cursorElem.type() == Object);

        BSONObj cursor = cursorElem.embeddedObject();
        BSONElement batchSizeElem = cursor["batchSize"];
        if (batchSizeElem.eoo()) {
            uassert(16955, "cursor object can't contain fields other than batchSize",
                cursor.isEmpty());
        }
        else {
            uassert(16956, "cursor.batchSize must be a number",
                    batchSizeElem.isNumber());

            // This can change in the future, but for now all negatives are reserved.
            uassert(16957, "Cursor batchSize must not be negative",
                    batchSizeElem.numberLong() >= 0);
        }

        return true;
    }

    void handleCursorCommand(CursorId id, BSONObj& cmdObj, BSONObjBuilder& result) {
        BSONElement batchSizeElem = cmdObj.getFieldDotted("cursor.batchSize");
        const long long batchSize = batchSizeElem.isNumber()
                                    ? batchSizeElem.numberLong()
                                    : 101; // same as query

        // Using limited cursor API that ignores many edge cases. Should be sufficient for commands.
        ClientCursor::Pin pin(id);
        ClientCursor* cursor = pin.c();

        massert(16958, "Cursor shouldn't have been deleted",
                cursor);

        // Make sure this cursor won't disappear on us
        fassert(16959, !cursor->c()->shouldDestroyOnNSDeletion());

        try {
            const string cursorNs = cursor->ns(); // we need this after cursor may have been deleted

            // can't use result BSONObjBuilder directly since it won't handle exceptions correctly.
            BSONArrayBuilder resultsArray;
            const int byteLimit = MaxBytesToReturnToClientAtOnce;
            for (int objs = 0;
                    objs < batchSize && cursor->ok() && resultsArray.len() <= byteLimit;
                    objs++) {
                // TODO may need special logic if cursor->current() would cause results to be > 16MB
                resultsArray.append(cursor->current());
                cursor->advance();
            }

            // The initial ok() on a cursor may be very expensive so we don't do it when batchSize
            // is 0 since that indicates a desire for a fast return.
            if (batchSize != 0 && !cursor->ok()) {
                // There is no more data. Kill the cursor.
                pin.release();
                ClientCursor::erase(id);
                id = 0;
                cursor = NULL; // make it an obvious error to use cursor after this point
            }

            BSONObjBuilder cursorObj(result.subobjStart("cursor"));
            cursorObj.append("id", id);
            cursorObj.append("ns", cursorNs);
            cursorObj.append("firstBatch", resultsArray.arr());
            cursorObj.done();
        }
        catch (...) {
            // Clean up cursor on way out of scope.
            pin.release();
            ClientCursor::erase(id);
            throw;
        }
    }

} // namespace mongo
