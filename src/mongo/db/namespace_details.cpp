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

#include "mongo/pch.h"

#include <algorithm>
#include <list>
#include <map>
#include <vector>
#include <utility>

#include <boost/filesystem/operations.hpp>

#include "mongo/base/init.h"
#include "mongo/base/units.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/collection.h"
#include "mongo/db/cursor.h"
#include "mongo/db/database.h"
#include "mongo/db/databaseholder.h"
#include "mongo/db/json.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/db/oplog.h"
#include "mongo/db/relock.h"
#include "mongo/db/txn_context.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/storage/dbt.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/txn.h"
#include "mongo/db/storage/key.h"
#include "mongo/db/oplog_helpers.h"
#include "mongo/db/repl/rs_optime.h"
#include "mongo/db/repl/rs.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/s/d_logic.h"
#include "mongo/scripting/engine.h"

namespace mongo {


} // namespace mongo
