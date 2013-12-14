// collection.h

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

#pragma once

#include <db.h>

#include "mongo/pch.h"

#include "mongo/db/namespacestring.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/index.h"
#include "mongo/db/index_set.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/queryoptimizercursor.h"
#include "mongo/db/query_plan_selection_policy.h"
#include "mongo/db/querypattern.h"
#include "mongo/db/relock.h"
#include "mongo/db/storage/env.h"
#include "mongo/db/storage/dictionary.h"
#include "mongo/db/storage/builder.h"
#include "mongo/util/concurrency/simplerwlock.h"
#include "mongo/util/string_map.h"

namespace mongo {


} // namespace mongo
