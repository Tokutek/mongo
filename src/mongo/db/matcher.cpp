// matcher.cpp

/* Matcher is our boolean expression evaluator for "where" clauses */

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
#include "mongo/db/matcher.h"
#include "mongo/util/goodies.h"
#include "mongo/util/startup_test.h"
#include "mongo/scripting/engine.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/client.h"
#include "mongo/db/namespacestring.h"
#include "mongo/db/auth/authorization_session.h"

#ifdef USE_OLD_MATCHER
#include "matcher_old.cpp"
#endif

