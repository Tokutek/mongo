/**
*    Copyright (C) 2012 Tokutek Inc.
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

#ifndef TOKUDB_ENV_H
#define TOKUDB_ENV_H

#include <tokudb.h>

#include "db/index.h"
#include "db/toku/invariant.h"

namespace toku {
    // if using txns and logging, begin the txn. else, set it to NULL
    void env_maybe_begin_txn(DB_TXN **txn);

    // if using txns and logging, commit the txn, else do nothing.
    void env_maybe_commit_txn(DB_TXN *txn);

    // given an idx, get a shared open DB handle
    // effect: creates db if it didn't exist
    DB *env_get_db_handle_by_idx(const mongo::IndexDetails &idx);

    // given an idx, drop the associated db from the environment
    void env_drop_index(const mongo::IndexDetails &idx);

    // shutdown by closing all dictionaries and the environment
    void env_shutdown(void);

} /* namespace toku */

#endif /* TOKUDB_ENV_H */
