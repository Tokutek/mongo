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

#include "txn_context.h"

#include "mongo/pch.h"

#include <db.h>
#include "mongo/db/storage/env.h"

namespace mongo {

    TxnContext::TxnContext(const TxnContext *parent, int txnFlags)
            : _txn(&parent->_txn, txnFlags)
    {
    }

    TxnContext::~TxnContext() {
    }

    void TxnContext::commit(int flags) {
    }

    void TxnContext::abort() {
    }
} // namespace mongo
