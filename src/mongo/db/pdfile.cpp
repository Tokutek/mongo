// pdfile.cpp

/**
*    Copyright (C) 2008 10gen Inc.
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

/*
todo:
_ table scans must be sequential, not next/prev pointers
_ coalesce deleted
_ disallow system* manipulations from the database.
*/

#include "pch.h"
#include "pdfile.h"
#include "db.h"
#include "../util/hashtab.h"
#include "../util/file_allocator.h"
#include "../util/processinfo.h"
#include "../util/file.h"
#include "btree.h"
#include <algorithm>
#include <list>
#include "repl.h"
#include "dbhelpers.h"
#include "namespace-inl.h"
#include "extsort.h"
#include "curop-inl.h"
#include "background.h"
#include "compact.h"
#include "ops/delete.h"
#include "instance.h"
#include "replutil.h"
#include "mongo/db/lasterror.h"

#include <boost/filesystem/operations.hpp>

namespace mongo {

    // TODO SERVER-4328
    bool inDBRepair = false;
    struct doingRepair {
        doingRepair() {
            verify( ! inDBRepair );
            inDBRepair = true;
        }
        ~doingRepair() {
            inDBRepair = false;
        }
    };


    /* ----------------------------------------- */
#ifdef _WIN32
    string dbpath = "\\data\\db\\";
#else
    string dbpath = "/data/db/";
#endif
    const char FREELIST_NS[] = ".$freelist";
    bool directoryperdb = false;
    string repairpath;
    string pidfilepath;

    // TODO: Get rid of the data theDataFileMgr.
    //DataFileMgr theDataFileMgr;

    DatabaseHolder _dbHolder;
    int MAGIC = 0x1000;

    DatabaseHolder& dbHolderUnchecked() {
        return _dbHolder;
    }

} // namespace mongo
