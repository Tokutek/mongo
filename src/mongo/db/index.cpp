/** @file index.cpp */

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

#include <boost/checked_delete.hpp>

#include "pch.h"
#include "namespace.h"
#include "index.h"
#include "indexcursor.h"
#include "background.h"
#include "repl/rs.h"
#include "ops/delete.h"
#include "mongo/util/scopeguard.h"

#include "db/toku/env.h"
#include "db/toku/index.h"

namespace mongo {

    IndexDetails::IndexDetails(const BSONObj &info) : _info(info.getOwned()) {
        tokulog() << "Opening IndexDetails " << _info["name"].String() << endl;
        // Open the dictionary. Creates it if necessary.
        _db = storage::db_open(_info["name"].String(), true);
    }

    IndexDetails::~IndexDetails() {
        tokulog() << "Closing IndexDetails " << _info["name"].String() << endl;
        storage::db_close(_db);
    }

    int IndexDetails::keyPatternOffset( const string& key ) const {
        BSONObjIterator i( keyPattern() );
        int n = 0;
        while ( i.more() ) {
            BSONElement e = i.next();
            if ( key == e.fieldName() )
                return n;
            n++;
        }
        return -1;
    }

    /* delete this index.  does NOT clean up the system catalog
       (system.indexes or system.namespaces) -- only NamespaceIndex.
    */
    void IndexDetails::kill_idx() {
        string ns = indexNamespace(); // e.g. foo.coll.$ts_1
        try {

            string pns = parentNS(); // note we need a copy, as parentNS() won't work after the drop() below

            // clean up parent namespace index cache
            NamespaceDetailsTransient::get( pns.c_str() ).deletedIndex();

            string name = indexName();
            
            // tokudb: ensure the db is dropped in the environment using dropIndex
            //idxInterface().dropIndex(*this);

            /* important to catch exception here so we can finish cleanup below. */
            try {
                ::abort();
                //dropNS(ns.c_str());
            }
            catch(DBException& ) {
                log(2) << "IndexDetails::kill(): couldn't drop ns " << ns << endl;
            }
            ::abort();
#if 0
            head.setInvalid();
            info.setInvalid();

            // clean up in system.indexes.  we do this last on purpose.
            int n = removeFromSysIndexes(pns.c_str(), name.c_str());
            wassert( n == 1 );
#endif

        }
        catch ( DBException &e ) {
            log() << "exception in kill_idx: " << e << ", ns: " << ns << endl;
        }
    }

    void IndexDetails::getKeysFromObject( const BSONObj& obj, BSONObjSet& keys) const {
        getSpec().getKeys( obj, keys );
    }

    const IndexSpec& IndexDetails::getSpec() const {
        SimpleMutex::scoped_lock lk(NamespaceDetailsTransient::_qcMutex);
        ::abort();
        return NamespaceDetailsTransient::get_inlock( /* info.obj() */ BSONObj()["ns"].valuestr() ).getIndexSpec( this );
    }

    void IndexSpec::reset( const IndexDetails * details ) {
        _details = details;
        reset( details->info() );
    }

    void IndexSpec::reset( const BSONObj& _info ) {
        info = _info;
        keyPattern = info["key"].embeddedObjectUserCheck();
        if ( keyPattern.objsize() == 0 ) {
            out() << info.toString() << endl;
            verify(false);
        }
        _init();
    }
}
