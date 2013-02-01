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

/* pdfile.h

   Files:
     database.ns - namespace index
     database.1  - data files
     database.2
     ...
*/

#pragma once

#include "mongo/db/client.h"
#include "mongo/db/diskloc.h"
#include "mongo/db/jsobjmanipulator.h"
#include "mongo/db/namespace-inl.h"
#include "mongo/db/namespace_details-inl.h"
#include "mongo/db/namespacestring.h"
#include "mongo/util/log.h"

#include "mongo/db/cursor.h"

namespace mongo {

    // TODO: Get rid of this along with the DiskLoc class
    inline BSONObj DiskLoc::obj() const {
        ::abort(); return BSONObj();
        //return BSONObj::make(rec()->accessed());
    }

} // namespace mongo

#include "database.h"

namespace mongo {

    inline NamespaceIndex* nsindex(const char *ns) {
        Database *database = cc().database();
        verify( database );
        //memconcept::is(database, memconcept::concept::database, ns, sizeof(Database));
        DEV {
            char buf[256];
            nsToDatabase(ns, buf);
            if ( database->name != buf ) {
                out() << "ERROR: attempt to write to wrong database\n";
                out() << " ns:" << ns << '\n';
                out() << " database->name:" << database->name << endl;
                verify( database->name == buf );
            }
        }
        return &database->namespaceIndex;
    }

    inline NamespaceDetails* nsdetails(const char *ns) {
        // if this faults, did you set the current db first?  (Client::Context + dblock)
        NamespaceDetails *d = nsindex(ns)->details(ns);
        if( d ) {
            //memconcept::is(d, memconcept::concept::nsdetails, ns, sizeof(NamespaceDetails));
        }
        return d;
    }

} // namespace mongo
