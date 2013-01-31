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
#include "mongo/util/mmap.h"

namespace mongo {

    // pdfile versions
    //const int PDFILE_VERSION = 4;
    //const int PDFILE_VERSION_MINOR = 5;

    //class DataFileHeader;
    //class Extent;
    //class Record;
    class Cursor;
    class OpDebug;

    void dropDatabase(string db);
    bool repairDatabase(string db, string &errmsg, bool preserveClonedFilesOnFailure = false, bool backupOriginalFiles = false);

    /* low level - only drops this ns */
    void dropNS(const string& dropNs);

    /* deletes this ns, indexes and cursors */
    void dropCollection( const string &name, string &errmsg, BSONObjBuilder &result );
    bool userCreateNS(const char *ns, BSONObj j, string& err, bool logForReplication, bool *deferIdIndex = 0);
    shared_ptr<Cursor> findTableScan(const char *ns, const BSONObj& order, const DiskLoc &startLoc=DiskLoc());

    bool isValidNS( const StringData& ns );


} // namespace mongo

#include "cursor.h"

namespace mongo {

    inline BSONObj DiskLoc::obj() const {
        ::abort(); return BSONObj();
        //return BSONObj::make(rec()->accessed());
    }

} // namespace mongo

#include "database.h"

namespace mongo {

    boost::intmax_t dbSize( const char *database );

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

    void ensureHaveIdIndex(const char *ns);

} // namespace mongo
