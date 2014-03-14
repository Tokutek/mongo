// removetests.cpp : unit tests relating to removing documents
//

/**
 *    Copyright (C) 2012 10gen Inc.
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

#include "mongo/client/dbclientcursor.h"

#include "dbtests.h"
#include "mongo/db/ops/delete.h"

namespace RemoveTests {

    static const char * const ns = "unittests.removetests";
    static DBDirectClient client;
    
    /** Simple test for deleteIndexRange */
    class DeleteIndexRange {
    public:
        DeleteIndexRange() :
        _min( 4 ),
        _max( 8 ) {
        }
        void run() {
            for( int i = 0; i < 10; ++i ) {
                client.insert( ns, BSON( "_id" << i ) );
            }
            
            {
                // Remove _id range [_min, _max).
                Client::WriteContext ctx(ns, mongo::unittest::EMPTY_STRING);
                Client::Transaction transaction(DB_SERIALIZABLE);
                deleteIndexRange( ns,
                                  BSON( "_id" << _min ),
                                  BSON( "_id" << _max ),
                                  BSON( "_id" << 1 ) );
                transaction.commit();
            }

            // Check that the expected documents remain.
            ASSERT_EQUALS( expected(), docs() );
        }
    private:
        BSONArray expected() const {
            BSONArrayBuilder bab;
            for( int i = 0; i < _min; ++i ) {
                bab << BSON( "_id" << i );
            }
            for( int i = _max; i < 10; ++i ) {
                bab << BSON( "_id" << i );
            }
            return bab.arr();
        }
        BSONArray docs() const {
            auto_ptr<DBClientCursor> cursor =
                    client.query( ns, Query().hint( BSON( "_id" << 1 ) ) );
            BSONArrayBuilder bab;
            while( cursor->more() ) {
                bab << cursor->next();
            }
            return bab.arr();
        }
        int _min;
        int _max;
    };

    class All : public Suite {
    public:
        All() : Suite( "remove" ) {
        }
        void setupTests() {
            add<DeleteIndexRange>();
        }
    } myall;
    
} // namespace RemoveTests
