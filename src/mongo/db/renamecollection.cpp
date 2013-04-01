/** @file touch.cpp
    compaction of deleted space in pdfiles (datafiles)
*/

/**
 *    Copyright (C) 2012 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,b
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "mongo/pch.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/client.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/commands.h"
#include "mongo/db/db.h"
#include "mongo/db/instance.h"
#include "mongo/db/ops/insert.h"

namespace mongo {

    class CmdRenameCollection : public Command {
    public:
        CmdRenameCollection() : Command( "renameCollection" ) {}
        virtual bool adminOnly() const {
            return true;
        }
        virtual bool requiresAuth() { return false; } // do our own auth
        virtual bool slaveOk() const {
            return false;
        }
        virtual LockType locktype() const { return WRITE; }
        virtual bool lockGlobally() const { return true; }
        virtual bool logTheOp() {
            return true; // can't log steps when doing fast rename within a db, so always log the op rather than individual steps comprising it.
        }
        virtual void help( stringstream &help ) const {
            help << " example: { renameCollection: foo.a, to: bar.b }";
        }
        virtual bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            string source = cmdObj.getStringField( name.c_str() );
            string target = cmdObj.getStringField( "to" );
            uassert(15967,"invalid collection name: " + target, NamespaceString::validCollectionName(target.c_str()));
            if ( source.empty() || target.empty() ) {
                errmsg = "invalid command syntax";
                return false;
            }

            bool capped = false;
            long long size = 0;
            {
                Client::Context ctx( source ); // auths against source
                NamespaceDetails *nsd = nsdetails( source.c_str() );
                uassert( 10026 ,  "source namespace does not exist", nsd );
                capped = nsd->isCapped();
                // TODO: Get the capped size
            }

            Client::Context ctx( target ); //auths against target

            if ( nsdetails( target.c_str() ) ) {
                uassert( 10027 ,  "target namespace exists", cmdObj["dropTarget"].trueValue() );
                BSONObjBuilder bb( result.subobjStart( "dropTarget" ) );
                dropCollection( target , errmsg , bb );
                bb.done();
                if ( errmsg.size() > 0 )
                    return false;
            }


            // if we are renaming in the same database, just
            // rename the namespace and we're done.
            {
                char from[256];
                nsToDatabase( source.c_str(), from );
                char to[256];
                nsToDatabase( target.c_str(), to );
                if ( strcmp( from, to ) == 0 ) {
                    renameNamespace( source.c_str(), target.c_str(), cmdObj["stayTemp"].trueValue() );
                    // make sure we drop counters etc
                    Top::global.collectionDropped( source );
                    return true;
                }
            }

            // renaming across databases, so we must copy all
            // the data and then remove the source collection.
            BSONObjBuilder spec;
            if ( capped ) {
                spec.appendBool( "capped", true );
                spec.append( "size", double( size ) );
            }
            if ( !userCreateNS( target.c_str(), spec.done(), errmsg , false) )
                return false;

            auto_ptr< DBClientCursor > c;
            DBDirectClient bridge;

            {
                c = bridge.query( source, BSONObj(), 0, 0, 0, fromRepl ? QueryOption_SlaveOk : 0 );
            }
            while( 1 ) {
                {
                    if ( !c->more() )
                        break;
                }
                BSONObj o = c->next();
                insertObject( target.c_str(), o );
            }

            char cl[256];
            nsToDatabase( source.c_str(), cl );
            string sourceIndexes = string( cl ) + ".system.indexes";
            nsToDatabase( target.c_str(), cl );
            string targetIndexes = string( cl ) + ".system.indexes";
            {
                c = bridge.query( sourceIndexes, QUERY( "ns" << source ), 0, 0, 0, fromRepl ? QueryOption_SlaveOk : 0 );
            }
            while( 1 ) {
                {
                    if ( !c->more() )
                        break;
                }
                BSONObj o = c->next();
                BSONObjBuilder b;
                BSONObjIterator i( o );
                while( i.moreWithEOO() ) {
                    BSONElement e = i.next();
                    if ( e.eoo() )
                        break;
                    if ( strcmp( e.fieldName(), "ns" ) == 0 ) {
                        b.append( "ns", target );
                    }
                    else {
                        b.append( e );
                    }
                }
                BSONObj n = b.done();
                insertObject( targetIndexes.c_str(), n );
            }

            {
                Client::Context ctx( source );
                dropCollection( source, errmsg, result );
            }
            return true;
        }
    } cmdrenamecollection;
}
