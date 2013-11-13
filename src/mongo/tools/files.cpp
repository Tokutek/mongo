// files.cpp

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

#include "pch.h"
#include "client/gridfs.h"
#include "mongo/base/initializer.h"
#include "mongo/client/dbclientcursor.h"

#include "tool.h"
#include "pcrecpp.h"

#include <fstream>
#include <iostream>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

using namespace mongo;

namespace fs = boost::filesystem;
namespace po = boost::program_options;

class Files : public Tool {
public:
    Files() : Tool( "files" ) {
        add_options()
        ( "local,l", po::value<string>(), "local filename for put|get (default is to use the same name as 'gridfs filename')")
        ( "type,t", po::value<string>(), "MIME type for put (default is to omit)")
        ( "replace,r", "Remove other files with same name after PUT")
        ;
        add_hidden_options()
        ( "command" , po::value<string>() , "command (list|search|put|get)" )
        ( "file" , po::value<string>() , "filename for get|put" )
        ;
        addPositionArg( "command" , 1 );
        addPositionArg( "file" , 2 );
    }

    virtual void printExtraHelp( ostream & out ) {
        out << "Browse and modify a GridFS filesystem.\n" << endl;
        out << "usage: " << _name << " [options] command [gridfs filename]" << endl;
        out << "command:" << endl;
        out << "  one of (list|search|put|get)" << endl;
        out << "  list - list all files.  'gridfs filename' is an optional prefix " << endl;
        out << "         which listed filenames must begin with." << endl;
        out << "  search - search all files. 'gridfs filename' is a substring " << endl;
        out << "           which listed filenames must contain." << endl;
        out << "  put - add a file with filename 'gridfs filename'" << endl;
        out << "  get - get a file with filename 'gridfs filename'" << endl;
        out << "  delete - delete all files with filename 'gridfs filename'" << endl;
    }

    void display( GridFS * grid , BSONObj obj ) {
        auto_ptr<DBClientCursor> c = grid->list( obj );
        while ( c->more() ) {
            BSONObj obj = c->next();
            cout
                    << obj["filename"].str() << "\t"
                    << (long)obj["length"].number()
                    << endl;
        }
    }

    int run() {
        string cmd = getParam( "command" );
        if ( cmd.size() == 0 ) {
            cerr << "ERROR: need command" << endl << endl;
            printHelp(cout);
            return -1;
        }

        GridFS g( conn() , _db );

        string filename = getParam( "file" );

        if ( cmd == "list" ) {
            BSONObjBuilder b;
            if ( filename.size() ) {
                b.appendRegex( "filename" , (string)"^" +
                               pcrecpp::RE::QuoteMeta( filename ) );
            }
            
            display( &g , b.obj() );
            return 0;
        }

        if ( filename.size() == 0 ) {
            cerr << "ERROR: need a filename" << endl << endl;
            printHelp(cout);
            return -1;
        }

        if ( cmd == "search" ) {
            BSONObjBuilder b;
            b.appendRegex( "filename" , filename );
            display( &g , b.obj() );
            return 0;
        }

        if ( cmd == "get" ) {
            GridFile f = g.findFile( filename );
            if ( ! f.exists() ) {
                cerr << "ERROR: file not found" << endl;
                return -2;
            }

            string out = getParam("local", f.getFilename());
            f.write( out );

            if (out != "-")
                cout << "done write to: " << out << endl;

            return 0;
        }

        if ( cmd == "put" ) {
            const string& infile = getParam("local", filename);
            const string& type = getParam("type", "");

            putFile(g, infile, filename, type);

            conn().getLastError();
            cout << "done!" << endl;
            return 0;
        }

        if ( cmd == "putdir" ) {
            const fs::path path(getParam("local", filename));
            const string& type = getParam("type", "");

            const fs::recursive_directory_iterator end;
            for (fs::recursive_directory_iterator it(path); it != end; ++it) {
                if (fs::is_regular_file(it->status())) {
                    bool ok = putFile(g, it->path(), it->path(), type);
                    if (!ok) {
                        cerr << "error putting file " << it->path().generic_string() << endl;
                        return 1;
                    }
                }
            }

            conn().getLastError();
            cout << "done!" << endl;
            return 0;
        }

        if ( cmd == "delete" ) {
            g.removeFile(filename);
            conn().getLastError();
            cout << "done!" << endl;
            return 0;
        }

        cerr << "ERROR: unknown command '" << cmd << "'" << endl << endl;
        printHelp(cout);
        return -1;
    }

  private:
    bool putFile(GridFS &g, const fs::path& infile, const fs::path& filename, const string& type) {
        BSONObj file = g.storeFile(infile.string(), filename.generic_string(), type);
        cout << "added file: " << file << endl;

        if (hasParam("replace")) {
            auto_ptr<DBClientCursor> cursor = conn().query(_db+".fs.files", BSON("filename" << filename.generic_string() << "_id" << NE << file["_id"] ));
            while (cursor->more()) {
                BSONObj o = cursor->nextSafe();
                conn().remove(_db+".fs.files", BSON("_id" << o["_id"]));
                conn().remove(_db+".fs.chunks", BSON("_id" << o["_id"]));
                cout << "removed file: " << o << endl;
            }

        }
        return true;
    }
};

int main( int argc , char ** argv, char** envp ) {
    mongo::runGlobalInitializersOrDie(argc, argv, envp);
    Files f;
    return f.main( argc , argv );
}
