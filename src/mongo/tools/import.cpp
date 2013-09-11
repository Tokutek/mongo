// import.cpp

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

#include <boost/algorithm/string.hpp>
#include <boost/filesystem/operations.hpp>
#include <fstream>
#include <iostream>

#include "mongo/base/initializer.h"
#include "mongo/db/json.h"
#include "mongo/db/namespacestring.h"
#include "mongo/tools/mongoimport_options.h"
#include "mongo/tools/tool.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/text.h"
#include "mongo/base/initializer.h"
#include "mongo/client/remote_loader.h"

using namespace mongo;
using std::string;
using std::stringstream;

class Import : public Tool {

    enum Type { JSON , CSV , TSV };
    Type _type;

    const char * _sep;
    bool _doBulkLoad;
    static const int BUF_SIZE;

    void csvTokenizeRow(const string& row, vector<string>& tokens) {
        bool inQuotes = false;
        bool prevWasQuote = false;
        bool tokenQuoted = false;
        string curtoken = "";
        for (string::const_iterator it = row.begin(); it != row.end(); ++it) {
            char element = *it;
            if (element == '"') {
                if (!inQuotes) {
                    inQuotes = true;
                    tokenQuoted = true;
                    curtoken = "";
                } else {
                    if (prevWasQuote) {
                        curtoken += "\"";
                        prevWasQuote = false;
                    } else {
                        prevWasQuote = true;
                    }
                }
            } else {
                if (inQuotes && prevWasQuote) {
                    inQuotes = false;
                    prevWasQuote = false;
                    tokens.push_back(curtoken);
                }

                if (element == ',' && !inQuotes) {
                    if (!tokenQuoted) { // If token was quoted, it's already been added
                        boost::trim(curtoken);
                        tokens.push_back(curtoken);
                    }
                    curtoken = "";
                    tokenQuoted = false;
                } else {
                    curtoken += element;
                }
            }
        }
        if (!tokenQuoted || (inQuotes && prevWasQuote)) {
            boost::trim(curtoken);
            tokens.push_back(curtoken);
        }
    }

    void _append( BSONObjBuilder& b , const string& fieldName , const string& data ) {
        if (mongoImportGlobalParams.ignoreBlanks && data.size() == 0)
            return;

        if ( b.appendAsNumber( fieldName , data ) )
            return;

        // TODO: other types?
        b.append ( fieldName , data );
    }

    /*
     * Reads one line from in into buf.
     * Returns the number of bytes that should be skipped - the caller should
     * increment buf by this amount.
     */
    int getLine(istream* in, char* buf) {
        if (mongoImportGlobalParams.jsonArray) {
            in->read(buf, BUF_SIZE);
            uassert(13295, "JSONArray file too large", (in->rdstate() & ios_base::eofbit));
            buf[ in->gcount() ] = '\0';
        }
        else {
            in->getline( buf , BUF_SIZE );
            if ((in->rdstate() & ios_base::eofbit) && (in->rdstate() & ios_base::failbit)) {
                // this is the last line, and it's empty (not even a newline)
                buf[0] = '\0';
                return 0;
            }

            uassert(16329, str::stream() << "read error, or input line too long (max length: "
                    << BUF_SIZE << ")", !(in->rdstate() & ios_base::failbit));
            LOG(1) << "got line:" << buf << endl;
        }
        uassert( 10263 ,  "unknown error reading file" ,
                 (!(in->rdstate() & ios_base::badbit)) &&
                 (!(in->rdstate() & ios_base::failbit) || (in->rdstate() & ios_base::eofbit)) );

        int numBytesSkipped = 0;
        if (strncmp("\xEF\xBB\xBF", buf, 3) == 0) { // UTF-8 BOM (notepad is stupid)
            buf += 3;
            numBytesSkipped += 3;
        }

        uassert(13289, "Invalid UTF8 character detected", isValidUTF8(buf));
        return numBytesSkipped;
    }

    /*
     * Parses a BSON object out of a JSON array.
     * Returns number of bytes processed on success and -1 on failure.
     */
    int parseJSONArray(char* buf, BSONObj& o) {
        int len = 0;
        while (buf[0] != '{' && buf[0] != '\0') {
            len++;
            buf++;
        }
        if (buf[0] == '\0')
            return -1;

        int jslen;
        try {
            o = fromjson(buf, &jslen);
        } catch ( MsgAssertionException& e ) {
            uasserted(13293, string("BSON representation of supplied JSON array is too large: ") + e.what());
        }
        len += jslen;

        return len;
    }

    /*
     * Parses one object from the input file.  This usually corresponds to one line in the input
     * file, unless the file is a CSV and contains a newline within a quoted string entry.
     * Returns a true if a BSONObj was successfully created and false if not.
     */
    bool parseRow(istream* in, BSONObj& o, int& numBytesRead) {
        boost::scoped_array<char> buffer(new char[BUF_SIZE+2]);
        char* line = buffer.get();

        numBytesRead = getLine(in, line);
        line += numBytesRead;

        if (line[0] == '\0') {
            return false;
        }
        numBytesRead += strlen( line );

        if (_type == JSON) {
            // Strip out trailing whitespace
            char * end = ( line + strlen( line ) ) - 1;
            while ( end >= line && isspace(*end) ) {
                *end = 0;
                end--;
            }
            try {
                o = fromjson( line );
            } catch ( MsgAssertionException& e ) {
                uasserted(13504, string("BSON representation of supplied JSON is too large: ") + e.what());
            }
            return true;
        }

        vector<string> tokens;
        if (_type == CSV) {
            string row;
            bool inside_quotes = false;
            size_t last_quote = 0;
            while (true) {
                string lineStr(line);
                // Deal with line breaks in quoted strings
                last_quote = lineStr.find_first_of('"');
                while (last_quote != string::npos) {
                    inside_quotes = !inside_quotes;
                    last_quote = lineStr.find_first_of('"', last_quote+1);
                }

                row.append(lineStr);

                if (inside_quotes) {
                    row.append("\n");
                    int num = getLine(in, line);
                    line += num;
                    numBytesRead += num;

                    uassert (15854, "CSV file ends while inside quoted field", line[0] != '\0');
                    numBytesRead += strlen( line );
                } else {
                    break;
                }
            }
            // now 'row' is string corresponding to one row of the CSV file
            // (which may span multiple lines) and represents one BSONObj
            csvTokenizeRow(row, tokens);
        }
        else {  // _type == TSV
            while (line[0] != '\t' && isspace(line[0])) { // Strip leading whitespace, but not tabs
                line++;
            }

            boost::split(tokens, line, boost::is_any_of(_sep));
        }

        // Now that the row is tokenized, create a BSONObj out of it.
        BSONObjBuilder b;
        unsigned int pos=0;
        for (vector<string>::iterator it = tokens.begin(); it != tokens.end(); ++it) {
            string token = *it;
            if (mongoImportGlobalParams.headerLine) {
                toolGlobalParams.fields.push_back(token);
            }
            else {
                string name;
                if (pos < toolGlobalParams.fields.size()) {
                    name = toolGlobalParams.fields[pos];
                }
                else {
                    stringstream ss;
                    ss << "field" << pos;
                    name = ss.str();
                }
                pos++;

                _append( b , name , token );
            }
        }
        o = b.obj();
        return true;
    }

public:
    Import() : Tool() {
        _type = JSON;
    }

    virtual void printHelp( ostream & out ) {
        printMongoImportHelp(toolsOptions, &out);
    }

    unsigned long long lastErrorFailures;

    /** @return true if ok */
    bool checkLastError() { 
        string s = conn().getLastError();
        if( !s.empty() ) { 
            if( str::contains(s,"uplicate") ) {
                // we don't want to return an error from the mongoimport process for
                // dup key errors
                log() << s << endl;
            }
            else {
                lastErrorFailures++;
                log() << "error: " << s << endl;
                return false;
            }
        }
        return true;
    }

    int run() {
        long long fileSize = 0;
        int headerRows = 0;

        istream * in = &cin;

        ifstream file(mongoImportGlobalParams.filename.c_str(), ios_base::in);

        if (mongoImportGlobalParams.filename.size() > 0 &&
            mongoImportGlobalParams.filename != "-") {
            if ( ! boost::filesystem::exists(mongoImportGlobalParams.filename) ) {
                error() << "file doesn't exist: " << mongoImportGlobalParams.filename << endl;
                return -1;
            }
            in = &file;
            fileSize = boost::filesystem::file_size(mongoImportGlobalParams.filename);
        }

        // check if we're actually talking to a machine that can write
        if (!isMaster()) {
            return -1;
        }

        string ns;

        try {
            ns = getNS();
        }
        catch (...) {
            printHelp(cerr);
            return -1;
        }

        LOG(1) << "ns: " << ns << endl;

        if (mongoImportGlobalParams.drop) {
            log() << "dropping: " << ns << endl;
            conn().dropCollection(ns.c_str());
        }

        _doBulkLoad = !mongoImportGlobalParams.upsert;
        if (!_doBulkLoad) {
            warning() << "not using bulk load because either upsert/upsertFields was specified" << endl;
        }

        if (mongoImportGlobalParams.type == "json")
            _type = JSON;
        else if (mongoImportGlobalParams.type == "csv") {
            _type = CSV;
            _sep = ",";
        }
        else if (mongoImportGlobalParams.type == "tsv") {
            _type = TSV;
            _sep = "\t";
        }
        else {
            error() << "don't know what type [" << mongoImportGlobalParams.type << "] is" << endl;
            return -1;
        }

        if (_type == CSV || _type == TSV) {
            if (mongoImportGlobalParams.headerLine) {
                headerRows = 1;
            }
            else {
                if (!toolGlobalParams.fieldsSpecified) {
                    throw UserException(9998, "You need to specify fields or have a headerline to "
                                              "import this file type");
                }
            }
        }

        time_t start = time(0);
        LOG(1) << "filesize: " << fileSize << endl;
        ProgressMeter pm( fileSize );
        int num = 0;
        int lastNumChecked = num;
        int errors = 0;
        lastErrorFailures = 0;
        int len = 0;
        // buffer and line are only used when parsing a jsonArray
        boost::scoped_array<char> buffer(new char[BUF_SIZE+2]);
        char* line = buffer.get();

        scoped_ptr<RemoteLoader> loader;
        if (_doBulkLoad) {
            // Pass no indexes or collection options, since this tool has no
            // way of specifying either.
            NamespaceString n(ns);
            loader.reset(new RemoteLoader(conn(), n.db, n.coll, vector<BSONObj>(), BSONObj()));
        }
        while (mongoImportGlobalParams.jsonArray || in->rdstate() == 0) {
            try {
                BSONObj o;
                if (mongoImportGlobalParams.jsonArray) {
                    int bytesProcessed = 0;
                    if (line == buffer.get()) { // Only read on first pass - the whole array must be on one line.
                        bytesProcessed = getLine(in, line);
                        line += bytesProcessed;
                        len += bytesProcessed;
                    }
                    if ((bytesProcessed = parseJSONArray(line, o)) < 0) {
                        len += bytesProcessed;
                        break;
                    }
                    len += bytesProcessed;
                    line += bytesProcessed;
                }
                else {
                    if (!parseRow(in, o, len)) {
                        continue;
                    }
                }

                if (mongoImportGlobalParams.headerLine) {
                    mongoImportGlobalParams.headerLine = false;
                }
                else if (mongoImportGlobalParams.doimport) {
                    bool doUpsert = mongoImportGlobalParams.upsert;
                    BSONObjBuilder b;
                    if (mongoImportGlobalParams.upsert) {
                        for (vector<string>::const_iterator it=mongoImportGlobalParams.upsertFields.begin(), end=mongoImportGlobalParams.upsertFields.end(); it!=end; ++it) {
                            BSONElement e = o.getFieldDotted(it->c_str());
                            if (e.eoo()) {
                                doUpsert = false;
                                break;
                            }
                            b.appendAs(e, *it);
                        }
                    }

                    if (doUpsert) {
                        conn().update(ns, Query(b.obj()), o, true);
                    }
                    else {
                        conn().insert(ns.c_str(), o);
                    }

                    if (num < 10) {
                        // we absolutely want to check the first and last op of the batch. we do
                        // a few more as that won't be too time expensive.
                        checkLastError();
                        lastNumChecked = num;
                    }
                }

                num++;
            }
            catch ( std::exception& e ) {
                log() << "exception:" << e.what() << endl;
                log() << line << endl;
                errors++;

                if (mongoImportGlobalParams.stopOnError || mongoImportGlobalParams.jsonArray)
                    break;
            }

            if ( pm.hit( len + 1 ) ) {
                log() << "\t\t\t" << num << "\t" << ( num / ( time(0) - start ) ) << "/second" << endl;
            }
        }
        if (loader) {
            loader->commit();
        }

        // this is for two reasons: to wait for all operations to reach the server and be processed, and this will wait until all data reaches the server,
        // and secondly to check if there were an error (on the last op)
        if( lastNumChecked+1 != num ) { // avoid redundant log message if already reported above
            log() << "check " << lastNumChecked << " " << num << endl;
            checkLastError();
        }

        bool hadErrors = lastErrorFailures || errors;

        // the message is vague on lastErrorFailures as we don't call it on every single operation. 
        // so if we have a lastErrorFailure there might be more than just what has been counted.
        log() << (lastErrorFailures ? "tried to import " : "imported ") << ( num - headerRows ) << " objects" << endl;

        if ( !hadErrors )
            return 0;

        error() << "encountered " << (lastErrorFailures?"at least ":"") << lastErrorFailures+errors <<  " error(s)" << ( lastErrorFailures+errors == 1 ? "" : "s" ) << endl;
        return -1;
    }
};

const int Import::BUF_SIZE(1024 * 1024 * 16);

REGISTER_MONGO_TOOL(Import);
