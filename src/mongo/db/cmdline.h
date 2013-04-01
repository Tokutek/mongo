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

#pragma once

#include "mongo/pch.h"
#include "jsobj.h"

namespace boost {
    namespace program_options {
        class options_description;
        class positional_options_description;
        class variables_map;
    }
}

namespace mongo {

#ifdef MONGO_SSL
    class SSLManager;
#endif

    /* command line options
    */
    /* concurrency: OK/READ */
    struct CmdLine {

        CmdLine();

        string binaryName;     // mongod or mongos
        string cwd;            // cwd of when process started

        // this is suboptimal as someone could rename a binary.  todo...
        bool isMongos() const { return binaryName == "mongos"; }

        int port;              // --port
        enum {
            DefaultDBPort = 27017,
            ConfigServerPort = 27019,
            ShardServerPort = 27018
        };
        bool isDefaultPort() const { return port == DefaultDBPort; }

        string bind_ip;        // --bind_ip
        bool rest;             // --rest
        bool jsonp;            // --jsonp

        string _replSet;       // --replSet[/<seedlist>]
        string ourSetName() const {
            string setname;
            size_t sl = _replSet.find('/');
            if( sl == string::npos )
                return _replSet;
            return _replSet.substr(0, sl);
        }
        bool usingReplSets() const { return !_replSet.empty(); }

        string rsIndexPrefetch;// --indexPrefetch

        // for master/slave replication
        string source;         // --source
        string only;           // --only

        bool quiet;            // --quiet
        bool noTableScan;      // --notablescan no table scans allowed

        bool configsvr;        // --configsvr

        bool quota;            // --quota
        int quotaFiles;        // --quotaFiles
        bool cpu;              // --cpu show cpu time periodically

        uint32_t logFlushPeriod; // group/batch commit interval ms


        bool objcheck;         // --objcheck

        long long oplogSize;   // --oplogSize
        int defaultProfile;    // --profile
        int slowMS;            // --time in ms that is "slow"
        int defaultLocalThresholdMillis;    // --localThreshold in ms to consider a node local
        bool moveParanoia;     // for move chunk paranoia
        double syncdelay;      // seconds between fsyncs

        bool noUnixSocket;     // --nounixsocket
        bool doFork;           // --fork
        string socket;         // UNIX domain socket directory

        bool keyFile;

#ifndef _WIN32
        pid_t parentProc;      // --fork pid of initial process
        pid_t leaderProc;      // --fork pid of leader process
#endif

#ifdef MONGO_SSL
        bool sslOnNormalPorts;      // --sslOnNormalPorts
        string sslPEMKeyFile;       // --sslPEMKeyFile
        string sslPEMKeyPassword;   // --sslPEMKeyPassword

        SSLManager* sslServerManager; // currently leaks on close
#endif
        
        // TokuDB variables
        bool directio;
        bool gdb;
        uint64_t cacheSize;
        uint32_t checkpointPeriod;
        uint32_t cleanerPeriod;
        uint32_t cleanerIterations;
        uint64_t lockTimeout;

        static void launchOk();

        static void addGlobalOptions( boost::program_options::options_description& general ,
                                      boost::program_options::options_description& hidden ,
                                      boost::program_options::options_description& ssl_options );

        static void addWindowsOptions( boost::program_options::options_description& windows ,
                                       boost::program_options::options_description& hidden );


        static void parseConfigFile( istream &f, stringstream &ss);
        /**
         * @return true if should run program, false if should exit
         */
        static bool store( int argc , char ** argv ,
                           boost::program_options::options_description& visible,
                           boost::program_options::options_description& hidden,
                           boost::program_options::positional_options_description& positional,
                           boost::program_options::variables_map &output );

        time_t started;
    };

    // todo move to cmdline.cpp?
    inline CmdLine::CmdLine() :
        port(DefaultDBPort), rest(false), jsonp(false), quiet(false),
        noTableScan(false),
        configsvr(false), quota(false), quotaFiles(8), cpu(false),
        logFlushPeriod(100), // 0 means fsync every transaction, 100 means fsync log once every 100 ms
        objcheck(false), oplogSize(0), defaultProfile(0),
        slowMS(100), defaultLocalThresholdMillis(15), moveParanoia( true ),
        syncdelay(60), noUnixSocket(false), doFork(0), socket("/tmp"),
        directio(false), cacheSize(0), checkpointPeriod(60), cleanerPeriod(2),
        cleanerIterations(5), lockTimeout(4000)
    {
        started = time(0);

#ifdef MONGO_SSL
        sslOnNormalPorts = false;
        sslServerManager = 0;
#endif
    }
            
    extern CmdLine cmdLine;

    void setupLaunchSignals();
    void setupCoreSignals();

    string prettyHostName();

    void printCommandLineOpts();

    /**
     * used for setParameter command
     * so you can write validation code that lives with code using it
     * rather than all in the command place
     * also lets you have mongos or mongod specific code
     * without pulling it all sorts of things
     */
    class ParameterValidator {
    public:
        ParameterValidator( const string& name );
        virtual ~ParameterValidator() {}

        virtual bool isValid( BSONElement e , string& errmsg ) const = 0;

        static ParameterValidator * get( const string& name );

    private:
        const string _name;
    };

}

