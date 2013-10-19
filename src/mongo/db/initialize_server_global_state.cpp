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
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#include "mongo/pch.h"

#include "mongo/db/initialize_server_global_state.h"

#include <boost/filesystem/operations.hpp>

#ifndef _WIN32
#include <sys/types.h>
#include <sys/wait.h>
#endif

#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/security_key.h"
#include "mongo/db/cmdline.h"
#include "mongo/util/log.h"
#include "mongo/util/net/listen.h"
#include "mongo/util/processinfo.h"

namespace fs = boost::filesystem;

namespace mongo {

#ifndef _WIN32
    // support for exit value propagation with fork
    void launchSignal( int sig ) {
        if ( sig == SIGUSR2 ) {
            pid_t cur = getpid();
            
            if ( cur == cmdLine.parentProc || cur == cmdLine.leaderProc ) {
                // signal indicates successful start allowing us to exit
                _exit(0);
            } 
        }
    }

    static void setupLaunchSignals() {
        verify( signal(SIGUSR2 , launchSignal ) != SIG_ERR );
    }

    void CmdLine::launchOk() {
        if ( cmdLine.doFork ) {
            // killing leader will propagate to parent
            verify( kill( cmdLine.leaderProc, SIGUSR2 ) == 0 );
        }
    }
#endif

    bool initializeServerGlobalState(bool isMongodShutdownSpecialCase) {

        Listener::globalTicketHolder.resize( cmdLine.maxConns );

#ifndef _WIN32
        if (!fs::is_directory(cmdLine.socket)) {
            cout << cmdLine.socket << " must be a directory" << endl;
            return false;
        }

        if (cmdLine.doFork) {
            fassert(16447, !cmdLine.logpath.empty() || cmdLine.logWithSyslog);

            cout.flush();
            cerr.flush();

            cmdLine.parentProc = getpid();

            // facilitate clean exit when child starts successfully
            setupLaunchSignals();

            pid_t c = fork();
            if ( c ) {
                int pstat;
                waitpid(c, &pstat, 0);

                if ( WIFEXITED(pstat) ) {
                    if ( ! WEXITSTATUS(pstat) ) {
                        cout << "child process started successfully, parent exiting" << endl;
                    }

                    _exit( WEXITSTATUS(pstat) );
                }

                _exit(50);
            }

            if ( chdir("/") < 0 ) {
                cout << "Cant chdir() while forking server process: " << strerror(errno) << endl;
                ::_exit(-1);
            }
            setsid();

            cmdLine.leaderProc = getpid();

            pid_t c2 = fork();
            if ( c2 ) {
                int pstat;
                cout << "forked process: " << c2 << endl;
                waitpid(c2, &pstat, 0);

                if ( WIFEXITED(pstat) ) {
                    _exit( WEXITSTATUS(pstat) );
                }

                _exit(51);
            }

            // stdout handled in initLogging
            //fclose(stdout);
            //freopen("/dev/null", "w", stdout);

            fclose(stderr);
            fclose(stdin);

            FILE* f = freopen("/dev/null", "w", stderr);
            if ( f == NULL ) {
                cout << "Cant reassign stderr while forking server process: " << strerror(errno) << endl;
                return false;
            }

            f = freopen("/dev/null", "r", stdin);
            if ( f == NULL ) {
                cout << "Cant reassign stdin while forking server process: " << strerror(errno) << endl;
                return false;
            }
        }

        if (cmdLine.logWithSyslog) {
            StringBuilder sb;
            sb << cmdLine.binaryName << "." << cmdLine.port;
            Logstream::useSyslog( sb.str().c_str() );
        }
#endif
        if (!cmdLine.logpath.empty() && !isMongodShutdownSpecialCase) {
            fassert(16448, !cmdLine.logWithSyslog);
            string absoluteLogpath = boost::filesystem::absolute(
                    cmdLine.logpath, cmdLine.cwd).string();
            if (!initLogging(absoluteLogpath, cmdLine.logAppend)) {
                cout << "Bad logpath value: \"" << absoluteLogpath << "\"; terminating." << endl;
                return false;
            }
        }

        if (!cmdLine.pidFile.empty()) {
            writePidFile(cmdLine.pidFile);
        }

        if (!cmdLine.keyFile.empty()) {

            if (!setUpSecurityKey(cmdLine.keyFile)) {
                // error message printed in setUpPrivateKey
                return false;
            }

            noauth = false;
        }

        return true;
    }

    static void ignoreSignal( int sig ) {}

    void setupCoreSignals() {
#if !defined(_WIN32)
        verify( signal(SIGHUP , ignoreSignal ) != SIG_ERR );
        verify( signal(SIGUSR2, ignoreSignal ) != SIG_ERR );
#endif
    }

}  // namespace mongo
