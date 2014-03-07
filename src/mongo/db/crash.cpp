/**
*    Copyright (C) 2014 Tokutek Inc.
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

#include "mongo/db/crash.h"

#include <string.h>

#include <db.h>

#include "mongo/base/string_data.h"
#include "mongo/db/client.h"
#include "mongo/db/cmdline.h"
#include "mongo/db/curop.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/stacktrace.h"
#include "mongo/util/version.h"

namespace mongo {

    // The goal here is to provide as much information as possible without causing more problems.
    // We use stack buffers as much as possible to avoid calling new/malloc, and we try to dump the
    // information least likely to cause more problems up front, saving the more dangerous debugging
    // information for later (including some things that should take locks and are therefore pretty
    // risky to do).
    namespace crashdump {

        static void header() {
            rawOut(" ");
            rawOut("================================================================================");
            rawOut(" Fatal error detected");
            rawOut("================================================================================");
            rawOut(" ");
            rawOut("About to gather debugging information, please include all of the following along");
            rawOut("with logs from other servers in the cluster in a bug report.");
            rawOut(" ");
        }

        static void versionInfo() {
            char *p;
            char buf[1<<12];
            rawOut("--------------------------------------------------------------------------------");
            rawOut("Version info:");
            rawOut(" ");
            p = buf;
            p = stpcpy(p, "tokumxVersion: ");
            p = stpcpy(p, tokumxVersionString);
            rawOut(buf);
            p = buf;
            p = stpcpy(p, "gitVersion: ");
            p = stpcpy(p, gitVersion());
            rawOut(buf);
            p = buf;
            p = stpcpy(p, "tokukvVersion: ");
            p = stpcpy(p, tokukvVersion());
            rawOut(buf);
            p = buf;
            p = stpcpy(p, "sysInfo: ");
            p = stpcpy(p, sysInfoCstr());
            rawOut(buf);
            p = buf;
            p = stpcpy(p, "loaderFlags: ");
            p = stpcpy(p, loaderFlags());
            rawOut(buf);
            p = buf;
            p = stpcpy(p, "compilerFlags: ");
            p = stpcpy(p, compilerFlags());
            rawOut(buf);
            p = buf;
            p = stpcpy(p, "debug: ");
            if (debug) {
                p = stpcpy(p, "true");
            } else {
                p = stpcpy(p, "false");
            }
            rawOut(buf);
            rawOut(" ");
        }

        static void simpleStacktrace() {
            rawOut("--------------------------------------------------------------------------------");
            rawOut("Simple stacktrace:");
            rawOut(" ");
            printStackTrace();
            rawOut(" ");
        }

        static void tokukvBacktrace() {
            rawOut("--------------------------------------------------------------------------------");
            rawOut("TokuKV engine backtrace:");
            rawOut(" ");
            db_env_do_backtrace();
            rawOut(" ");
        }

        static void gdbStacktrace() {
            if (!cmdLine.gdbPath.empty()) {
                rawOut("--------------------------------------------------------------------------------");
                rawOut("GDB backtrace:");
                rawOut(" ");
                db_env_try_gdb_stack_trace(cmdLine.gdbPath.c_str());
                rawOut(" ");
            }
        }

        static void basicInfo() {
            header();
            versionInfo();
            simpleStacktrace();
            tokukvBacktrace();
            gdbStacktrace();
        }

        static void reason(const char *s) {
            rawOut("--------------------------------------------------------------------------------");
            rawOut("Crash reason:");
            rawOut(" ");
            rawOut(s);
            rawOut(" ");
        }

        static void parsedOpts() {
            rawOut("--------------------------------------------------------------------------------");
            rawOut("Parsed server options:");
            rawOut(" ");
            const BSONObj &opts = CmdLine::getParsedOpts();
            for (BSONObjIterator it(opts); it.more(); ) {
                const BSONElement &elt = it.next();
                rawOut(elt.toString());
            }
            rawOut(" ");
        }

        static void curOpInfo() {
            rawOut("--------------------------------------------------------------------------------");
            rawOut("Current operations in progress:");
            rawOut(" ");
            for (set<Client *>::const_iterator i = Client::clients.begin(); i != Client::clients.end(); i++) {
                Client *c = *i;
                if (c == NULL) {
                    rawOut("NULL pointer in clients map");
                    continue;
                }
                CurOp *co = c->curop();
                if (co == NULL) {
                    rawOut("NULL pointer in curop");
                    continue;
                }
                rawOut(co->info().toString());
            }
            rawOut(" ");
        }

        static void opDebugInfo() {
            rawOut("--------------------------------------------------------------------------------");
            rawOut("OpDebug info:");
            rawOut(" ");
            for (set<Client *>::const_iterator i = Client::clients.begin(); i != Client::clients.end(); i++) {
                Client *c = *i;
                if (c == NULL) {
                    rawOut("NULL pointer in clients map");
                    continue;
                }
                CurOp *co = c->curop();
                if (co == NULL) {
                    rawOut("NULL pointer in curop");
                    continue;
                }
                rawOut(co->debug().report(*co));
            }
            rawOut(" ");
        }

        static void extraInfo() {
            parsedOpts();
            curOpInfo();
            opDebugInfo();
        }

    } // namespace crashdump

    void dumpCrashInfo(const StringData &reason) {
        crashdump::basicInfo();
        char buf[1<<12];
        strncpy(buf, reason.rawData(), reason.size());
        buf[reason.size()] = '\0';
        crashdump::reason(buf);
        crashdump::extraInfo();
    }

    void dumpCrashInfo(const DBException &e) {
        crashdump::basicInfo();
        char buf[1<<12];
        snprintf(buf, 1<<12, "DBException code: %d what: %s", e.getCode(), e.what());
        crashdump::reason(buf);
        crashdump::extraInfo();
    }

} // namespace mongo
