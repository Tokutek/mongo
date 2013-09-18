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

#include <db.h>

#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/curop.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/server_options.h"
#include "mongo/db/storage_options.h"
#include "mongo/db/storage/env.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/paths.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/stacktrace.h"
#include "mongo/util/version.h"


#if MONGO_HAVE_HEADER_LIMITS_H
  #include <limits.h>
#endif
#if MONGO_HAVE_HEADER_UNISTD_H
  #include <unistd.h>
#endif
#if MONGO_HAVE_HEADER_SYS_RESOURCE_H
  #include <sys/resource.h>
#endif
#if MONGO_HAVE_HEADER_SYS_VFS_H
  #include <sys/vfs.h>
  #ifndef MONGO_CRASH_HAVE_STATFS_IMPL
    #define MONGO_CRASH_HAVE_STATFS_IMPL 1
  #endif
#endif
#if MONGO_HAVE_HEADER_SYS_STATFS_H
  #include <sys/statfs.h>
  #ifndef MONGO_CRASH_HAVE_STATFS_IMPL
    #define MONGO_CRASH_HAVE_STATFS_IMPL 1
  #endif
#endif
#if MONGO_HAVE_HEADER_SYS_PARAM_H && MONGO_HAVE_HEADER_SYS_MOUNT_H
  #include <sys/mount.h>
  #include <sys/param.h>
  #ifndef MONGO_CRASH_HAVE_STATFS_IMPL
    #define MONGO_CRASH_HAVE_STATFS_IMPL 1
  #endif
  #if MONGO_HAVE_HEADER_SYS_DISKLABEL_H
    #include <sys/disklabel.h>
  #endif
#endif
#if MONGO_HAVE_HEADER_LINUX_MAGIC_H
  #include <linux/magic.h>
#endif
#if MONGO_HAVE_HEADER_XFS_XFS_H
  #include <xfs/xfs.h>
#endif

#ifndef MONGO_CRASH_HAVE_STATFS_IMPL
  #define MONGO_CRASH_HAVE_STATFS_IMPL 0
#endif

#include <string.h>


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
            storage::do_backtrace();
            rawOut(" ");
        }

        static void gdbStacktrace() {
            if (!serverGlobalParams.gdbPath.empty()) {
                rawOut("--------------------------------------------------------------------------------");
                rawOut("GDB backtrace:");
                rawOut(" ");
                db_env_try_gdb_stack_trace(serverGlobalParams.gdbPath.c_str());
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

#if MONGO_HAVE_HEADER_SYS_RESOURCE_H
        static void printResourceLimit(int resource, const char *rname) {
            char buf[1<<12];
            char *p;
            int r;
            struct rlimit rlim;
            r = getrlimit(resource, &rlim);
            if (r != 0) {
                int eno = errno;
                p = buf;
                p = stpcpy(p, "Error getting ");
                p = stpcpy(p, rname);
                p = stpcpy(p, ": ");
                p = stpcpy(p, strerror(eno));
                rawOut(buf);
                return;
            }

            p = buf;
            p = stpcpy(p, rname);
            p = stpcpy(p, ": ");
            if (rlim.rlim_cur == RLIM_INFINITY) {
                p = stpcpy(p, "unlimited (soft)");
            } else {
                int n;
                snprintf(p, (sizeof buf) - (p - buf), "%zu (soft)%n", static_cast<size_t>(rlim.rlim_cur), &n);
                p += n;
            }
            if (rlim.rlim_max == RLIM_INFINITY) {
                p = stpcpy(p, ", unlimited (hard)");
            } else {
                int n;
                snprintf(p, (sizeof buf) - (p - buf), ", %zu (hard)%n", static_cast<size_t>(rlim.rlim_max), &n);
                p += n;
            }
            rawOut(buf);
        }
#endif

        static void printSysconf(int var, const char *name) {
            char buf[1<<12];
            long val = sysconf(var);
            if (val == -1) {
                int eno = errno;
                char *p = buf;
                p = stpcpy(p, "Error getting ");
                p = stpcpy(p, name);
                p = stpcpy(p, ": ");
                p = stpcpy(p, strerror(eno));
                rawOut(buf);
                return;
            }
            snprintf(buf, sizeof buf, "%s: %ld", name, val);
            rawOut(buf);
        }

        static void processInfo() {
            rawOut("--------------------------------------------------------------------------------");
            rawOut("Process info:");
            rawOut(" ");
            ProcessInfo pi;
            char buf[1<<12];
            snprintf(buf, sizeof buf, "OS:   %s %s %s %s",
                     pi.getOsType().c_str(), pi.getOsName().c_str(), pi.getOsVersion().c_str(), pi.getArch().c_str());
            rawOut(buf);
            snprintf(buf, sizeof buf, "NCPU: %d", pi.getNumCores());
            rawOut(buf);
            snprintf(buf, sizeof buf, "VIRT: %d MB", pi.getVirtualMemorySize());
            rawOut(buf);
            snprintf(buf, sizeof buf, "RES:  %d MB", pi.getVirtualMemorySize());
            rawOut(buf);
            snprintf(buf, sizeof buf, "PHYS: %llu MB", pi.getMemSizeMB());
            rawOut(buf);
#if MONGO_HAVE_HEADER_SYS_RESOURCE_H
            printResourceLimit(RLIMIT_CORE,   "RLIMIT_CORE");
            printResourceLimit(RLIMIT_CPU,    "RLIMIT_CPU");
            printResourceLimit(RLIMIT_DATA,   "RLIMIT_DATA");
            printResourceLimit(RLIMIT_FSIZE,  "RLIMIT_FSIZE");
            printResourceLimit(RLIMIT_NOFILE, "RLIMIT_NOFILE");
            printResourceLimit(RLIMIT_STACK,  "RLIMIT_STACK");
            printResourceLimit(RLIMIT_AS,     "RLIMIT_AS");
#endif
#if MONGO_HAVE_HEADER_UNISTD_H
  #ifdef _SC_OPEN_MAX
            printSysconf(_SC_OPEN_MAX,         "_SC_OPEN_MAX");
  #endif
  #ifdef _SC_PAGESIZE
            printSysconf(_SC_PAGESIZE,         "_SC_PAGESIZE");
  #endif
  #ifdef _SC_PHYS_PAGES
            printSysconf(_SC_PHYS_PAGES,       "_SC_PHYS_PAGES");
  #endif
  #ifdef _SC_AVPHYS_PAGES
            printSysconf(_SC_AVPHYS_PAGES,     "_SC_AVPHYS_PAGES");
  #endif
  #ifdef _SC_NPROCESSORS_CONF
            printSysconf(_SC_NPROCESSORS_CONF, "_SC_NPROCESSORS_CONF");
  #endif
  #ifdef _SC_NPROCESSORS_ONLN
            printSysconf(_SC_NPROCESSORS_ONLN, "_SC_NPROCESSORS_ONLN");
  #endif
#endif
            rawOut(" ");
        }

        static void parsedOpts() {
            rawOut("--------------------------------------------------------------------------------");
            rawOut("Parsed server options:");
            rawOut(" ");
            for (BSONObjIterator it(serverGlobalParams.parsedOpts); it.more(); ) {
                const BSONElement &elt = it.next();
                rawOut(elt.toString());
            }
            rawOut(" ");
        }

#if MONGO_CRASH_HAVE_STATFS_IMPL

        static const char *f_typeString(const struct statfs &st) {
#if defined(FSTYPENAMES)
            return fstypenames[st.f_type];
#elif defined(_DARWIN_FEATURE_64_BIT_INODE)
            return st.f_fstypename;
#else /* !defined(FSTYPENAMES) && !defined(_DARWIN_FEATURE_64_BIT_INODE) */

#ifdef AUTOFS_SUPER_MAGIC
            if (st.f_type == AUTOFS_SUPER_MAGIC) { return "autofs"; }
#endif
#ifdef BTRFS_SUPER_MAGIC
            if (st.f_type == BTRFS_SUPER_MAGIC) { return "btrfs"; }
#endif
#ifdef ECRYPTFS_SUPER_MAGIC
            if (st.f_type == ECRYPTFS_SUPER_MAGIC) { return "ecryptfs"; }
#endif
#ifdef EXT_SUPER_MAGIC
            if (st.f_type == EXT_SUPER_MAGIC) { return "ext"; }
#endif
#ifdef EXT2_OLD_SUPER_MAGIC
            if (st.f_type == EXT2_OLD_SUPER_MAGIC) { return "ext2 (old magic)"; }
#endif
#ifdef EXT2_SUPER_MAGIC
            if (st.f_type == EXT2_SUPER_MAGIC) {
                // For some reason, EXT2_SUPER_MAGIC, EXT3_SUPER_MAGIC, and EXT4_SUPER_MAGIC are all the same.
                // Probably need some ext-specific function to distinguish them...
                return "ext2/3/4";
            }
#endif
#ifdef EXT3_SUPER_MAGIC
            if (st.f_type == EXT3_SUPER_MAGIC) { return "ext2/3/4"; }
#endif
#ifdef EXT4_SUPER_MAGIC
            if (st.f_type == EXT4_SUPER_MAGIC) { return "ext2/3/4"; }
#endif
#ifdef HFS_SUPER_MAGIC
            if (st.f_type == HFS_SUPER_MAGIC) { return "hfs"; }
#endif
#ifdef JFS_SUPER_MAGIC
            if (st.f_type == JFS_SUPER_MAGIC) { return "jfs"; }
#endif
#ifdef NFS_SUPER_MAGIC
            if (st.f_type == NFS_SUPER_MAGIC) { return "nfs"; }
#endif
#ifdef NILFS_SUPER_MAGIC
            if (st.f_type == NILFS_SUPER_MAGIC) { return "nilfs"; }
#endif
#ifdef RAMFS_MAGIC
            if (st.f_type == RAMFS_MAGIC) { return "ramfs"; }
#endif
#ifdef REISERFS_SUPER_MAGIC
            if (st.f_type == REISERFS_SUPER_MAGIC) { return "reiserfs"; }
#endif
#ifdef SQUASHFS_MAGIC
            if (st.f_type == SQUASHFS_MAGIC) { return "squashfs"; }
#endif
#ifdef TMPFS_MAGIC
            if (st.f_type == TMPFS_MAGIC) { return "tmpfs"; }
#endif
#ifdef UFS_MAGIC
            if (st.f_type == UFS_MAGIC) { return "ufs"; }
#endif
#ifdef XFS_SUPER_MAGIC
            if (st.f_type == XFS_SUPER_MAGIC) { return "xfs"; }
#endif

#define ZFS_SUPER_MAGIC 0x2fc12fc1  // stolen from https://github.com/zfsonlinux/zfs/blob/8b4646494c23fc17c7cc5f7a857e27c463540098/include/sys/zfs_vfsops.h#L97
#ifdef ZFS_SUPER_MAGIC
            if (st.f_type == ZFS_SUPER_MAGIC) { return "zfs"; }
#endif

            return "unknown";
#endif /* !defined(FSTYPENAMES) && !defined(_DARWIN_FEATURE_64_BIT_INODE) */
        }

#endif /* MONGO_CRASH_HAVE_STATFS_IMPL */

        static void singleFsInfo(const char *path) {
#if MONGO_CRASH_HAVE_STATFS_IMPL
            char buf[1<<12];
            char *p;
            struct statfs st;
            int r = statfs(path, &st);
            if (r != 0) {
                int eno = errno;
                p = buf;
                p = stpcpy(p, "Error looking up filesystem information: ");
                p = stpcpy(p, strerror(eno));
                rawOut(buf);
                return;
            }
            snprintf(buf, sizeof buf, "type magic: 0x%08lX", static_cast<long unsigned>(st.f_type));
            rawOut(buf);
            p = buf;
            p = stpcpy(p, "type: ");
#if MONGO_HAVE_HEADER_XFS_XFS_H
            if (platform_test_xfs_path(path)) {
                p = stpcpy(p, "xfs");
            } else {
#endif
            p = stpcpy(p, f_typeString(st));
#if MONGO_HAVE_HEADER_XFS_XFS_H
            }
#endif
            rawOut(buf);
            snprintf(buf, sizeof buf, "bsize: %zu", static_cast<size_t>(st.f_bsize));
            rawOut(buf);
            snprintf(buf, sizeof buf, "blocks: %zu", static_cast<size_t>(st.f_blocks));
            rawOut(buf);
            snprintf(buf, sizeof buf, "bfree: %zu", static_cast<size_t>(st.f_bfree));
            rawOut(buf);
            snprintf(buf, sizeof buf, "bavail: %zu", static_cast<size_t>(st.f_bavail));
            rawOut(buf);

#else /* !MONGO_CRASH_HAVE_STATFS_IMPL */
            rawOut("statfs(2) unavailable");
#endif
        }

        static void fsInfo() {
            rawOut("--------------------------------------------------------------------------------");
            rawOut("Filesystem information:");
            rawOut(" ");

            char buf[1<<12];
            char *p;

            char dbpath_cstr[1<<12];
            if (serverGlobalParams.doFork || storageGlobalParams.dbpath[0] == '/') {
                strncpy(dbpath_cstr, storageGlobalParams.dbpath.c_str(), sizeof dbpath_cstr);
            } else {
                // mallocs, hopefully this is ok
                std::stringstream ss;
                ss << serverGlobalParams.cwd << "/" << storageGlobalParams.dbpath;
                std::string absdbpath = ss.str();
                strncpy(dbpath_cstr, absdbpath.c_str(), sizeof dbpath_cstr);
            }

            p = buf;
            p = stpcpy(p, "Information for dbpath \"");
            p = stpncpy(p, dbpath_cstr, (sizeof buf) - (p - buf));
            p = stpncpy(p, "\":", (sizeof buf) - (p - buf));
            rawOut(buf);
            rawOut(" ");
            singleFsInfo(dbpath_cstr);
            rawOut(" ");

            if (!storageGlobalParams.logDir.empty() && storageGlobalParams.logDir != storageGlobalParams.dbpath) {
                p = buf;
                p = stpcpy(p, "Information for logDir \"");
                p = stpncpy(p, storageGlobalParams.logDir.c_str(), (sizeof buf) - (p - buf));
                p = stpncpy(p, "\":", (sizeof buf) - (p - buf));
                rawOut(buf);
                rawOut(" ");
                singleFsInfo(storageGlobalParams.logDir.c_str());
                rawOut(" ");
            }

            if (!storageGlobalParams.tmpDir.empty() && storageGlobalParams.tmpDir != storageGlobalParams.dbpath) {
                p = buf;
                p = stpcpy(p, "Information for tmpDir \"");
                p = stpncpy(p, storageGlobalParams.tmpDir.c_str(), (sizeof buf) - (p - buf));
                p = stpncpy(p, "\":", (sizeof buf) - (p - buf));
                rawOut(buf);
                rawOut(" ");
                singleFsInfo(storageGlobalParams.tmpDir.c_str());
                rawOut(" ");
            }
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
            processInfo();
            parsedOpts();
            fsInfo();
            curOpInfo();
            opDebugInfo();
        }

    } // namespace crashdump

    void dumpCrashInfo(const StringData &reason) try {
        crashdump::basicInfo();
        char buf[1<<12];
        strncpy(buf, reason.rawData(), reason.size());
        buf[reason.size()] = '\0';
        crashdump::reason(buf);
        crashdump::extraInfo();
    } catch (DBException &e) {
        // can't rethrow here, might be crashing
        try {
            rawOut(" ");
            rawOut("Unhandled DBException while dumping crash info:");
            rawOut(e.what());
            char buf[1<<12];
            snprintf(buf, 1<<12, "code: %d", e.getCode());
            rawOut(buf);
        } catch (...) {
            // uh-oh
        }
    } catch (std::exception &e) {
        try {
            rawOut(" ");
            rawOut("Unhandled exception while dumping crash info:");
            rawOut(e.what());
        } catch (...) {
            // uh-oh
        }
    } catch (...) {
        try {
            rawOut(" ");
            rawOut("Unhandled unknown exception while dumping crash info.");
        } catch (...) {
            // whoa, nelly
        }
    }

    void dumpCrashInfo(const DBException &e) try {
        crashdump::basicInfo();
        char buf[1<<12];
        snprintf(buf, 1<<12, "DBException code: %d what: %s", e.getCode(), e.what());
        crashdump::reason(buf);
        crashdump::extraInfo();
    } catch (DBException &newEx) {
        // can't rethrow here, might be crashing
        try {
            rawOut(" ");
            rawOut("Unhandled DBException while dumping crash info:");
            rawOut(newEx.what());
            char buf[1<<12];
            snprintf(buf, 1<<12, "code: %d", newEx.getCode());
            rawOut(buf);
        } catch (...) {
            // uh-oh
        }
    } catch (std::exception &newEx) {
        try {
            rawOut(" ");
            rawOut("Unhandled exception while dumping crash info:");
            rawOut(newEx.what());
        } catch (...) {
            // uh-oh
        }
    } catch (...) {
        try {
            rawOut(" ");
            rawOut("Unhandled unknown exception while dumping crash info.");
        } catch (...) {
            // whoa, nelly
        }
    }

    /* for testing */
    class CmdDumpCrashInfo : public InformationCommand {
      public:
        CmdDumpCrashInfo() : InformationCommand("_dumpCrashInfo") { }
        virtual bool adminOnly() const { return true; }
        virtual void help( stringstream& help ) const {
            help << "internal testing-only command: makes the server dump some debugging info to the log";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {}
        bool run(const string& ns, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            LOG(0) << "Dumping crash info for _dumpCrashInfo command." << endl;
            dumpCrashInfo("Dumping crash info for _dumpCrashInfo command.");
            return true;
        }
    };
    MONGO_INITIALIZER(RegisterDumpCrashInfoCmd)(InitializerContext* context) {
        if (Command::testCommandsEnabled) {
            // Leaked intentionally: a Command registers itself when constructed.
            new CmdDumpCrashInfo();
        }
        return Status::OK();
    }

} // namespace mongo
