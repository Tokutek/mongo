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

#include <iostream>
#include <iomanip>

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
            severe() << std::endl
                     << "================================================================================" << std::endl
                     << " Fatal error detected" << std::endl
                     << "================================================================================" << std::endl
                     << std::endl
                     << "About to gather debugging information, please include all of the following along" << std::endl
                     << "with logs from other servers in the cluster in a bug report." << std::endl
                     << std::endl;
        }

        static void versionInfo() {
            severe() << "--------------------------------------------------------------------------------" << std::endl
                     << "Version info:" << std::endl
                     << std::endl
                     << "tokumxVersion: " << tokumxVersionString << std::endl
                     << "gitVersion: " << gitVersion() << std::endl
                     << "tokukvVersion: " << tokukvVersion() << std::endl
                     << "sysInfo: " << sysInfo() << std::endl
                     << "loaderFlags: " << loaderFlags() << std::endl
                     << "compilerFlags: " << compilerFlags() << std::endl
                     << "debug: " << (debug ? "true" : "false") << std::endl
                     << std::endl;
        }

        static void simpleStacktrace() {
            severe() << "--------------------------------------------------------------------------------" << std::endl
                     << "Simple stacktrace:" << std::endl
                     << std::endl;
            printStackTrace();
            severe() << std::endl;
        }

        static void tokukvBacktrace() {
            severe() << "--------------------------------------------------------------------------------" << std::endl
                     << "TokuKV engine backtrace:" << std::endl
                     << std::endl;
            storage::do_backtrace();
            severe() << std::endl;
        }

        static void gdbStacktrace() {
            if (!serverGlobalParams.gdbPath.empty()) {
                severe() << "--------------------------------------------------------------------------------" << std::endl
                         << "GDB backtrace:" << std::endl
                         << std::endl;
                db_env_try_gdb_stack_trace(serverGlobalParams.gdbPath.c_str());
                severe() << std::endl;
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
        }

#if MONGO_HAVE_HEADER_SYS_RESOURCE_H
        static void printResourceLimit(LogstreamBuilder &s, int resource, const char *rname) {
            struct rlimit rlim;
            int r = getrlimit(resource, &rlim);
            if (r != 0) {
                int eno = errno;
                s << "Error getting " << rname << ": " << strerror(eno) << std::endl;
                return;
            }

            s << rname << ": ";
            if (rlim.rlim_cur == RLIM_INFINITY) {
                s << "unlimited";
            } else {
                s << static_cast<size_t>(rlim.rlim_cur);
            }
            s << " (soft), ";
            if (rlim.rlim_max == RLIM_INFINITY) {
                s << "unlimited";
            } else {
                s << static_cast<size_t>(rlim.rlim_max);
            }
            s << " (hard)" << std::endl;
        }
#endif

        static void printSysconf(LogstreamBuilder &s, int var, const char *name) {
            long val = sysconf(var);
            if (val == -1) {
                int eno = errno;
                s << "Error getting " << name << ": " << strerror(eno) << std::endl;
                return;
            }
            s << name << ": " << val << std::endl;
        }

        static void processInfo() {
            severe() << "--------------------------------------------------------------------------------" << std::endl
                     << "Process info:" << std::endl
                     << std::endl;
            ProcessInfo pi;
            LogstreamBuilder s = severe();
            s << "OS:   " << pi.getOsType() << pi.getOsName() << pi.getOsVersion() << pi.getArch() << std::endl
              << "NCPU: " << pi.getNumCores() << std::endl
              << "VIRT: " << pi.getVirtualMemorySize() << " MB" << std::endl
              << "RES:  " << pi.getResidentSize() << " MB" << std::endl
              << "PHYS: " << pi.getMemSizeMB() << " MB" << std::endl;
#if MONGO_HAVE_HEADER_SYS_RESOURCE_H
            printResourceLimit(s, RLIMIT_CORE,   "RLIMIT_CORE");
            printResourceLimit(s, RLIMIT_CPU,    "RLIMIT_CPU");
            printResourceLimit(s, RLIMIT_DATA,   "RLIMIT_DATA");
            printResourceLimit(s, RLIMIT_FSIZE,  "RLIMIT_FSIZE");
            printResourceLimit(s, RLIMIT_NOFILE, "RLIMIT_NOFILE");
            printResourceLimit(s, RLIMIT_STACK,  "RLIMIT_STACK");
            printResourceLimit(s, RLIMIT_AS,     "RLIMIT_AS");
#endif
#if MONGO_HAVE_HEADER_UNISTD_H
  #ifdef _SC_OPEN_MAX
            printSysconf(s, _SC_OPEN_MAX,         "_SC_OPEN_MAX");
  #endif
  #ifdef _SC_PAGESIZE
            printSysconf(s, _SC_PAGESIZE,         "_SC_PAGESIZE");
  #endif
  #ifdef _SC_PHYS_PAGES
            printSysconf(s, _SC_PHYS_PAGES,       "_SC_PHYS_PAGES");
  #endif
  #ifdef _SC_AVPHYS_PAGES
            printSysconf(s, _SC_AVPHYS_PAGES,     "_SC_AVPHYS_PAGES");
  #endif
  #ifdef _SC_NPROCESSORS_CONF
            printSysconf(s, _SC_NPROCESSORS_CONF, "_SC_NPROCESSORS_CONF");
  #endif
  #ifdef _SC_NPROCESSORS_ONLN
            printSysconf(s, _SC_NPROCESSORS_ONLN, "_SC_NPROCESSORS_ONLN");
  #endif
#endif
            s << std::endl;
        }

        static void parsedOpts() {
            severe() << "--------------------------------------------------------------------------------" << std::endl
                     << "Parsed server options:" << std::endl
                     << " " << std::endl;
            LogstreamBuilder s = severe();
            for (BSONObjIterator it(serverGlobalParams.parsedOpts); it.more(); ) {
                const BSONElement &elt = it.next();
                s << elt.toString() << std::endl;
            }
            s << std::endl;
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

    static void singleFsInfo(LogstreamBuilder &s, const char *path) {
#if MONGO_CRASH_HAVE_STATFS_IMPL
            struct statfs st;
            int r = statfs(path, &st);
            if (r != 0) {
                int eno = errno;
                s << "Error looking up filesystem information: " << strerror(eno) << std::endl;
                return;
            }
            s.stream() << "type magic: 0x" << std::hex << std::setw(8) << std::setfill('0')
                       << static_cast<long unsigned>(st.f_type) << std::endl
                       << "type: ";
#if MONGO_HAVE_HEADER_XFS_XFS_H
            if (platform_test_xfs_path(path)) {
                s << "xfs";
            } else {
#endif
            s << f_typeString(st);
#if MONGO_HAVE_HEADER_XFS_XFS_H
            }
#endif
            s << std::endl
              << "bsize: " << st.f_bsize << std::endl
              << "blocks: " << st.f_blocks << std::endl
              << "bfree: " << st.f_bfree << std::endl
              << "bavail: " << st.f_bavail << std::endl;

#else /* !MONGO_CRASH_HAVE_STATFS_IMPL */
            severe() << "statfs(2) unavailable" << std::endl;
#endif
        }

        static void fsInfo() {
            severe() << "--------------------------------------------------------------------------------" << std::endl
                     << "Filesystem information:" << std::endl
                     << " " << std::endl;

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

            LogstreamBuilder s = severe();
            s << "Information for dbpath \"" << dbpath_cstr << "\":" << std::endl
              << std::endl;
            singleFsInfo(s, dbpath_cstr);
            s << std::endl;

            if (!storageGlobalParams.logDir.empty() && storageGlobalParams.logDir != storageGlobalParams.dbpath) {
                s << "Information for logDir \"" << storageGlobalParams.logDir << "\":" << std::endl
                  << std::endl;
                singleFsInfo(s, storageGlobalParams.logDir.c_str());
                s << std::endl;
            }

            if (!storageGlobalParams.tmpDir.empty() && storageGlobalParams.tmpDir != storageGlobalParams.dbpath) {
                s << "Information for tmpDir \"" << storageGlobalParams.tmpDir << "\":" << std::endl
                  << std::endl;
                singleFsInfo(s, storageGlobalParams.tmpDir.c_str());
                s << std::endl;
            }
        }

        static void curOpInfo() {
            severe() << "--------------------------------------------------------------------------------" << std::endl
                     << "Current operations in progress:" << std::endl
                     << " " << std::endl;
            for (set<Client *>::const_iterator i = Client::clients.begin(); i != Client::clients.end(); i++) {
                Client *c = *i;
                if (c == NULL) {
                    severe() << "NULL pointer in clients map" << std::endl;
                    continue;
                }
                CurOp *co = c->curop();
                if (co == NULL) {
                    severe() << "NULL pointer in curop" << std::endl;
                    continue;
                }
                severe() << co->info().toString() << std::endl;
            }
            severe() << std::endl;
        }

        static void opDebugInfo() {
            severe() << "--------------------------------------------------------------------------------" << std::endl
                     << "OpDebug info:" << std::endl
                     << " " << std::endl;
            for (set<Client *>::const_iterator i = Client::clients.begin(); i != Client::clients.end(); i++) {
                Client *c = *i;
                if (c == NULL) {
                    severe() << "NULL pointer in clients map" << std::endl;
                    continue;
                }
                CurOp *co = c->curop();
                if (co == NULL) {
                    severe() << "NULL pointer in curop" << std::endl;
                    continue;
                }
                severe() << co->debug().report(*co) << std::endl;
            }
            severe() << std::endl;
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
        severe() << "--------------------------------------------------------------------------------" << std::endl
                 << "Crash reason:" << std::endl
                 << std::endl
                 << reason << std::endl
                 << std::endl;
        crashdump::extraInfo();
    } catch (DBException &e) {
        // can't rethrow here, might be crashing
        try {
            severe() << std::endl
                     << "Unhandled DBException while dumping crash info: " << e.what() << std::endl
                     << " code: " << e.getCode() << std::endl;
        } catch (...) {
            // uh-oh
        }
    } catch (std::exception &e) {
        try {
            severe() << std::endl
                     << "Unhandled exception while dumping crash info: " << e.what() << std::endl;
        } catch (...) {
            // uh-oh
        }
    } catch (...) {
        try {
            severe() << std::endl
                     << "Unhandled unknown exception while dumping crash info." << std::endl;
        } catch (...) {
            // whoa, nelly
        }
    }

    void dumpCrashInfo(const DBException &e) try {
        crashdump::basicInfo();
        severe() << "--------------------------------------------------------------------------------" << std::endl
                 << "Crash reason:" << std::endl
                 << std::endl
                 << "DBException code: " << e.getCode() << " what: " << e.what() << std::endl
                 << std::endl;
        crashdump::extraInfo();
    } catch (DBException &newEx) {
        // can't rethrow here, might be crashing
        try {
            severe() << std::endl
                     << "Unhandled DBException while dumping crash info: " << newEx.what() << std::endl
                     << " code: " << newEx.getCode() << std::endl;
        } catch (...) {
            // uh-oh
        }
    } catch (std::exception &newEx) {
        try {
            severe() << std::endl
                     << "Unhandled exception while dumping crash info: " << newEx.what() << std::endl;
        } catch (...) {
            // uh-oh
        }
    } catch (...) {
        try {
            severe() << std::endl
                     << "Unhandled unknown exception while dumping crash info." << std::endl;
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
