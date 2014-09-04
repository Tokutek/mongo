/*
 *    Copyright (C) 2013 10gen Inc.
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

#pragma once

#include <stdint.h>
#include <string>

/*
 * This file defines the storage for options that come from the command line related to data file
 * persistence.  Many executables that can access data files directly such as mongod and certain
 * tools use these variables, but each executable may have a different set of command line flags
 * that allow the user to change a different subset of these options.
 */

namespace mongo {

    struct StorageGlobalParams {

        StorageGlobalParams() :
#ifdef _WIN32
            dbpath("\\data\\db\\"),
#else
            dbpath("/data/db/"),
#endif
            quota(false), quotaFiles(8),
            syncdelay(60),
            useHints(true),

            // TokuMX variables
            cacheSize(0),
            loaderMaxMemory(100ULL << 20),
            lockTimeout(4000),
            locktreeMaxMemory(0),
            txnMemLimit(1ULL << 20),
            checkpointPeriod(60),
            cleanerPeriod(2),
            cleanerIterations(5),
            fsRedzone(5),
            logFlushPeriod(100),
            directio(false),
            fastupdates(false),
            fastupdatesIgnoreErrors(false),
            loaderCompressTmp(true),
            logDir(""),
            tmpDir("")
        {}

        std::string dbpath;

        bool noTableScan;      // --notablescan no table scans allowed

        bool quota;            // --quota
        int quotaFiles;        // --quotaFiles

        double syncdelay;      // seconds between fsyncs

        bool useHints;         // only off if --nohints

        // TokuMX variables
        uint64_t cacheSize;
        uint64_t loaderMaxMemory;
        uint64_t lockTimeout;
        uint64_t locktreeMaxMemory;
        uint64_t txnMemLimit;
        int checkpointPeriod;
        int cleanerPeriod;
        int cleanerIterations;
        int fsRedzone;
        int logFlushPeriod;
        bool directio;
        bool fastupdates;
        bool fastupdatesIgnoreErrors;
        bool loaderCompressTmp;
        std::string logDir;
        std::string tmpDir;
    };

    extern StorageGlobalParams storageGlobalParams;

    // This is not really related to persistence, but mongos and the other executables share code
    // and we use this function to determine at runtime which executable we are in.
    bool isMongos();

} // namespace mongo
