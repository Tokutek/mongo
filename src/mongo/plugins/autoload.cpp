// @file autoload.cpp

/**
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

#include "mongo/plugins/autoload.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/filesystem.hpp>

#include "mongo/plugins/loader.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/processinfo.h"

namespace fs = boost::filesystem;

namespace mongo {

    namespace plugins {

        string defaultDir(void) {
            ProcessInfo p;
            fs::path exePath(p.getExePath());
            fs::path pluginsDir = exePath.parent_path().parent_path() / "lib64" / "plugins";
            return pluginsDir.string();
        }

        void autoload(const string &pluginsDir) {
            LOG(1) << "Trying to auto-load plugins from " << pluginsDir << endl;
            fs::path pluginsPath(pluginsDir);
            if (!fs::exists(pluginsPath)) {
                LOG(1) << "Plugin directory \"" << pluginsDir << "\" doesn't exist, not auto-loading any plugins." << endl;
                return;
            }

            {
                // TODO: portability
                struct stat st;
                int r = stat(pluginsDir.c_str(), &st);
                if (r != 0) {
                    LOG(1) << "Couldn't stat plugins directory \"" << pluginsDir << "\" (error " << errnoWithDescription() << "), not auto-loading any plugins." << endl;
                    return;
                }
                if (st.st_mode & S_IWOTH) {
                    LOG(1) << "Plugins directory \"" << pluginsDir << " is world writable, won't load plugins from it." << endl;
                    return;
                }
            }

            vector<BSONObj> loaded;
            fs::directory_iterator end;
            for (fs::directory_iterator it(pluginsPath); it != end; ++it) {
                const fs::path &ent = it->path();
                if (fs::exists(ent)) {
                    StringData filename(ent.filename());
                    if (filename.startsWith("lib") && filename.endsWith(".so")) {
                        LOG(1) << "Trying to auto-load plugin from file \"" << filename << "\"" << endl;
                        string errmsg;
                        BSONObjBuilder b;
                        bool ok = loader.load(ent.string(), errmsg, b);
                        BSONObj res = b.done();
                        if (ok) {
                            loaded.push_back(res["loaded"].Obj().getOwned());
                        }
                        else {
                            LOG(0) << errmsg << endl;
                        }
                    }
                }
            }
            LOG(0) << "Auto-loaded plugins:" << endl;
            for (vector<BSONObj>::const_iterator it = loaded.begin(); it != loaded.end(); ++it) {
                LOG(0) << "\t" << *it << endl;
            }
        }

    } // namespace plugins

} // namespace mongo
