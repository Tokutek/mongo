// @file loader.cpp

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

#include "mongo/plugins/loader.h"

#include <dlfcn.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <fstream>
#include <iomanip>

#include <boost/filesystem.hpp>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/db/jsobj.h"
#include "mongo/plugins/dl.h"
#include "mongo/plugins/plugins.h"
#include "mongo/util/log.h"
#include "mongo/util/md5.hpp"
#include "mongo/util/processinfo.h"

namespace fs = boost::filesystem;

namespace mongo {

    namespace plugins {

        static bool checkPermissions(fs::path abspath, int components, int invalidPerms, string &errmsg) {
            for (fs::path::iterator it = abspath.begin(); it != abspath.end(); ++it) {
                if (*it == ".." || *it == ".") {
                    stringstream ss;
                    ss << "invalid path component \"" << *it << "\" in plugin path";
                    errmsg = ss.str();
                    return false;
                }
            }

            // We want to check the paths from top to bottom.
            vector<fs::path> paths;
            for (int i = 0; i < components && !abspath.empty(); ++i) {
                paths.push_back(abspath);
                abspath = abspath.parent_path();
            }
            for (vector<fs::path>::const_reverse_iterator it = paths.rbegin(); it != paths.rend(); ++it) {
                struct stat st;
                const string &curPath = it->string();
                int r = stat(curPath.c_str(), &st);
                if (r != 0) {
                    stringstream ss;
                    ss << "couldn't stat \"" << curPath << "\" ( error " << errnoWithDescription() << ")" << endl;
                    errmsg = ss.str();
                    return false;
                }
                if ((st.st_mode & invalidPerms) != 0) {
                    stringstream ss;
                    ss << "invalid permissions " << std::oct << std::setw(4) << std::setfill('0') << invalidPerms << " on path \"" << curPath << "\"";
                    errmsg = ss.str();
                    return false;
                }
            }
            return true;
        }

        bool PluginHandle::init(const StringData &expectedHash, string &errmsg, BSONObjBuilder &result) {
            bool ok = _dl.open(_filename.c_str(), RTLD_NOW);
            if (!ok) {
                errmsg += "error loading plugin from file " + _filename + ": " + _dl.error();
                return false;
            }
            void *getInterfacev = _dl.sym("TokuMX_Plugin__getInterface");
            if (getInterfacev == NULL) {
                errmsg += "error finding symbol `TokuMX_Plugin__getInterface' in file " + _filename + ": " + _dl.error();
                return false;
            }

            // Check permissions
            {
                // TODO: portability, dlinfo should work on freebsd
                Dl_info info;
                int r = dladdr(getInterfacev, &info);
                // dladdr() returns 0 on failure, non-zero on success.  Really.
                if (r == 0) {
                    errmsg += "unknown error getting info about plugin using dladdr()";
                    return false;
                }

                // Need to check before resolving symlinks and after resolving them.
                ok = checkPermissions(info.dli_fname, 2, S_IWOTH, errmsg);
                if (!ok) {
                    return false;
                }
                shared_ptr<char> buf(realpath(info.dli_fname, NULL), free);
                ok = checkPermissions(buf.get(), 2, S_IWOTH, errmsg);
                if (!ok) {
                    return false;
                }
                _fullpath = buf.get();

                LOG(2) << "Found plugin \"" << _filename << "\" actually at \"" << _fullpath << "\"" << endl;
            }

            // Verify checksum
            {
                class Closer : boost::noncopyable {
                    std::ifstream &_f;
                  public:
                    Closer(std::ifstream &f) : _f(f) {}
                    ~Closer() { _f.close(); }
                };

                boost::scoped_ptr<Hasher> hasher(HasherFactory::createHasher(0));
                size_t bufsize = 1<<12;
                scoped_array<char> hashBuf(new char[bufsize]);
                std::ifstream fs(_fullpath.c_str(), std::ifstream::binary);
                if (!fs.is_open()) {
                    stringstream ss;
                    ss << "couldn't open file \"" << _fullpath << "\": " << errnoWithDescription() << endl;
                    errmsg = ss.str();
                    return false;
                }
                Closer closer(fs);

                while (fs.good()) {
                    fs.read(hashBuf.get(), bufsize);
                    hasher->addData(static_cast<const void *>(hashBuf.get()), fs.gcount());
                }
                hasher->finish(_hash);

                if (!expectedHash.empty()) {
                    if (expectedHash != hashString()) {
                        stringstream ss;
                        ss << "MD5 mismatch for plugin \"" << _filename << "\": expected " << expectedHash << ", found " << hashString();
                        errmsg = ss.str();
                        return false;
                    }
                }
            }

            GetInterfaceFunc getInterface = reinterpret_cast<GetInterfaceFunc>(getInterfacev);
            PluginInterface *interfacep = getInterface();
            if (interfacep == NULL) {
                errmsg += "couldn't load plugin from " + _filename;
                return false;
            }
            _interface = interfacep;
            return true;
        }

        const string &PluginHandle::name() const {
            if (!_interface) {
                DEV LOG(0) << "in PluginHandle::name, plugin is not initialized" << endl;
                static const string empty("");
                return empty;
            }
            return _interface->name();
        }

        string PluginHandle::hashString() const {
            return digestToString(_hash);
        }

        bool PluginHandle::load(string &errmsg, BSONObjBuilder &result) {
            if (!_interface) {
                errmsg = "plugin not initialized";
                return false;
            }
            bool ok = _interface->load(errmsg, result);
            if (ok) {
                LOG(0) << "Loaded plugin " << name() << " from " << _filename << endl;
                _loaded = true;
            }
            return ok;
        }

        bool PluginHandle::info(string &errmsg, BSONObjBuilder &result) const {
            if (!_interface) {
                errmsg = "plugin not initialized";
                return false;
            }
            result.append("filename", _filename);
            result.append("fullpath", _fullpath);
            result.append("name", _interface->name());
            result.append("version", _interface->version());
            result.append("checksum", hashString());
            return _interface->info(errmsg, result);
        }

        PluginHandle::~PluginHandle() {
            if (_loaded) {
                string errmsg;
                if (!unload(errmsg)) {
                    LOG(0) << "Error unloading plugin in destructor: " << errmsg << endl;
                }
            }
        }

        bool PluginHandle::unload(string &errmsg) {
            if (!_interface) {
                errmsg = "plugin not initialized";
                return false;
            }
            string nameCopy = name();
            _interface->unload(errmsg);
            _interface = NULL;
            _dl.close();
            LOG(0) << "Unloaded plugin " << nameCopy << " from " << _filename << endl;
            _loaded = false;
            return true;
        }

        void Loader::autoload(const vector<string> &plugins) {
            for (vector<string>::const_iterator it = plugins.begin(); it != plugins.end(); ++it) {
                StringData pluginStr = *it;
                size_t colonPos = pluginStr.find(':');
                if (colonPos == string::npos) {
                    LOG(0) << "Invalid plugin specification \"" << pluginStr << "\", should be \"<plugin name>:<md5 checksum>\"." << endl;
                    continue;
                }
                StringData plugin = pluginStr.substr(0, colonPos);
                StringData hash = pluginStr.substr(colonPos + 1);
                if (plugin.find('/') != string::npos) {
                    LOG(0) << "Not loading plugin \"" << plugin << "\" because it contains a '/'." << endl;
                    continue;
                }

                stringstream ss;
                ss << "lib" << plugin.toString() << ".so";
                BSONObjBuilder b;
                string errmsg;
                bool ok = load(ss.str(), hash, errmsg, b);
                if (ok) {
                    LOG(1) << "\t" << b.done() << endl;
                }
                else {
                    LOG(0) << "Error loading plugin \"" << plugin << "\": " << errmsg << endl;
                }
            }
        }

        bool Loader::load(const string &filename, StringData expectedHash, string &errmsg, BSONObjBuilder &result) {
            if (expectedHash == "skipChecksumValidation") {
                expectedHash = StringData();
            }
            else if (expectedHash.spn("1234567890abcdef") != expectedHash.size()) {
                stringstream ss;
                ss << "Invalid expected checksum: \"" << expectedHash << "\", not loading plugin \"" << filename << "\"." << endl;
                errmsg = ss.str();
                return false;
            }

            fs::path filepath = _pluginsDir / filename;
            // If the user set pluginsDir, we should basically trust it.  If we're going from
            // "basedir", we should verify all the way up to it.
            bool ok = checkPermissions(filepath, _pluginsDir == defaultPluginsDir() ? 4 : 2, S_IWOTH, errmsg);
            if (!ok) {
                return false;
            }
            shared_ptr<PluginHandle> pluginHandle(new PluginHandle(filepath.string()));
            ok = pluginHandle->init(expectedHash, errmsg, result);
            if (!ok) {
                return false;
            }
            const string &name = pluginHandle->name();
            if (_plugins[name]) {
                errmsg = "cannot load plugin " + name + " twice";
                return false;
            }
            ok = pluginHandle->load(errmsg, result);
            if (!ok) {
                return false;
            }
            _plugins[name] = pluginHandle;
            BSONObjBuilder lb(result.subobjStart("loaded"));
            ok = pluginHandle->info(errmsg, lb);
            lb.doneFast();
            return ok;
        }

        bool Loader::list(string &errmsg, BSONObjBuilder &result) const {
            BSONArrayBuilder ab(result.subarrayStart("plugins"));
            for (PluginMap::const_iterator it = _plugins.begin(); it != _plugins.end(); ++it) {
                const shared_ptr<PluginHandle> &plugin = it->second;
                BSONObjBuilder b(ab.subobjStart());
                bool ok = plugin->info(errmsg, b);
                b.doneFast();
                if (!ok) {
                    break;
                }
            }
            ab.doneFast();
            return true;
        }

        bool Loader::unload(const StringData &name, string &errmsg, BSONObjBuilder &result) {
            PluginMap::iterator it = _plugins.find(name.toString());
            if (it == _plugins.end()) {
                errmsg = "plugin '" + name.toString() + "' not found";
                return false;
            }
            shared_ptr<PluginHandle> pluginHandle = it->second;
            _plugins.erase(it);
            if (pluginHandle) {
                pluginHandle->unload(errmsg);
            }
            return true;
        }

        void Loader::shutdown() {
            while (!_plugins.empty()) {
                PluginMap::iterator it = _plugins.begin();
                shared_ptr<PluginHandle> plugin = it->second;
                string errmsg;
                if (plugin) {
                    bool ok = plugin->unload(errmsg);
                    if (!ok) {
                        LOG(0) << "During shutdown, error unloading plugin " << it->first << ": " << errmsg << endl;
                    }
                }
                _plugins.erase(it);
            }
        }

        void Loader::setPluginsDir(const string &path) {
            string errmsg;
            bool ok = checkPermissions(path, 1, S_IWOTH, errmsg);
            massert(16901, errmsg, ok);
            _pluginsDir = path;
        }

        fs::path Loader::defaultPluginsDir() {
            ProcessInfo p;
            fs::path exePath = p.getExePath();
            return exePath.parent_path().parent_path() / "lib64" / "plugins";
        }

        Loader *loader;

        namespace {
            MONGO_INITIALIZER(CreatePluginLoader)(InitializerContext* context) {
                // intentionally leaked
                loader = new Loader;
                return Status::OK();
            }
        } // namespace

    }  // namespace plugins

}  // namespace mongo
