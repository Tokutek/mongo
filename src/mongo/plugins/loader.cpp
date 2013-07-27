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

#include "mongo/base/string_data.h"
#include "mongo/db/jsobj.h"
#include "mongo/plugins/dl.h"
#include "mongo/plugins/plugins.h"
#include "mongo/util/log.h"

namespace mongo {

    namespace plugins {

        bool PluginHandle::init(string &errmsg, BSONObjBuilder &result) {
            bool ok = _dl.open(_filename.c_str(), RTLD_NOW);
            if (!ok) {
                errmsg += "error loading plugin from file " + _filename + ": " + _dl.error();
                return false;
            }
            void *getInterfacev = _dl.sym("getInterface");
            if (getInterfacev == NULL) {
                errmsg += "error finding symbol `getInterface' in file " + _filename + ": " + _dl.error();
                return false;
            }

            {
                // TODO: portability, dlinfo should work on freebsd
                Dl_info info;
                int r = dladdr(getInterfacev, &info);
                // dladdr() returns 0 on failure, non-zero on success.  Really.
                if (r == 0) {
                    errmsg += "unknown error getting info about plugin using dladdr()";
                    return false;
                }

                shared_ptr<char> buf(realpath(info.dli_fname, NULL), free);
                _fullpath = buf.get();

                LOG(2) << "Found plugin \"" << _filename << "\" actually at \"" << _fullpath << "\"" << endl;

                struct stat st;
                r = stat(_fullpath.c_str(), &st);
                if (r != 0) {
                    stringstream ss;
                    ss << "couldn't stat [" << _fullpath << "] (error " << errnoWithDescription() << "), not loading plugin";
                    errmsg += ss.str();
                    return false;
                }
                if (st.st_mode & S_IWOTH) {
                    stringstream ss;
                    ss << "plugin \"" << _filename << "\" at \"" << _fullpath << "\" is world writable, not loading";
                    errmsg += ss.str();
                    return false;
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

        bool Loader::load(const StringData &filename, string &errmsg, BSONObjBuilder &result) {
            shared_ptr<PluginHandle> pluginHandle(new PluginHandle(filename.toString()));
            bool ok = pluginHandle->init(errmsg, result);
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

        Loader loader;

    }  // namespace plugins

}  // namespace mongo
