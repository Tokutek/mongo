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
            GetInterfaceFunc getInterface = reinterpret_cast<GetInterfaceFunc>(_dl.sym("getInterface"));
            if (getInterface == NULL) {
                errmsg += "error finding symbol `getInterface' in file " + _filename + ": " + _dl.error();
                return false;
            }
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

        const string &PluginHandle::version() const {
            if (!_interface) {
                DEV LOG(0) << "in PluginHandle::version, plugin is not initialized" << endl;
                static const string empty("");
                return empty;
            }
            return _interface->version();
        }

        bool PluginHandle::load(string &errmsg, BSONObjBuilder &result) {
            if (!_interface) {
                errmsg = "plugin not initialized";
                return false;
            }
            bool ok = _interface->load(errmsg, result);
            if (ok) {
                LOG(0) << "Loaded plugin " << name() << " from " << _filename << endl;
            }
            return ok;
        }

        bool PluginHandle::info(string &errmsg, BSONObjBuilder &result) const {
            if (!_interface) {
                errmsg = "plugin not initialized";
                return false;
            }
            return _interface->info(errmsg, result);
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
            lb.append("name", name);
            lb.append("version", pluginHandle->version());
            lb.doneFast();
            return true;
        }

        bool Loader::list(string &errmsg, BSONObjBuilder &result) const {
            BSONArrayBuilder ab(result.subarrayStart("plugins"));
            for (PluginMap::const_iterator it = _plugins.begin(); it != _plugins.end(); ++it) {
                const shared_ptr<PluginHandle> &plugin = it->second;
                BSONObjBuilder b(ab.subobjStart());
                b.append("name", plugin->name());
                b.append("version", plugin->version());
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
