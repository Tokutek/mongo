// @file loader.h

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

#pragma once

#include "mongo/pch.h"

#include <map>
#include <boost/filesystem.hpp>

#include "mongo/db/hasher.h"
#include "mongo/plugins/dl.h"
#include "mongo/plugins/plugins.h"

namespace mongo {

    namespace plugins {

        class PluginHandle : boost::noncopyable {
            const string _filename;
            string _fullpath;
            DLHandle _dl;
            PluginInterface *_interface;
            bool _loaded;
            HashDigest _hash;

          public:
            explicit PluginHandle(const string &filename)
                    : _filename(filename),
                      _fullpath(""),
                      _dl(),
                      _interface(NULL),
                      _loaded(false) {}
            ~PluginHandle();
            const string &name() const;
            bool init(const StringData &expectedHash, string &errmsg, BSONObjBuilder &result);
            bool load(string &errmsg, BSONObjBuilder &result);
            string hashString() const;
            bool info(string &errmsg, BSONObjBuilder &result) const;
            bool unload(string &errmsg);
        };

        class Loader : boost::noncopyable {
            typedef map<string, shared_ptr<PluginHandle> > PluginMap;
            PluginMap _plugins;
            boost::filesystem::path _pluginsDir;

            static boost::filesystem::path defaultPluginsDir();

          public:
            Loader() : _pluginsDir(defaultPluginsDir()) {}
            void setPluginsDir(const string &path);
            void autoload(const vector<string> &plugins);
            bool load(const string &filename, StringData expectedHash, string &errmsg, BSONObjBuilder &result);
            bool list(string &errmsg, BSONObjBuilder &result) const;
            bool unload(const StringData &name, string &errmsg, BSONObjBuilder &result);
            void shutdown();
        };

        extern Loader *loader;

    }  // namespace plugins

}  // namespace mongo
