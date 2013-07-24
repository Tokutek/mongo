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

#include "mongo/plugins/dl.h"
#include "mongo/plugins/plugins.h"

namespace mongo {

    namespace plugins {

        class PluginHandle : boost::noncopyable {
            const string _filename;
            DLHandle _dl;
            PluginInterface *_interface;

          public:
            explicit PluginHandle(const string &filename) : _filename(filename) {}
            const string &name() const;
            const string &version() const;
            bool init(string &errmsg, BSONObjBuilder &result);
            bool load(string &errmsg, BSONObjBuilder &result);
            bool info(string &errmsg, BSONObjBuilder &result) const;
            bool unload(string &errmsg);
        };

        class Loader : boost::noncopyable {
            typedef map<string, shared_ptr<PluginHandle> > PluginMap;
            PluginMap _plugins;
          public:
            bool load(const StringData &filename, string &errmsg, BSONObjBuilder &result);
            bool list(string &errmsg, BSONObjBuilder &result) const;
            bool unload(const StringData &name, string &errmsg, BSONObjBuilder &result);
            void shutdown();
        };

        extern Loader loader;

    }  // namespace plugins

}  // namespace mongo
