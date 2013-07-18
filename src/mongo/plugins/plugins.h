// @file plugin.h

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

namespace mongo {

    namespace plugins {

        class PluginInterface {
          public:
            virtual const string &name() const = 0;
            virtual void help(stringstream &h) const = 0;
            virtual bool load(string &errmsg, BSONObjBuilder &result) = 0;
            virtual void unload(string &errmsg) = 0;
            virtual ~PluginInterface() {}
        };

        typedef PluginInterface *(*GetInterfaceFunc)();

    } // namespace plugins

} // namespace mongo
