// @file autoload.h

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

        /**
         * Calculates (based on the location of the binary) the default location to look for plugins
         * (in RPATH terms, $ORIGIN/../plugins).
         */
        string defaultDir(void);

        void autoload(const string &pluginsDir);

    } // namespace plugins

} // namespace mongo
