// @file dl.h

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

// TODO: feature detection to disable plugin code if we don't have dlopen/dlsym
// Include this in the header so that users of DLHandle have macros, e.g. RTLD_NOW3
#include <dlfcn.h>

namespace mongo {

    namespace plugins {

        /**
         * Wrapper for dlopen/dlsym/dlclose/dlerror that closes the handle in the destructor.
         */
        class DLHandle : boost::noncopyable {
            void *_h;
          public:
            DLHandle() : _h(NULL) {}
            ~DLHandle();
            bool open(const char *filename, int flags);
            void *sym(const char *symbol) const;
            bool close();
            string error() const;
        };

    } // namespace plugins

} // namespace mongo
