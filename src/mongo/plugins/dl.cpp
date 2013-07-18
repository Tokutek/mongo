// @file dl.cpp

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

#include "mongo/plugins/dl.h"

#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

namespace mongo {

    namespace plugins {

        bool DLHandle::open(const char *filename, int flags) {
            verify(_h == NULL);
            _h = dlopen(filename, flags);
            return _h != NULL;
        }
        void *DLHandle::sym(const char *symbol) const {
            return dlsym(_h, symbol);
        }
        bool DLHandle::close() {
            verify(_h != NULL);
            int r = dlclose(_h);
            _h = NULL;
            return r == 0;
        }
        string DLHandle::error() const {
            char *err = dlerror();
            if (err == NULL) {
                return "unknown error";
            }
            else {
                return err;
            }
        }
        DLHandle::~DLHandle() {
            if (_h != NULL) {
                if (!close()) {
                    log() << "error in dlclose: " << error() << endl;
                }
            }
        }

    } // namespace plugins

} // namespace mongo
