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
#include "mongo/bson/bsonobj.h"

#include <db.h>

namespace mongo {

    class Descriptor;

    namespace storage {

        // Wrapper for a ydb dictionary
        class Dictionary : boost::noncopyable {
        public:
            Dictionary(const string &dname, const BSONObj &info,
                       const mongo::Descriptor &descriptor,
                       const bool may_create, const bool hot_index,
                       const bool set_memcmp_magic = false);
            ~Dictionary();

            // @param change the parameters described by the info object
            // @param wasBuilder used to describe the old state
            // @return true if something was changed
            bool changeAttributes(const BSONObj &info, BSONObjBuilder &wasBuilder);

            DB *db() const {
                return _db;
            }

            int close();

            class NeedsCreate : std::exception {};

        private:
            void open(const mongo::Descriptor &descriptor,
                      const bool may_create, const bool hot_index,
                      const bool set_memcmp_magic);

            const string _dname;
            DB *_db;
        };

    } // namespace storage

} // namespace mongo

