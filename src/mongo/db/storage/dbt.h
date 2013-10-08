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

#include <db.h>
#include <errno.h>
#include <string>

namespace mongo {

    namespace storage {

        inline DBT dbt_make(const char *buf, size_t size, const int flags = 0) {
            DBT dbt;
            dbt.data = const_cast<char *>(buf);
            dbt.size = size;
            dbt.ulen = 0;
            dbt.flags = flags;
            return dbt;
        }

        inline void dbt_realloc(DBT *dbt, const void *data, const size_t size) {
            if (dbt->flags != DB_DBT_REALLOC || dbt->ulen < size) {
                dbt->ulen = size;
                dbt->data = realloc(dbt->flags == DB_DBT_REALLOC ? dbt->data : NULL, dbt->ulen);
                dbt->flags = DB_DBT_REALLOC;
                // Calling realloc() with a size of 0 may return NULL on some platforms
                verify(dbt->ulen == 0 || dbt->data != NULL);
            }
            dbt->size = size;
            memcpy(dbt->data, data, size);
        }

        inline void dbt_array_clear_and_resize(DBT_ARRAY *dbt_array,
                                               const size_t new_capacity) {
            const size_t old_capacity = dbt_array->capacity;
            if (old_capacity < new_capacity) {
                dbt_array->capacity = new_capacity;
                dbt_array->dbts = static_cast<DBT *>(
                                  realloc(dbt_array->dbts, new_capacity * sizeof(DBT)));
                memset(&dbt_array->dbts[old_capacity], 0, (new_capacity - old_capacity) * sizeof(DBT));
            }
            dbt_array->size = 0;
        }

        inline void dbt_array_push(DBT_ARRAY *dbt_array, const void *data, const size_t size) {
            verify(dbt_array->size < dbt_array->capacity);
            dbt_realloc(&dbt_array->dbts[dbt_array->size], data, size);
            dbt_array->size++;
        }

        // Manages an array of DBT_ARRAYs and the lifetime of the objects they store.
        //
        // It may be a good idea to cache two of these in the client object so
        // they're not created/destroyed every time a connection/transaction
        // does a single write. (multi inserts/updates/deletes come to mind)
        class DBTArrays : boost::noncopyable {
        public:
            DBTArrays(const size_t n) :
                _arrays(new DBT_ARRAY[n]),
                _n(n) {
                memset(_arrays.get(), 0, n * sizeof(DBT_ARRAY));
            }
            ~DBTArrays() {
                for (size_t i = 0; i < _n; i++) {
                    DBT_ARRAY *dbt_array = &_arrays[i];
                    for (size_t j = 0; j < dbt_array->capacity; j++) {
                        DBT *dbt = &dbt_array->dbts[j];
                        if (dbt->data != NULL && dbt->flags == DB_DBT_REALLOC) {
                            free(dbt->data);
                            dbt->data = NULL;
                        }
                    }
                    if (dbt_array->dbts != NULL) {
                        free(dbt_array->dbts);
                        dbt_array->dbts = NULL;
                    }
                }
            }
            DBT_ARRAY &operator[](size_t i) const {
                return _arrays.get()[i];
            }
            DBT_ARRAY *arrays() const {
                return _arrays.get();
            }
        private:
            scoped_array<DBT_ARRAY> _arrays;
            const size_t _n;
        };

    } // namespace storage

} // namespace mongo
