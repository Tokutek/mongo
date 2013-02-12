// namespace.h

/**
*    Copyright (C) 2008 10gen Inc.
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

#include <cstring>
#include <string>

namespace mongo {

    class Namespace {
    public:
        explicit Namespace(const char *ns) { *this = ns; }
        Namespace& operator=(const char *ns) {
            // we fill the remaining space with all zeroes here.  as the full Namespace struct is in
            // the datafiles (the .ns files specifically), that is helpful as then they are deterministic
            // in the bytes they have for a given sequence of operations.  that makes testing and debugging
            // the data files easier.
            //
            // if profiling indicates this method is a significant bottleneck, we could have a version we
            // use for reads which does not fill with zeroes, and keep the zeroing behavior on writes.
            //
            unsigned len = strlen(ns);
            uassert( 10080 , "ns name too long, max size is 128", len < MaxNsLen);
            memset(buf, 0, MaxNsLen);
            memcpy(buf, ns, len);
            return *this;
        }

        bool hasDollarSign() const { return strchr( buf , '$' ) > 0;  }
        bool operator==(const char *r) const { return strcmp(buf, r) == 0; }
        bool operator==(const Namespace& r) const { return strcmp(buf, r.buf) == 0; }
        bool operator< (const char *r) const { return strcmp(buf, r) < 0; }
        bool operator< (const Namespace& r) const { return strcmp(buf, r.buf) < 0; }

        // value returned is always > 0
        int hash() const {
            unsigned x = 0;
            const char *p = buf;
            while ( *p ) {
                x = x * 131 + *p;
                p++;
            }
            return (x & 0x7fffffff) | 0x8000000; // must be > 0
        }

        size_t size() const { return strlen( buf ); }

        string toString() const { return buf; }
        operator string() const { return buf; }

        /** ( foo.bar ).getSisterNS( "blah" ) == foo.blah
            perhaps this should move to the NamespaceString helper?
         */
        string getSisterNS( const char * local ) const {
            verify( local && local[0] != '.' );
            string old(buf);
            if ( old.find( "." ) != string::npos )
                old = old.substr( 0 , old.find( "." ) );
            return old + "." + local;
        }

        enum MaxNsLenValue { MaxNsLen = 128 };
    private:
        char buf[MaxNsLen];
    };

} // namespace mongo
