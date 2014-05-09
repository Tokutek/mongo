// server_parameters_inline.h

/**
*    Copyright (C) 2012 10gen Inc.
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
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>

#include "mongo/util/stringutils.h"
#include "mongo/base/units.h"

namespace mongo {

    template<typename T>
    inline Status ExportedServerParameter<T>::set( const BSONElement& newValueElement ) {
        T newValue;

        if ( !newValueElement.coerce( &newValue) )
            return Status( ErrorCodes::BadValue, "can't set value" );

        return set( newValue );
    }

    template<typename T>
    inline Status ExportedServerParameter<T>::set( const T& newValue ) {

        Status v = validate( newValue );
        if ( !v.isOK() )
            return v;

        *_value = newValue;
        return Status::OK();
    }

    template<>
    inline Status ExportedServerParameter<int>::setFromString( const string& str ) {
        return set( atoi(str.c_str() ) );
    }

    template<>
    inline Status ExportedServerParameter<uint32_t>::setFromString( const string& str ) {
        unsigned long int val = strtoul(str.c_str(), NULL, 0);
        if (val == ULONG_MAX) {
            return Status(ErrorCodes::BadValue, strerror(errno));
        }
        return set( val );
    }

    template<>
    inline Status ExportedServerParameter<uint64_t>::setFromString( const string& str ) {
        unsigned long long int val = strtoull(str.c_str(), NULL, 0);
        if (val == ULLONG_MAX) {
            return Status(ErrorCodes::BadValue, strerror(errno));
        }
        return set( val );
    }

    template<>
    inline Status ExportedServerParameter<BytesQuantity<int> >::setFromString( const string& str ) {
        return set(BytesQuantity<int>::fromString(str));
    }

    template<>
    inline Status ExportedServerParameter<BytesQuantity<uint64_t> >::setFromString( const string& str ) {
        return set(BytesQuantity<uint64_t>::fromString(str));
    }

    template<>
    inline Status ExportedServerParameter<double>::setFromString( const string& str ) {
        const char* start = str.c_str();
        char* end;
        double d = strtod( str.c_str(), &end );
        if ( start == end )
            return Status( ErrorCodes::BadValue, "not a double" );
        return set( d );
    }

    template<>
    inline Status ExportedServerParameter<string>::setFromString( const string& str ) {
        return set( str );
    }

    template<>
    inline Status ExportedServerParameter<bool>::setFromString( const string& str ) {
        if ( str == "true" ||
             str == "1" )
            return set(true);
        if ( str == "false" ||
             str == "0" )
            return set(false);
        return Status( ErrorCodes::BadValue, "can't convert string to bool" );
    }


    template<>
    inline Status ExportedServerParameter< vector<string> >::setFromString( const string& str ) {
        vector<string> v;
        splitStringDelim( str, &v, ',' );
        return set( v );
    }

    template<>
    inline void ExportedServerParameter<BytesQuantity<int> >::append( BSONObjBuilder& b, const string& name ) {
        b.append( name, (int) *_value );
    }

    template<>
    inline void ExportedServerParameter<BytesQuantity<uint64_t> >::append( BSONObjBuilder& b, const string& name ) {
        b.append( name, (long long) *_value );
    }

}
