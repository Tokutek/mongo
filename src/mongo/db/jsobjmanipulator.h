/** jsobjManipulator.h */

/**
 *    Copyright (C) 2009 10gen Inc.
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

#pragma once

#include "mongo/db/jsobj.h"

namespace mongo {

    /** Manipulate the binary representation of a BSONElement in-place.
        Careful, this casts away const.
    */
    class BSONElementManipulator {
    public:
        BSONElementManipulator( const BSONElement &element ) :
            _element( element ) {
            verify( !_element.eoo() );
        }

        /** Change the value, in place, of the number. */
        void setNumber(double d) {
            if ( _element.type() == NumberDouble ) *reinterpret_cast< double * >( value() )  = d;
            else if ( _element.type() == NumberInt ) *reinterpret_cast< int * >( value() ) = (int) d;
            else verify(0);
        }

        void setLong(long long n) {
            verify( _element.type() == NumberLong );
            *reinterpret_cast< long long * >( value() ) = n;
        }

        void setInt(int n) {
            verify( _element.type() == NumberInt );
            *reinterpret_cast< int * >( value() ) = n;
        }

        void setBool(bool val) {
            verify( _element.type() == Bool );
            *reinterpret_cast< bool * >( value() ) = val;
        }

        /** Replace the type and value of the element with the type and value of e,
            preserving the original fieldName */
        void replaceTypeAndValue( const BSONElement &e ) {
            *data() = e.type();
            memcpy( value(), e.value(), e.valuesize() );
        }

    private:
        char *data() { return nonConst( _element.rawdata() ); }
        char *value() { return nonConst( _element.value() ); }
        static char *nonConst( const char *s ) { return const_cast< char * >( s ); }

        const BSONElement _element;
    };

} // namespace mongo
