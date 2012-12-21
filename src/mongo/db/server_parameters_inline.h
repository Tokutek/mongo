// server_parameters_inline.h

#include "mongo/util/stringutils.h"

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



}
