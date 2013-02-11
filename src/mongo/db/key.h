// @file key.h class(es) representing individual keys in a btree

/**
*    Copyright (C) 2011 10gen Inc.
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
 
#include "jsobj.h"

namespace mongo { 

    /** Key class for precomputing a small format index key that is denser than a traditional BSONObj. 

        Key is the new implementation.
    */
    class KeyOwned;

    class Key { 
        void operator=(const Key&); // disallowed just to make people be careful as we don't own the buffer
        Key(const KeyOwned&);     // disallowed as this is not a great idea as KeyOwned likely will go out of scope
    public:
        Key() { _keyData = 0; }
        ~Key() { DEV _keyData = (const unsigned char *) 1; }

        Key(const Key& rhs) : _keyData(rhs._keyData) { 
            dassert( _keyData > (const unsigned char *) 1 );
        }

        // explicit version of operator= to be safe
        void assign(const Key& rhs) { 
            _keyData = rhs._keyData;
        }

        /** @param keyData can be a buffer containing data in either BSON format, OR in Key format. 
                   when BSON, we are just a wrapper
        */
        explicit Key(const char *keyData) : _keyData((unsigned char *) keyData) { }

        int woCompare(const Key& r, const Ordering &o) const;
        bool woEqual(const Key& r) const;
        BSONObj toBson() const;
        string toString() const { return toBson().toString(); }

        /** get the key data we want to store in the btree bucket */
        const char * data() const { return (const char *) _keyData; }

        /** @return size of data() */
        int dataSize() const;

        /** only used by geo, which always has bson keys */
        BSONElement _firstElement() const { return bson().firstElement(); }
        bool isCompactFormat() const { return *_keyData != IsBSON; }

        bool isValid() const { return _keyData > (const unsigned char*)1; }
    protected:
        enum { IsBSON = 0xff };
        const unsigned char *_keyData;
        BSONObj bson() const {
            dassert( !isCompactFormat() );
            return BSONObj((const char *) _keyData+1);
        }
    private:
        int compareHybrid(const Key& right, const Ordering& order) const;
    };

    class KeyOwned : public Key { 
        void operator=(const KeyOwned&);
    public:
        /** @obj a BSON object to be translated to Key format.  If the object isn't 
                 representable in Key format (which happens, intentionally, at times)
                 it will stay as bson herein.
        */
        KeyOwned(const BSONObj& obj);

        /** makes a copy (memcpy's the whole thing) */
        KeyOwned(const Key& rhs);

    private:
        StackBufBuilder b;
        void traditional(const BSONObj& obj); // store as traditional bson not as compact format
    };

};
