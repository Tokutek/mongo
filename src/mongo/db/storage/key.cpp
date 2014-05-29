// @file key.cpp

/**
*    Copyright (C) 2011 10gen Inc.
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

#include "mongo/bson/util/builder.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/storage/key.h"
#include "mongo/server.h"

namespace mongo {

    namespace storage {

        /** object cannot be represented in compact format.  so store in traditional bson format 
            with a leading sentinel byte IsBSON to indicate it's in that format.

            Given that the KeyV1Owned constructor already grabbed a bufbuilder, we reuse it here 
            so that we don't have to do an extra malloc.
        */
        void KeyV1Owned::traditional(const BSONObj& obj) { 
            b.reset();
            b.appendUChar(IsBSON);
            b.appendBuf(obj.objdata(), obj.objsize());
            _keyData = (const unsigned char *) b.buf();
        }

        KeyV1Owned::KeyV1Owned(const KeyV1& rhs) {
            b.appendBuf( rhs.data(), rhs.dataSize() );
            _keyData = (const unsigned char *) b.buf();
            dassert( b.len() == dataSize() ); // check datasize method is correct
            dassert( (*_keyData & cNOTUSED) == 0 );
        }

        // fromBSON to KeyV1 format
        KeyV1Owned::KeyV1Owned(const BSONObj& obj) {
            // bindata bson type
            // unused:
            //const unsigned BinDataLenMask = 0xf0;  // lengths are powers of 2 of this value
            static const int BinDataLenMax = 32;
            static const int BinDataLengthToCode[] = { 
                0x00, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 
                0x80, -1/*9*/, 0x90/*10*/, -1/*11*/, 0xa0/*12*/, -1/*13*/, 0xb0/*14*/, -1/*15*/,
                0xc0/*16*/, -1, -1, -1, 0xd0/*20*/, -1, -1, -1, 
                0xe0/*24*/, -1, -1, -1, -1, -1, -1, -1, 
                0xf0/*32*/ 
            };

            BSONObj::iterator i(obj);
            unsigned char bits = 0;
            while( 1 ) { 
                BSONElement e = i.next();
                if( i.more() )
                    bits |= cHASMORE;
                switch( e.type() ) { 
                case MinKey:
                    b.appendUChar(cminkey|bits);
                    break;
                case jstNULL:
                    b.appendUChar(cnull|bits);
                    break;
                case MaxKey:
                    b.appendUChar(cmaxkey|bits);
                    break;
                case Bool:
                    b.appendUChar( (e.boolean()?ctrue:cfalse) | bits );
                    break;
                case jstOID:
                    b.appendUChar(coid|bits);
                    b.appendBuf(&e.__oid(), sizeof(OID));
                    break;
                case BinData:
                    {
                        int t = e.binDataType();
                        // 0-7 and 0x80 to 0x87 are supported by KeyV1
                        if( (t & 0x78) == 0 && t != ByteArrayDeprecated ) {
                            int len;
                            const char * d = e.binData(len);
                            if( len <= BinDataLenMax ) {
                                int code = BinDataLengthToCode[len];
                                if( code >= 0 ) {
                                    if( t >= 128 )
                                        t = (t-128) | 0x08;
                                    dassert( (code&t) == 0 );
                                    b.appendUChar( cbindata|bits );
                                    b.appendUChar( code | t );
                                    b.appendBuf(d, len);
                                    break;
                                }
                            }
                        }
                        traditional(obj);
                        return;
                    }
                case Date:
                    b.appendUChar(cdate|bits);
                    b.appendStruct(e.date());
                    break;
                case String:
                    {
                        b.appendUChar(cstring|bits);
                        // note we do not store the terminating null, to save space.
                        unsigned x = (unsigned) e.valuestrsize() - 1;
                        if( x > 255 ) { 
                            traditional(obj);
                            return;
                        }
                        b.appendUChar(x);
                        b.appendBuf(e.valuestr(), x);
                        break;
                    }
                case NumberInt:
                    b.appendUChar(cint|bits);
                    b.appendNum((double) e._numberInt());
                    break;
                case NumberLong:
                    {
                        long long n = e._numberLong();
                        long long m = 2LL << 52;
                        DEV {
                            long long d = m-1;
                            verify( ((long long) ((double) -d)) == -d );
                        }
                        if( n >= m || n <= -m ) {
                            // can't represent exactly as a double
                            b.appendUChar(cint64|bits);
                            b.appendNum(n);
                        } else {
                            b.appendUChar(clong|bits);
                            b.appendNum((double) n);
                        }
                        break;
                    }
                case NumberDouble:
                    {
                        double d = e._numberDouble();
                        if( isNaN(d) ) {
                            traditional(obj);
                            return;
                        }
                        b.appendUChar(cdouble|bits);
                        b.appendNum(d);
                        break;
                    }
                default:
                    // if other types involved, store as traditional BSON
                    traditional(obj);
                    return;
                }
                if( !i.more() )
                    break;
                bits = 0;
            }
            _keyData = (const unsigned char *) b.buf();
            dassert( b.len() == dataSize() ); // check datasize method is correct
            dassert( (*_keyData & cNOTUSED) == 0 );
        }

        BSONObj KeyV1::toBson(BufBuilder &bb) const { 
            verify( _keyData != 0 );
            if( !isCompactFormat() )
                return bson();

            static const unsigned BinDataTypeMask = 0x0f; // 0-7 as you would expect, 8-15 are 128+value.  see BinDataType.

            BSONObjBuilder b(bb);
            const unsigned char *p = _keyData;
            while( 1 ) { 
                unsigned bits = *p++;

                switch( bits & 0x3f ) {
                    case cminkey: b.appendMinKey(""); break;
                    case cnull:   b.appendNull(""); break;
                    case cfalse:  b.appendBool("", false); break;
                    case ctrue:   b.appendBool("", true); break;
                    case cmaxkey: 
                        b.appendMaxKey(""); 
                        break;
                    case cstring:
                        {
                            unsigned sz = *p++;
                            // we build the element ourself as we have to null terminate it
                            BufBuilder &bb = b.bb();
                            bb.appendNum((char) String);
                            bb.appendUChar(0); // fieldname ""
                            bb.appendNum(sz+1);
                            bb.appendBuf(p, sz);
                            bb.appendUChar(0); // null char at end of string
                            p += sz;
                            break;
                        }
                    case coid:
                        b.appendOID("", (OID *) p);
                        p += sizeof(OID);
                        break;
                    case cbindata:
                        {
                            int len = binDataCodeToLength(*p);
                            int subtype = (*p) & BinDataTypeMask;
                            if( subtype & 0x8 ) { 
                                subtype = (subtype & 0x7) | 0x80;
                            }
                            b.appendBinData("", len, (BinDataType) subtype, ++p);
                            p += len;
                            break;
                        }
                    case cdate:
                        b.appendDate("", (Date_t&) *p);
                        p += 8;
                        break;
                    case cdouble:
                        b.append("", (double&) *p);
                        p += sizeof(double);
                        break;
                    case cint:
                        b.append("", static_cast< int >((reinterpret_cast< const PackedDouble& >(*p)).d));
                        p += sizeof(double);
                        break;
                    case clong:
                        b.append("", static_cast< long long>((reinterpret_cast< const PackedDouble& >(*p)).d));
                        p += sizeof(double);
                        break;
                    case cint64:
                        b.append("", *reinterpret_cast<const long long *>(p));
                        p += sizeof(long long);
                        break;
                    default:
                        verify(false);
                }

                if( (bits & cHASMORE) == 0 )
                    break;
            }
            return b.done();
        }

        // at least one of this and right are traditional BSON format
        int NOINLINE_DECL KeyV1::compareHybrid(const KeyV1& right, const Ordering& order) const { 
            BSONObj L = toBson();
            BSONObj R = right.toBson();
            return L.woCompare(R, order, /*considerfieldname*/false);
        }

        bool KeyV1::woEqual(const KeyV1& right) const {
            const unsigned char *l = _keyData;
            const unsigned char *r = right._keyData;

            if( (*l|*r) == IsBSON ) {
                return toBson().equal(right.toBson());
            }

            while( 1 ) { 
                char lval = *l; 
                char rval = *r;
                if( (lval&(cCANONTYPEMASK|cHASMORE)) != (rval&(cCANONTYPEMASK|cHASMORE)) )
                    return false;
                l++; r++;
                switch( lval&cCANONTYPEMASK ) { 
                case coid:
                    if( *((unsigned*) l) != *((unsigned*) r) )
                        return false;
                    l += 4; r += 4;
                case cdate:
                    if( *((unsigned long long *) l) != *((unsigned long long *) r) )
                        return false;
                    l += 8; r += 8;
                    break;
                case cdouble:
                    if( (reinterpret_cast< const PackedDouble* > (l))->d != (reinterpret_cast< const PackedDouble* >(r))->d )
                        return false;
                    l += 8; r += 8;
                    break;
                case cstring:
                    {
                        if( *l != *r ) 
                            return false; // not same length
                        unsigned sz = ((unsigned) *l) + 1;
                        if( memcmp(l, r, sz) )
                            return false;
                        l += sz; r += sz;
                        break;
                    }
                case cbindata:
                    {
                        if( *l != *r )
                            return false; // len or subtype mismatch
                        int len = binDataCodeToLength(*l) + 1;
                        if( memcmp(l, r, len) ) 
                            return false;
                        l += len; r += len;
                        break;
                    }
                case cminkey:
                case cnull:
                case cfalse:
                case ctrue:
                case cmaxkey:
                    break;
                default:
                    verify(false);
                }
                if( (lval&cHASMORE) == 0 )
                    break;
            }
            return true;
        }

    } // namespace storage

} // namespace mongo
