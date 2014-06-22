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

        static const Ordering nullOrdering = Ordering::make(BSONObj());

        // [ ][HASMORE][x][y][canontype_4bits]
        enum CanonicalsEtc { 
            cminkey=1,
            cnull=2,
            cdouble=4,
            cstring=6,
            cbindata=7,
            coid=8,
            cfalse=10,
            ctrue=11,
            cdate=12,
            cmaxkey=14,
            cCANONTYPEMASK = 0xf,
            cY = 0x10,
            cint = cY | cdouble,
            cX = 0x20,
            clong = cX | cdouble,
            cZ = 0x30,
            cint64 = cZ | cdouble,
            cFULLTYPEMASK = 0x3f,
            cHASMORE = 0x40,
            cNOTUSED = 0x80 // but see IsBSON sentinel - this bit not usable without great care
        };

        unsigned char memcmpMagic() {
            return coid;
        }

        // bindata bson type
        // unused:
        //const unsigned BinDataLenMask = 0xf0;  // lengths are powers of 2 of this value
        const unsigned BinDataTypeMask = 0x0f; // 0-7 as you would expect, 8-15 are 128+value.  see BinDataType.
        const int BinDataLenMax = 32;
        const int BinDataLengthToCode[] = { 
            0x00, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 
            0x80, -1/*9*/, 0x90/*10*/, -1/*11*/, 0xa0/*12*/, -1/*13*/, 0xb0/*14*/, -1/*15*/,
            0xc0/*16*/, -1, -1, -1, 0xd0/*20*/, -1, -1, -1, 
            0xe0/*24*/, -1, -1, -1, -1, -1, -1, -1, 
            0xf0/*32*/ 
        };
        const int BinDataCodeToLength[] = { 
            0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 32
        };

        int binDataCodeToLength(int codeByte) { 
            return BinDataCodeToLength[codeByte >> 4];
        }

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

            BSONObjBuilder b(bb);
            const unsigned char *p = _keyData;
            while( 1 ) { 
                unsigned bits = *p++;

                switch( bits & cFULLTYPEMASK ) {
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

        static int compare(const unsigned char *&l, const unsigned char *&r) {
            int lt_real = (*l & cFULLTYPEMASK);
            int rt_real = (*r & cFULLTYPEMASK);
            int lt = (lt_real & cCANONTYPEMASK);
            int rt = (rt_real & cCANONTYPEMASK);
            int x = lt - rt;
            if( x ) 
                return x;

            l++; r++;

            // same type
            switch( lt ) { 
            case cdouble:
                {
                    if (unlikely(lt_real == cint64 && rt_real == cint64)) {
                        long long L = *reinterpret_cast<const long long *>(l);
                        long long R = *reinterpret_cast<const long long *>(r);
                        if (L < R) {
                            return -1;
                        }
                        if (L != R) {
                            return 1;
                        }
                    } else {
                        // We only pack numbers as cint64 if they are larger than the largest thing
                        // we would store as a double.  However, user inputted doubles can be larger
                        // than 2^52 and just be packed as doubles because they came that way, so we
                        // need to actually do the comparison, not just take the one that's packed
                        // as an int to be greater or lesser.
                        double L = (unlikely(lt_real == cint64)
                                    ? double(*reinterpret_cast<const long long *>(l))
                                    : (reinterpret_cast<const PackedDouble *>(l))->d);
                        double R = (unlikely(rt_real == cint64)
                                    ? double(*reinterpret_cast<const long long *>(r))
                                    : (reinterpret_cast<const PackedDouble *>(r))->d);
                        if (L < R) {
                            return -1;
                        }
                        if (L != R) {
                            return 1;
                        }
                    }
                    l += 8; r += 8;
                    break;
                }
            case cstring:
                {
                    int lsz = *l;
                    int rsz = *r;
                    int common = min(lsz, rsz);
                    l++; r++; // skip the size byte
                    // use memcmp as we (will) allow zeros in UTF8 strings
                    int res = memcmp(l, r, common);
                    if( res ) 
                        return res;
                    // longer string is the greater one
                    int diff = lsz-rsz;
                    if( diff ) 
                        return diff;
                    l += lsz; r += lsz;
                    break;
                }
            case cbindata:
                {
                    int L = *l;
                    int R = *r;
                    int llen = binDataCodeToLength(L);
                    int diff = L-R; // checks length and subtype simultaneously
                    if( diff ) {
                        // unfortunately nibbles are backwards to do subtype and len in one check (could bit swap...)
                        int rlen = binDataCodeToLength(R);
                        if( llen != rlen ) 
                            return llen - rlen;
                        return diff;
                    }
                    // same length, same type
                    l++; r++;
                    int res = memcmp(l, r, llen);
                    if( res ) 
                        return res;
                    l += llen; r += llen;
                    break;
                }
            case cdate:
                {
                    long long L = *((long long *) l);
                    long long R = *((long long *) r);
                    if( L < R )
                        return -1;
                    if( L > R )
                        return 1;
                    l += 8; r += 8;
                    break;
                }
            case coid:
                {
                    int res = memcmp(l, r, sizeof(OID));
                    if( res ) 
                        return res;
                    l += 12; r += 12;
                    break;
                }
            default:
                // all the others are a match -- e.g. null == null
                ;
            }

            return 0;
        }

        // at least one of this and right are traditional BSON format
        int NOINLINE_DECL KeyV1::compareHybrid(const KeyV1& right, const Ordering& order) const { 
            BSONObj L = toBson();
            BSONObj R = right.toBson();
            return L.woCompare(R, order, /*considerfieldname*/false);
        }

        int KeyV1::woCompare(const KeyV1& right, const Ordering &order) const {
            const unsigned char *l = _keyData;
            const unsigned char *r = right._keyData;

            if( (*l|*r) == IsBSON ) // only can do this if cNOTUSED maintained
                return compareHybrid(right, order);

            unsigned mask = 1;
            while( 1 ) { 
                char lval = *l; 
                char rval = *r;
                {
                    int x = compare(l, r); // updates l and r pointers
                    if( x ) {
                        if( order.descending(mask) )
                            x = -x;
                        return x;
                    }
                }

                {
                    int x = ((int)(lval & cHASMORE)) - ((int)(rval & cHASMORE));
                    if( x ) 
                        return x;
                    if( (lval & cHASMORE) == 0 )
                        break;
                }

                mask <<= 1;
            }

            return 0;
        }

        static unsigned sizes[] = {
            0,
            1, //cminkey=1,
            1, //cnull=2,
            0,
            9, //cdouble=4,
            0,
            0, //cstring=6,
            0,
            13, //coid=8,
            0,
            1, //cfalse=10,
            1, //ctrue=11,
            9, //cdate=12,
            0,
            1, //cmaxkey=14,
            0
        };

        inline unsigned sizeOfElement(const unsigned char *p) { 
            unsigned type = *p & cCANONTYPEMASK;
            unsigned sz = sizes[type];
            if( sz == 0 ) {
                if( type == cstring ) { 
                    sz = ((unsigned) p[1]) + 2;
                }
                else {
                    verify( type == cbindata );
                    sz = binDataCodeToLength(p[1]) + 2;
                }
            }
            return sz;
        }

        int KeyV1::dataSize() const { 
            const unsigned char *p = _keyData;
            if( !isCompactFormat() ) {
                return bson().objsize() + 1;
            }

            bool more;
            do { 
                unsigned z = sizeOfElement(p);
                more = (*p & cHASMORE) != 0;
                p += z;
            } while( more );
            return p - _keyData;
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
