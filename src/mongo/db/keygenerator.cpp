// keygenerator.cpp

/**
*    Copyright (C) 2008 10gen Inc.
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
#include "mongo/db/hasher.h"
#include "mongo/db/keygenerator.h"
#include "mongo/db/storage/assert_ids.h"
#include "mongo/util/stringutils.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/text.h"

namespace mongo {

    /* Takes a BSONElement, seed and hash version, and outputs the
     * 64-bit hash used for this index
     * E.g. if the element is {a : 3} this outputs v1-hash(3)
     * */
    long long int HashKeyGenerator::makeSingleKey(const BSONElement &e,
                                                  const HashSeed &seed,
                                                  const int v) {
        massert(16245, "Only hash version 0 has been defined", v == 0);
        return BSONElementHasher::hash64( e , seed );
    }

    void HashKeyGenerator::getKeys(const BSONObj &obj, BSONObjSet &keys) {
        const char *hashedFieldPtr = _hashedField;
        const BSONElement &fieldVal = obj.getFieldDottedOrArray( hashedFieldPtr );
        uassert( storage::ASSERT_IDS::CannotHashArrays,
                 "Error: hashed indexes do not currently support array values",
                 fieldVal.type() != Array );

        if (!fieldVal.eoo()) {
            BSONObj key = BSON("" << makeSingleKey(fieldVal, _seed, _hashVersion));
            keys.insert(key);
        } else if (!_sparse) {
            BSONObj key = BSON("" << makeSingleKey(nullElt, _seed, _hashVersion));
            keys.insert(key);
        }
    }

    void KeyGenerator::getKeys(const BSONObj &obj, BSONObjSet &keys) const {
        vector<const char *> fieldNames(_fieldNames);
        getKeys(obj, fieldNames, _sparse, keys);
    }     

    void KeyGenerator::getKeys(const BSONObj &obj, vector<const char *> &fieldNames,
                               const bool sparse, BSONObjSet &keys) {
        vector<BSONElement> fixed( fieldNames.size() );
        _getKeys( fieldNames , fixed , obj, sparse, keys );
        if ( keys.empty() && ! sparse ) {
            BSONObjBuilder nullKey(128);
            for (size_t i = 0; i < fieldNames.size(); i++) {
                nullKey.appendNull("");
            }
            keys.insert( nullKey.obj() );
        }
    }
        
    /**
     * @param arrayNestedArray - set if the returned element is an array nested directly within arr.
     */
    BSONElement KeyGenerator::extractNextElement( const BSONObj &obj, const BSONObj &arr, const char *&field, bool &arrayNestedArray ) {
        string firstField = mongoutils::str::before( field, '.' );
        bool haveObjField = !obj.getField( firstField ).eoo();
        BSONElement arrField = arr.getField( firstField );
        bool haveArrField = !arrField.eoo();

        // An index component field name cannot exist in both a document array and one of that array's children.
        uassert( storage::ASSERT_IDS::AmbiguousFieldNames,
                 mongoutils::str::stream() << "Ambiguous field name found in array (do not use numeric field names in embedded elements in an array), field: '" << arrField.fieldName() << "' for array: " << arr,
                 !haveObjField || !haveArrField );

        arrayNestedArray = false;
                    if ( haveObjField ) {
            return obj.getFieldDottedOrArray( field );
        }
        else if ( haveArrField ) {
            if ( arrField.type() == Array ) {
                arrayNestedArray = true;
            }
            return arr.getFieldDottedOrArray( field );
        }
        return BSONElement();
    }
        
    void KeyGenerator::_getKeysArrEltFixed( vector<const char*> &fieldNames , vector<BSONElement> &fixed , const BSONElement &arrEntry, const bool sparse, BSONObjSet &keys, int numNotFound, const BSONElement &arrObjElt, const set< unsigned > &arrIdxs, bool mayExpandArrayUnembedded ) {
        // set up any terminal array values
        for( set<unsigned>::const_iterator j = arrIdxs.begin(); j != arrIdxs.end(); ++j ) {
            if ( *fieldNames[ *j ] == '\0' ) {
                fixed[ *j ] = mayExpandArrayUnembedded ? arrEntry : arrObjElt;
            }
        }
        // recurse
        _getKeys( fieldNames, fixed, ( arrEntry.type() == Object ) ? arrEntry.embeddedObject() : BSONObj(), sparse, keys, numNotFound, arrObjElt.embeddedObject() );        
    }
        
        /**
         * @param fieldNames - fields to index, may be postfixes in recursive calls
         * @param fixed - values that have already been identified for their index fields
         * @param obj - object from which keys should be extracted, based on names in fieldNames
         * @param keys - set where index keys are written
         * @param numNotFound - number of index fields that have already been identified as missing
         * @param array - array from which keys should be extracted, based on names in fieldNames
         *        If obj and array are both nonempty, obj will be one of the elements of array.
         */        
    void KeyGenerator::_getKeys( vector<const char*> fieldNames , vector<BSONElement> fixed , const BSONObj &obj, const bool sparse, BSONObjSet &keys, int numNotFound, const BSONObj &array ) {
        BSONElement arrElt;
        set<unsigned> arrIdxs;
        bool mayExpandArrayUnembedded = true;
        for( unsigned i = 0; i < fieldNames.size(); ++i ) {
            if ( *fieldNames[ i ] == '\0' ) {
                continue;
            }
            
            bool arrayNestedArray;
            // Extract element matching fieldName[ i ] from object xor array.
            BSONElement e = extractNextElement( obj, array, fieldNames[ i ], arrayNestedArray );
            
            if ( e.eoo() ) {
                // if field not present, set to null
                fixed[ i ] = nullElt;
                // done expanding this field name
                fieldNames[ i ] = "";
                numNotFound++;
            }
            else if ( e.type() == Array ) {
                arrIdxs.insert( i );
                if ( arrElt.eoo() ) {
                    // we only expand arrays on a single path -- track the path here
                    arrElt = e;
                }
                else if ( e.rawdata() != arrElt.rawdata() ) {
                    // enforce single array path here
                    uasserted( storage::ASSERT_IDS::ParallelArrays, 
                               mongoutils::str::stream() << "cannot index parallel arrays "
                               << "[" << e.fieldName() << "] [" << arrElt.fieldName() << "]" );
                }
                if ( arrayNestedArray ) {
                    mayExpandArrayUnembedded = false;   
                }
            }
            else {
                // not an array - no need for further expansion
                fixed[ i ] = e;
            }
        }
        
        if ( arrElt.eoo() ) {
            // No array, so generate a single key.
            if ( sparse && numNotFound == (int) fieldNames.size() ) {
                return;
            }            
            BSONObjBuilder b(128);
            for( vector< BSONElement >::iterator i = fixed.begin(); i != fixed.end(); ++i ) {
                b.appendAs( *i, "" );
            }
            keys.insert( b.obj() );
        }
        else if ( arrElt.embeddedObject().firstElement().eoo() ) {
            // Empty array, so set matching fields to undefined.
            _getKeysArrEltFixed( fieldNames, fixed, undefinedElt, sparse, keys, numNotFound, arrElt, arrIdxs, true );
        }
        else {
            // Non empty array that can be expanded, so generate a key for each member.
            BSONObj arrObj = arrElt.embeddedObject();
            BSONObjIterator i( arrObj );
            while( i.more() ) {
                _getKeysArrEltFixed( fieldNames, fixed, i.next(), sparse, keys, numNotFound, arrElt, arrIdxs, mayExpandArrayUnembedded );
            }
        }
    }

} // namespace mongo
