//@file update_internal.h

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
 */

#include "mongo/pch.h"

#include "mongo/bson/bson_builder_base.h"
#include "mongo/db/index_set.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/jsobjmanipulator.h"
#include "mongo/db/matcher.h"
#include "mongo/util/embedded_builder.h"
#include "mongo/util/stringutils.h"

namespace mongo {

    class ModState;
    class ModSetState;

    /* Used for modifiers such as $inc, $set, $push, ...
     * stores the info about a single operation
     * once created should never be modified
     */
    struct Mod {
        // See opFromStr below
        //        0    1    2     3         4     5          6    7      8       9       10    11        12           13         14
        enum Op { INC, SET, PUSH, PUSH_ALL, PULL, PULL_ALL , POP, UNSET, BITAND, BITOR , BIT , ADDTOSET, RENAME_FROM, RENAME_TO, SET_ON_INSERT } op;

        static const char* modNames[];
        static unsigned modNamesNum;

        const char* fieldName;
        const char* shortFieldName;

        BSONElement elt; // x:5 note: this is the actual element from the updateobj
        boost::shared_ptr<Matcher> matcher;
        bool matcherOnPrimitive;

        void init( Op o , BSONElement& e ) {
            op = o;
            elt = e;
            if ( op == PULL && e.type() == Object ) {
                BSONObj t = e.embeddedObject();
                if ( t.firstElement().getGtLtOp() == 0 ) {
                    matcher.reset( new Matcher( t ) );
                    matcherOnPrimitive = false;
                }
                else {
                    matcher.reset( new Matcher( BSON( "" << t ) ) );
                    matcherOnPrimitive = true;
                }
            }
        }

        void setFieldName( const char* s ) {
            fieldName = s;
            shortFieldName = strrchr( fieldName , '.' );
            if ( shortFieldName )
                shortFieldName++;
            else
                shortFieldName = fieldName;
        }

        /**
         * @param in increments the actual value inside in
         */
        void incrementMe( BSONElement& in ) const {
            BSONElementManipulator manip( in );
            switch ( in.type() ) {
            case NumberDouble:
                manip.setNumber( elt.numberDouble() + in.numberDouble() );
                break;
            case NumberLong:
                manip.setLong( elt.numberLong() + in.numberLong() );
                break;
            case NumberInt:
                manip.setInt( elt.numberInt() + in.numberInt() );
                break;
            default:
                verify(0);
            }
        }

        void appendIncremented( BSONBuilderBase& bb , const BSONElement& in, ModState& ms ) const;

        bool operator<( const Mod& other ) const {
            return strcmp( fieldName, other.fieldName ) < 0;
        }

        bool arrayDep() const {
            switch (op) {
            case PUSH:
            case PUSH_ALL:
            case POP:
                return true;
            default:
                return false;
            }
        }

        void apply( BSONBuilderBase& b , BSONElement in , ModState& ms ) const;

        /**
         * @return true iff toMatch should be removed from the array
         */
        bool _pullElementMatch( BSONElement& toMatch ) const;

        void _checkForAppending( const BSONElement& e ) const {
            if ( e.type() == Object ) {
                // this is a tiny bit slow, but rare and important
                // only when setting something TO an object, not setting something in an object
                // and it checks for { $set : { x : { 'a.b' : 1 } } }
                // which is feel has been common
                uassert( 12527 , "not okForStorage" , e.embeddedObject().okForStorage() );
            }
        }

        bool isEach() const {
            if ( elt.type() != Object )
                return false;
            BSONElement e = elt.embeddedObject().firstElement();
            if ( e.type() != Array )
                return false;
            return strcmp( e.fieldName() , "$each" ) == 0;
        }

        BSONObj getEach() const {
            return elt.embeddedObjectUserCheck().firstElement().embeddedObjectUserCheck();
        }

        void parseEach( BSONElementSet& s ) const {
            BSONObjIterator i(getEach());
            while ( i.more() ) {
                s.insert( i.next() );
            }
        }

        const char* renameFrom() const {
            massert( 13492, "mod must be RENAME_TO type", op == Mod::RENAME_TO );
            return elt.fieldName();
        }
    };

    /**
     * stores a set of Mods
     * once created, should never be changed
     */
    class ModSet : boost::noncopyable {
        typedef map<string,Mod> ModHolder;
        ModHolder _mods;
        int _isIndexed;
        bool _hasDynamicArray;

        static Mod::Op opFromStr( const char* fn ) {
            verify( fn[0] == '$' );
            switch( fn[1] ) {
            case 'i': {
                if ( fn[2] == 'n' && fn[3] == 'c' && fn[4] == 0 )
                    return Mod::INC;
                break;
            }
            case 's': {
                if ( fn[2] == 'e' && fn[3] == 't' ) {
                    if ( fn[4] == 0 ) {
                        return Mod::SET;
                    }
                    else if ( fn[4] == 'O' && fn[5] == 'n' && fn[6] == 'I' && fn[7] == 'n' &&
                              fn[8] == 's' && fn[9] == 'e' && fn[10] == 'r' && fn[11] == 't' &&
                              fn[12] == 0 ) {
                        return Mod::SET_ON_INSERT;
                    }
                }
                break;
            }
            case 'p': {
                if ( fn[2] == 'u' ) {
                    if ( fn[3] == 's' && fn[4] == 'h' ) {
                        if ( fn[5] == 0 )
                            return Mod::PUSH;
                        if ( fn[5] == 'A' && fn[6] == 'l' && fn[7] == 'l' && fn[8] == 0 )
                            return Mod::PUSH_ALL;
                    }
                    else if ( fn[3] == 'l' && fn[4] == 'l' ) {
                        if ( fn[5] == 0 )
                            return Mod::PULL;
                        if ( fn[5] == 'A' && fn[6] == 'l' && fn[7] == 'l' && fn[8] == 0 )
                            return Mod::PULL_ALL;
                    }
                }
                else if ( fn[2] == 'o' && fn[3] == 'p' && fn[4] == 0 )
                    return Mod::POP;
                break;
            }
            case 'u': {
                if ( fn[2] == 'n' && fn[3] == 's' && fn[4] == 'e' && fn[5] == 't' && fn[6] == 0 )
                    return Mod::UNSET;
                break;
            }
            case 'b': {
                if ( fn[2] == 'i' && fn[3] == 't' ) {
                    if ( fn[4] == 0 )
                        return Mod::BIT;
                    if ( fn[4] == 'a' && fn[5] == 'n' && fn[6] == 'd' && fn[7] == 0 )
                        return Mod::BITAND;
                    if ( fn[4] == 'o' && fn[5] == 'r' && fn[6] == 0 )
                        return Mod::BITOR;
                }
                break;
            }
            case 'a': {
                if ( fn[2] == 'd' && fn[3] == 'd' ) {
                    // add
                    if ( fn[4] == 'T' && fn[5] == 'o' && fn[6] == 'S' && fn[7] == 'e' && fn[8] == 't' && fn[9] == 0 )
                        return Mod::ADDTOSET;

                }
                break;
            }
            case 'r': {
                if ( fn[2] == 'e' && fn[3] == 'n' && fn[4] == 'a' && fn[5] == 'm' && fn[6] =='e' ) {
                    return Mod::RENAME_TO; // with this return code we handle both RENAME_TO and RENAME_FROM
                }
                break;
            }
            default: break;
            }
            uassert( 10161 ,  "Invalid modifier specified " + string( fn ), false );
            return Mod::INC;
        }

        ModSet() {}

        void updateIsIndexed( const Mod& m, const IndexPathSet& idxKeys ) {
            if ( idxKeys.mightBeIndexed( m.fieldName ) ) {
                _isIndexed++;
            }
        }

    public:

        ModSet( const BSONObj& from,
                const IndexPathSet& idxKeys = IndexPathSet() );

        /**
         * re-check if this mod is impacted by indexes
         */
        void updateIsIndexed( const IndexPathSet& idxKeys );



        // TODO: this is inefficient - should probably just handle when iterating
        ModSet * fixDynamicArray( const string& elemMatchKey ) const;

        bool hasDynamicArray() const { return _hasDynamicArray; }

        /**
         * creates a ModSetState suitable for operation on obj
         * doesn't change or modify this ModSet or any underlying Mod
         *
         * flag 'insertion' differentiates between obj existing prior to this update.
         */
        auto_ptr<ModSetState> prepare( const BSONObj& obj, bool insertion = false ) const;

        /**
         * given a query pattern, builds an object suitable for an upsert
         * will take the query spec and combine all $ operators
         */
        BSONObj createNewFromQuery( const BSONObj& query );

        int isIndexed() const { return _isIndexed; }

        unsigned size() const { return _mods.size(); }

        bool haveModForField( const char* fieldName ) const {
            return _mods.find( fieldName ) != _mods.end();
        }

        bool haveConflictingMod( const string& fieldName ) {
            size_t idx = fieldName.find( '.' );
            if ( idx == string::npos )
                idx = fieldName.size();

            ModHolder::const_iterator start = _mods.lower_bound(fieldName.substr(0,idx));
            for ( ; start != _mods.end(); start++ ) {
                FieldCompareResult r = compareDottedFieldNames( fieldName , start->first ,
                                                               LexNumCmp( true ) );
                switch ( r ) {
                case LEFT_SUBFIELD: return true;
                case LEFT_BEFORE: return false;
                case SAME: return true;
                case RIGHT_BEFORE: return false;
                case RIGHT_SUBFIELD: return true;
                }
            }
            return false;
        }

    };

    /**
     * stores any information about a single Mod operating on a single Object
     */
    class ModState : boost::noncopyable {
    public:
        const Mod* m;
        BSONElement old;
        BSONElement newVal;
        BSONObj _objData;

        const char* fixedOpName;
        BSONElement* fixed;
        BSONArray fixedArray;
        bool forceEmptyArray;
        bool forcePositional;
        int position;
        int DEPRECATED_pushStartSize;

        BSONType incType;
        int incint;
        double incdouble;
        long long inclong;

        bool dontApply;

        ModState() {
            fixedOpName = 0;
            fixed = 0;
            forceEmptyArray = false;
            forcePositional = false;
            position = 0;
            DEPRECATED_pushStartSize = -1;
            incType = EOO;
            dontApply = false;
        }

        Mod::Op op() const {
            return m->op;
        }

        const char* fieldName() const {
            return m->fieldName;
        }

        bool DEPRECATED_needOpLogRewrite() const {
            if ( dontApply )
                return false;

            if ( fixed || fixedOpName || incType )
                return true;

            switch( op() ) {
            case Mod::RENAME_FROM:
            case Mod::RENAME_TO:
                return true;
            case Mod::BIT:
            case Mod::BITAND:
            case Mod::BITOR:
                return true;
            default:
                return false;
            }
        }

        const char* getOpLogName() const;
        void appendForOpLog( BSONObjBuilder& b ) const;

        void apply( BSONBuilderBase& b , BSONElement in ) {
            m->apply( b , in , *this );
        }

        void appendIncValue( BSONBuilderBase& b , bool useFullName ) const {
            const char* n = useFullName ? m->fieldName : m->shortFieldName;

            switch ( incType ) {
            case NumberDouble:
                b.append( n , incdouble ); break;
            case NumberLong:
                b.append( n , inclong ); break;
            case NumberInt:
                b.append( n , incint ); break;
            default:
                verify(0);
            }
        }

        string toString() const;

        void handleRename( BSONBuilderBase& newObjBuilder, const char* shortFieldName );
    };

    /**
     * this is used to hold state, meta data while applying a ModSet to a BSONObj
     * the goal is to make ModSet const so its re-usable
     */
    class ModSetState : boost::noncopyable {
        typedef map<string,shared_ptr<ModState>,LexNumCmp> ModStateHolder;
        typedef pair<const ModStateHolder::iterator,const ModStateHolder::iterator> ModStateRange;
        const BSONObj& _obj;
        ModStateHolder _mods;
        BSONObj _newFromMods; // keep this data alive, as oplog generation may depend on it

        ModSetState( const BSONObj& obj )
            : _obj( obj ) , _mods( LexNumCmp( true ) ) {
        }

        ModStateRange modsForRoot( const string& root );

        void createNewObjFromMods( const string& root, BSONObjBuilder& b, const BSONObj& obj );
        void createNewArrayFromMods( const string& root, BSONArrayBuilder& b,
                                    const BSONArray& arr );

        void createNewFromMods( const string& root , BSONBuilderBase& b , BSONIteratorSorted& es ,
                               const ModStateRange& modRange , const LexNumCmp& lexNumCmp );

        void _appendNewFromMods( const string& root , ModState& m , BSONBuilderBase& b , set<string>& onedownseen );

        void appendNewFromMod( ModState& ms , BSONBuilderBase& b ) {
            if ( ms.dontApply ) {
                return;
            }

            //const Mod& m = *(ms.m); // HACK
            Mod& m = *((Mod*)(ms.m)); // HACK

            switch ( m.op ) {

            case Mod::PUSH: {
                ms.fixedOpName = "$set";
                if ( m.isEach() ) {
                    BSONObj arr = m.getEach();
                    b.appendArray( m.shortFieldName, arr );
                    ms.forceEmptyArray = true;
                    ms.fixedArray = BSONArray(arr.getOwned());
                } else {
                    BSONObjBuilder arr( b.subarrayStart( m.shortFieldName ) );
                    arr.appendAs( m.elt, "0" );
                    ms.forceEmptyArray = true;
                    ms.fixedArray = BSONArray(arr.done().getOwned());
                }
                break;
            }

            case Mod::ADDTOSET: {
                ms.fixedOpName = "$set";
                if ( m.isEach() ) {
                    // Remove any duplicates in given array
                    BSONArrayBuilder arr( b.subarrayStart( m.shortFieldName ) );
                    BSONElementSet toadd;
                    m.parseEach( toadd );
                    BSONObjIterator i( m.getEach() );
                    // int n = 0;
                    while ( i.more() ) {
                        BSONElement e = i.next();
                        if ( toadd.count(e) ) {
                            arr.append( e );
                            toadd.erase( e );
                        }
                    }
                    ms.forceEmptyArray = true;
                    ms.fixedArray = BSONArray(arr.done().getOwned());
                }
                else {
                    BSONArrayBuilder arr( b.subarrayStart( m.shortFieldName ) );
                    arr.append( m.elt );
                    ms.forceEmptyArray = true;
                    ms.fixedArray = BSONArray(arr.done().getOwned());
                }
                break;
            }

            case Mod::PUSH_ALL: {
                b.appendAs( m.elt, m.shortFieldName );
                ms.fixedOpName = "$set";
                ms.forceEmptyArray = true;
                ms.fixedArray = BSONArray(m.elt.Obj());
                break;
            }

            case Mod::POP:
            case Mod::PULL:
            case Mod::PULL_ALL:
            case Mod::UNSET:
                // No-op b/c unset/pull of nothing does nothing. Still, explicilty log that
                // the target array was reset.
                ms.fixedOpName = "$unset";
                break;

            case Mod::INC:
            case Mod::SET_ON_INSERT:
                ms.fixedOpName = "$set";
            case Mod::SET: {
                m._checkForAppending( m.elt );
                b.appendAs( m.elt, m.shortFieldName );
                break;
            }

            // shouldn't see RENAME_FROM here
            case Mod::RENAME_TO:
                ms.handleRename( b, m.shortFieldName );
                break;

            default:
                stringstream ss;
                ss << "unknown mod in appendNewFromMod: " << m.op;
                throw UserException( 9015, ss.str() );
            }

        }

        /** @return true iff the elements aren't eoo(), are distinct, and share a field name. */
        static bool duplicateFieldName( const BSONElement& a, const BSONElement& b );

    public:

        BSONObj createNewFromMods();

        // re-writing for oplog

        bool DEPRECATED_needOpLogRewrite() const {
            for ( ModStateHolder::const_iterator i = _mods.begin(); i != _mods.end(); i++ )
                if ( i->second->DEPRECATED_needOpLogRewrite() )
                    return true;
            return false;
        }

        BSONObj getOpLogRewrite() const;

        bool DEPRECATED_haveArrayDepMod() const {
            for ( ModStateHolder::const_iterator i = _mods.begin(); i != _mods.end(); i++ )
                if ( i->second->m->arrayDep() )
                    return true;
            return false;
        }

        void DEPRECATED_appendSizeSpecForArrayDepMods( BSONObjBuilder& b ) const {
            for ( ModStateHolder::const_iterator i = _mods.begin(); i != _mods.end(); i++ ) {
                const ModState& m = *i->second;
                if ( m.m->arrayDep() ) {
                    if ( m.DEPRECATED_pushStartSize == -1 )
                        b.appendNull( m.fieldName() );
                    else
                        b << m.fieldName() << BSON( "$size" << m.DEPRECATED_pushStartSize );
                }
            }
        }

        string toString() const;

        friend class ModSet;
    };

}  // namespace mongo
