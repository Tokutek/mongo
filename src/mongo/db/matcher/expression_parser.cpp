// expression_parser.cpp

/**
 *    Copyright (C) 2013 10gen Inc.
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

#include "mongo/db/matcher/expression_parser.h"

#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/bsonobjiterator.h"
#include "mongo/bson/bson-inl.h"
#include "mongo/db/matcher/expression_array.h"
#include "mongo/db/matcher/expression_leaf.h"
#include "mongo/db/matcher/expression_tree.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    StatusWithMatchExpression MatchExpressionParser::_parseComparison( const char* name,
                                                                       ComparisonMatchExpression* cmp,
                                                                       const BSONElement& e ) {
        std::auto_ptr<ComparisonMatchExpression> temp( cmp );

        Status s = temp->init( name, e );
        if ( !s.isOK() )
            return StatusWithMatchExpression(s);

        return StatusWithMatchExpression( temp.release() );
    }

    StatusWithMatchExpression MatchExpressionParser::_parseSubField( const BSONObj& context,
                                                                     const char* name,
                                                                     const BSONElement& e,
                                                                     int position,
                                                                     bool* stop ) {

        *stop = false;

        // TODO: these should move to getGtLtOp, or its replacement

        if ( mongoutils::str::equals( "$eq", e.fieldName() ) )
            return _parseComparison( name, new EqualityMatchExpression(), e );

        if ( mongoutils::str::equals( "$not", e.fieldName() ) ) {
            return _parseNot( name, e );
        }


        int x = e.getGtLtOp(-1);
        switch ( x ) {
        case -1:
            return StatusWithMatchExpression( ErrorCodes::BadValue,
                                         mongoutils::str::stream() << "unknown operator: "
                                         << e.fieldName() );
        case BSONObj::LT:
            return _parseComparison( name, new LTMatchExpression(), e );
        case BSONObj::LTE:
            return _parseComparison( name, new LTEMatchExpression(), e );
        case BSONObj::GT:
            return _parseComparison( name, new GTMatchExpression(), e );
        case BSONObj::GTE:
            return _parseComparison( name, new GTEMatchExpression(), e );
        case BSONObj::NE:
            return _parseComparison( name, new NEMatchExpression(), e );
        case BSONObj::Equality:
            return _parseComparison( name, new EqualityMatchExpression(), e );

        case BSONObj::opIN: {
            if ( e.type() != Array )
                return StatusWithMatchExpression( ErrorCodes::BadValue, "$in needs an array" );
            std::auto_ptr<InMatchExpression> temp( new InMatchExpression() );
            temp->init( name );
            Status s = _parseArrayFilterEntries( temp->getArrayFilterEntries(), e.Obj() );
            if ( !s.isOK() )
                return StatusWithMatchExpression( s );
            return StatusWithMatchExpression( temp.release() );
        }

        case BSONObj::NIN: {
            if ( e.type() != Array )
                return StatusWithMatchExpression( ErrorCodes::BadValue, "$nin needs an array" );
            std::auto_ptr<NinMatchExpression> temp( new NinMatchExpression() );
            temp->init( name );
            Status s = _parseArrayFilterEntries( temp->getArrayFilterEntries(), e.Obj() );
            if ( !s.isOK() )
                return StatusWithMatchExpression( s );
            return StatusWithMatchExpression( temp.release() );
        }

        case BSONObj::opSIZE: {
            int size = 0;
            if ( e.type() == String ) {
                // matching old odd semantics
                size = 0;
            }
            else if ( e.type() == NumberInt || e.type() == NumberLong ) {
                size = e.numberInt();
            }
            else if ( e.type() == NumberDouble ) {
                if ( e.numberInt() == e.numberDouble() ) {
                    size = e.numberInt();
                }
                else {
                    // old semantcs require exact numeric match
                    // so [1,2] != 1 or 2
                    size = -1;
                }
            }
            else {
                return StatusWithMatchExpression( ErrorCodes::BadValue, "$size needs a number" );
            }

            std::auto_ptr<SizeMatchExpression> temp( new SizeMatchExpression() );
            Status s = temp->init( name, size );
            if ( !s.isOK() )
                return StatusWithMatchExpression( s );
            return StatusWithMatchExpression( temp.release() );
        }

        case BSONObj::opEXISTS: {
            if ( e.eoo() )
                return StatusWithMatchExpression( ErrorCodes::BadValue, "$exists can't be eoo" );
            std::auto_ptr<ExistsMatchExpression> temp( new ExistsMatchExpression() );
            Status s = temp->init( name, e.trueValue() );
            if ( !s.isOK() )
                return StatusWithMatchExpression( s );
            return StatusWithMatchExpression( temp.release() );
        }

        case BSONObj::opTYPE: {
            if ( !e.isNumber() )
                return StatusWithMatchExpression( ErrorCodes::BadValue, "$type has to be a number" );
            int type = e.numberInt();
            if ( e.type() != NumberInt && type != e.number() )
                type = -1;
            std::auto_ptr<TypeMatchExpression> temp( new TypeMatchExpression() );
            Status s = temp->init( name, type );
            if ( !s.isOK() )
                return StatusWithMatchExpression( s );
            return StatusWithMatchExpression( temp.release() );
        }


        case BSONObj::opMOD:
            return _parseMOD( name, e );

        case BSONObj::opOPTIONS:
            return StatusWithMatchExpression( ErrorCodes::BadValue, "$options has to be after a $regex" );

        case BSONObj::opREGEX: {
            if ( position != 0 )
                return StatusWithMatchExpression( ErrorCodes::BadValue, "$regex has to be first" );

            *stop = true;
            return _parseRegexDocument( name, context );
        }

        case BSONObj::opELEM_MATCH:
            return _parseElemMatch( name, e );

        case BSONObj::opALL:
            return _parseAll( name, e );

        case BSONObj::opWITHIN:
            return expressionParserGeoCallback( name, context );

        default:
            return StatusWithMatchExpression( ErrorCodes::BadValue, "not done" );
        }

    }

    StatusWithMatchExpression MatchExpressionParser::_parse( const BSONObj& obj, bool topLevel ) {

        std::auto_ptr<AndMatchExpression> root( new AndMatchExpression() );

        BSONObjIterator i( obj );
        while ( i.more() ){

            BSONElement e = i.next();
            if ( e.fieldName()[0] == '$' ) {
                const char * rest = e.fieldName() + 1;

                // TODO: optimize if block?
                if ( mongoutils::str::equals( "or", rest ) ) {
                    if ( e.type() != Array )
                        return StatusWithMatchExpression( ErrorCodes::BadValue,
                                                     "$or needs an array" );
                    std::auto_ptr<OrMatchExpression> temp( new OrMatchExpression() );
                    Status s = _parseTreeList( e.Obj(), temp.get() );
                    if ( !s.isOK() )
                        return StatusWithMatchExpression( s );
                    root->add( temp.release() );
                }
                else if ( mongoutils::str::equals( "and", rest ) ) {
                    if ( e.type() != Array )
                        return StatusWithMatchExpression( ErrorCodes::BadValue,
                                                     "and needs an array" );
                    std::auto_ptr<AndMatchExpression> temp( new AndMatchExpression() );
                    Status s = _parseTreeList( e.Obj(), temp.get() );
                    if ( !s.isOK() )
                        return StatusWithMatchExpression( s );
                    root->add( temp.release() );
                }
                else if ( mongoutils::str::equals( "nor", rest ) ) {
                    if ( e.type() != Array )
                        return StatusWithMatchExpression( ErrorCodes::BadValue,
                                                     "and needs an array" );
                    std::auto_ptr<NorMatchExpression> temp( new NorMatchExpression() );
                    Status s = _parseTreeList( e.Obj(), temp.get() );
                    if ( !s.isOK() )
                        return StatusWithMatchExpression( s );
                    root->add( temp.release() );
                }
                else if ( mongoutils::str::equals( "atomic", rest ) ) {
                    if ( !topLevel )
                        return StatusWithMatchExpression( ErrorCodes::BadValue,
                                                          "$atomic has to be at the top level" );
                    if ( e.trueValue() )
                        root->add( new AtomicMatchExpression() );
                }
                else if ( mongoutils::str::equals( "where", rest ) ) {
                    if ( !topLevel )
                        return StatusWithMatchExpression( ErrorCodes::BadValue,
                                                          "$within has to be at the top level" );
                    StatusWithMatchExpression s = expressionParserWhereCallback( e );
                    if ( !s.isOK() )
                        return s;
                    root->add( s.getValue() );
                }
                else {
                    return StatusWithMatchExpression( ErrorCodes::BadValue,
                                                 mongoutils::str::stream()
                                                 << "unknown top level operator: "
                                                 << e.fieldName() );
                }

                continue;
            }

            if ( e.type() == Object && e.Obj().firstElement().fieldName()[0] == '$' ) {
                Status s = _parseSub( e.fieldName(), e.Obj(), root.get() );
                if ( !s.isOK() )
                    return StatusWithMatchExpression( s );
                continue;
            }

            if ( e.type() == RegEx ) {
                StatusWithMatchExpression result = _parseRegexElement( e.fieldName(), e );
                if ( !result.isOK() )
                    return result;
                root->add( result.getValue() );
                continue;
            }

            std::auto_ptr<ComparisonMatchExpression> eq( new EqualityMatchExpression() );
            Status s = eq->init( e.fieldName(), e );
            if ( !s.isOK() )
                return StatusWithMatchExpression( s );

            root->add( eq.release() );
        }

        if ( root->numChildren() == 1 ) {
            const MatchExpression* real = root->getChild(0);
            root->clearAndRelease();
            return StatusWithMatchExpression( const_cast<MatchExpression*>(real) );
        }

        return StatusWithMatchExpression( root.release() );
    }

    Status MatchExpressionParser::_parseSub( const char* name,
                                        const BSONObj& sub,
                                        AndMatchExpression* root ) {

        int position = 0;

        BSONObjIterator j( sub );
        while ( j.more() ) {
            BSONElement deep = j.next();

            bool stop = false;
            StatusWithMatchExpression s = _parseSubField( sub, name, deep, position, &stop );
            if ( !s.isOK() )
                return s.getStatus();

            root->add( s.getValue() );

            if ( stop )
                break;
            position++;
        }

        return Status::OK();
    }



    StatusWithMatchExpression MatchExpressionParser::_parseMOD( const char* name,
                                                      const BSONElement& e ) {

        if ( e.type() != Array )
            return StatusWithMatchExpression( ErrorCodes::BadValue, "malformed mod, needs to be an array" );

        BSONObjIterator i( e.Obj() );

        if ( !i.more() )
            return StatusWithMatchExpression( ErrorCodes::BadValue, "malformed mod, not enough elements" );
        BSONElement d = i.next();
        if ( !d.isNumber() )
            return StatusWithMatchExpression( ErrorCodes::BadValue, "malformed mod, divisor not a number" );

        if ( !i.more() )
            return StatusWithMatchExpression( ErrorCodes::BadValue, "malformed mod, not enough elements" );
        BSONElement r = i.next();
        if ( !d.isNumber() )
            return StatusWithMatchExpression( ErrorCodes::BadValue, "malformed mod, remainder not a number" );

        if ( i.more() )
            return StatusWithMatchExpression( ErrorCodes::BadValue, "malformed mod, too many elements" );

        std::auto_ptr<ModMatchExpression> temp( new ModMatchExpression() );
        Status s = temp->init( name, d.numberInt(), r.numberInt() );
        if ( !s.isOK() )
            return StatusWithMatchExpression( s );
        return StatusWithMatchExpression( temp.release() );
    }

    StatusWithMatchExpression MatchExpressionParser::_parseRegexElement( const char* name,
                                                               const BSONElement& e ) {
        if ( e.type() != RegEx )
            return StatusWithMatchExpression( ErrorCodes::BadValue, "not a regex" );

        std::auto_ptr<RegexMatchExpression> temp( new RegexMatchExpression() );
        Status s = temp->init( name, e.regex(), e.regexFlags() );
        if ( !s.isOK() )
            return StatusWithMatchExpression( s );
        return StatusWithMatchExpression( temp.release() );
    }

    StatusWithMatchExpression MatchExpressionParser::_parseRegexDocument( const char* name,
                                                                const BSONObj& doc ) {
        string regex;
        string regexOptions;

        BSONObjIterator i( doc );
        while ( i.more() ) {
            BSONElement e = i.next();
            switch ( e.getGtLtOp() ) {
            case BSONObj::opREGEX:
                if ( e.type() != String )
                    return StatusWithMatchExpression( ErrorCodes::BadValue,
                                                 "$regex has to be a string" );
                regex = e.String();
                break;
            case BSONObj::opOPTIONS:
                if ( e.type() != String )
                    return StatusWithMatchExpression( ErrorCodes::BadValue,
                                                 "$options has to be a string" );
                regexOptions = e.String();
                break;
            default:
                return StatusWithMatchExpression( ErrorCodes::BadValue,
                                             mongoutils::str::stream()
                                             << "bad $regex doc option: " << e.fieldName() );
            }

        }

        std::auto_ptr<RegexMatchExpression> temp( new RegexMatchExpression() );
        Status s = temp->init( name, regex, regexOptions );
        if ( !s.isOK() )
            return StatusWithMatchExpression( s );
        return StatusWithMatchExpression( temp.release() );

    }

    Status MatchExpressionParser::_parseArrayFilterEntries( ArrayFilterEntries* entries,
                                                       const BSONObj& theArray ) {


        BSONObjIterator i( theArray );
        while ( i.more() ) {
            BSONElement e = i.next();

            if ( e.type() == RegEx ) {
                std::auto_ptr<RegexMatchExpression> r( new RegexMatchExpression() );
                Status s = r->init( "", e );
                if ( !s.isOK() )
                    return s;
                s =  entries->addRegex( r.release() );
                if ( !s.isOK() )
                    return s;
            }
            else {
                Status s = entries->addEquality( e );
                if ( !s.isOK() )
                    return s;
            }
        }
        return Status::OK();

    }

    StatusWithMatchExpression MatchExpressionParser::_parseElemMatch( const char* name,
                                                            const BSONElement& e ) {
        if ( e.type() != Object )
            return StatusWithMatchExpression( ErrorCodes::BadValue, "$elemMatch needs an Object" );

        BSONObj obj = e.Obj();
        if ( obj.firstElement().fieldName()[0] == '$' ) {
            // value case

            AndMatchExpression theAnd;
            Status s = _parseSub( "", obj, &theAnd );
            if ( !s.isOK() )
                return StatusWithMatchExpression( s );

            std::auto_ptr<ElemMatchValueMatchExpression> temp( new ElemMatchValueMatchExpression() );
            s = temp->init( name );
            if ( !s.isOK() )
                return StatusWithMatchExpression( s );

            for ( size_t i = 0; i < theAnd.numChildren(); i++ ) {
                temp->add( theAnd.getChild( i ) );
            }
            theAnd.clearAndRelease();

            return StatusWithMatchExpression( temp.release() );
        }

        // object case

        StatusWithMatchExpression sub = _parse( obj, false );
        if ( !sub.isOK() )
            return sub;

        std::auto_ptr<ElemMatchObjectMatchExpression> temp( new ElemMatchObjectMatchExpression() );
        Status status = temp->init( name, sub.getValue() );
        if ( !status.isOK() )
            return StatusWithMatchExpression( status );

        return StatusWithMatchExpression( temp.release() );
    }

    StatusWithMatchExpression MatchExpressionParser::_parseAll( const char* name,
                                                      const BSONElement& e ) {
        if ( e.type() != Array )
            return StatusWithMatchExpression( ErrorCodes::BadValue, "$all needs an array" );

        BSONObj arr = e.Obj();
        if ( arr.firstElement().type() == Object &&
             mongoutils::str::equals( "$elemMatch",
                                      arr.firstElement().Obj().firstElement().fieldName() ) ) {
            // $all : [ { $elemMatch : {} } ... ]

            std::auto_ptr<AllElemMatchOp> temp( new AllElemMatchOp() );
            Status s = temp->init( name );
            if ( !s.isOK() )
                return StatusWithMatchExpression( s );

            BSONObjIterator i( arr );
            while ( i.more() ) {
                BSONElement hopefullyElemMatchElemennt = i.next();

                if ( hopefullyElemMatchElemennt.type() != Object ) {
                    // $all : [ { $elemMatch : ... }, 5 ]
                    return StatusWithMatchExpression( ErrorCodes::BadValue,
                                                 "$all/$elemMatch has to be consistent" );
                }

                BSONObj hopefullyElemMatchObj = hopefullyElemMatchElemennt.Obj();
                if ( !mongoutils::str::equals( "$elemMatch",
                                               hopefullyElemMatchObj.firstElement().fieldName() ) ) {
                    // $all : [ { $elemMatch : ... }, { x : 5 } ]
                    return StatusWithMatchExpression( ErrorCodes::BadValue,
                                                 "$all/$elemMatch has to be consistent" );
                }

                StatusWithMatchExpression inner = _parseElemMatch( "", hopefullyElemMatchObj.firstElement() );
                if ( !inner.isOK() )
                    return inner;
                temp->add( static_cast<ArrayMatchingMatchExpression*>( inner.getValue() ) );
            }

            return StatusWithMatchExpression( temp.release() );
        }

        std::auto_ptr<AllMatchExpression> temp( new AllMatchExpression() );
        Status s = temp->init( name );
        if ( !s.isOK() )
            return StatusWithMatchExpression( s );

        s = _parseArrayFilterEntries( temp->getArrayFilterEntries(), arr );
        if ( !s.isOK() )
            return StatusWithMatchExpression( s );

        return StatusWithMatchExpression( temp.release() );
    }

    StatusWithMatchExpression expressionParserGeoCallbackDefault( const char* name,
                                                                  const BSONObj& section ) {
        return StatusWithMatchExpression( ErrorCodes::BadValue, "geo not linked in" );
    }

    MatchExpressionParserGeoCallback expressionParserGeoCallback = expressionParserGeoCallbackDefault;

    StatusWithMatchExpression expressionParserWhereCallbackDefault(const BSONElement& where) {
        return StatusWithMatchExpression( ErrorCodes::BadValue, "$where not linked in" );
    }

    MatchExpressionParserWhereCallback expressionParserWhereCallback = expressionParserWhereCallbackDefault;


}
