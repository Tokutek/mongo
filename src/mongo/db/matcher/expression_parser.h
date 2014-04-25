// expression_parser.h

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

#pragma once

#include <boost/function.hpp>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/db/matcher/expression_leaf.h"
#include "mongo/db/matcher/expression_tree.h"

namespace mongo {

    typedef StatusWith<MatchExpression*> StatusWithMatchExpression;

    class MatchExpressionParser {
    public:

        /**
         * caller has to maintain ownership obj
         * the tree has views (BSONElement) into obj
         */
        static StatusWithMatchExpression parse( const BSONObj& obj ) {
            // The 0 initializes the match expression tree depth.
            return _parse( obj, 0 );
        }

    private:

        /**
         * 5 = false
         * { a : 5 } = false
         * { $lt : 5 } = true
         * { $ref: "s", $id: "x" } = false
         * { $ref: "s", $id: "x", $db: "mydb" } = false
         * { $ref : "s" } = false (if incomplete DBRef is allowed)
         * { $id : "x" } = false (if incomplete DBRef is allowed)
         * { $db : "mydb" } = false (if incomplete DBRef is allowed)
         */
        static bool _isExpressionDocument( const BSONElement& e, bool allowIncompleteDBRef );

        /**
         * { $ref: "s", $id: "x" } = true
         * { $ref : "s" } = true (if incomplete DBRef is allowed)
         * { $id : "x" } = true (if incomplete DBRef is allowed)
         * { $db : "x" } = true (if incomplete DBRef is allowed)
         */
        static bool _isDBRefDocument( const BSONObj& obj, bool allowIncompleteDBRef );

        /**
         * Parse 'obj' and return either a MatchExpression or an error.
         *
         * 'level' tracks the current depth of the tree across recursive calls to this
         * function. Used in order to apply special logic at the top-level and to return an
         * error if the tree exceeds the maximum allowed depth.
         */
        static StatusWithMatchExpression _parse( const BSONObj& obj, int level );

        /**
         * parses a field in a sub expression
         * if the query is { x : { $gt : 5, $lt : 8 } }
         * e is { $gt : 5, $lt : 8 }
         */
        static Status _parseSub( const char* name,
                                 const BSONObj& obj,
                                 AndMatchExpression* root,
                                 int level );

        /**
         * parses a single field in a sub expression
         * if the query is { x : { $gt : 5, $lt : 8 } }
         * e is $gt : 5
         */
        static StatusWithMatchExpression _parseSubField( const BSONObj& context,
                                                         const AndMatchExpression* andSoFar,
                                                         const char* name,
                                                         const BSONElement& e,
                                                         int level );

        static StatusWithMatchExpression _parseComparison( const char* name,
                                                           ComparisonMatchExpression* cmp,
                                                           const BSONElement& e );

        static StatusWithMatchExpression _parseMOD( const char* name,
                                               const BSONElement& e );

        static StatusWithMatchExpression _parseRegexElement( const char* name,
                                                        const BSONElement& e );

        static StatusWithMatchExpression _parseRegexDocument( const char* name,
                                                         const BSONObj& doc );


        static Status _parseArrayFilterEntries( ArrayFilterEntries* entries,
                                                const BSONObj& theArray );

        // arrays

        static StatusWithMatchExpression _parseElemMatch( const char* name,
                                                          const BSONElement& e,
                                                          int level );

        static StatusWithMatchExpression _parseAll( const char* name,
                                                    const BSONElement& e,
                                                    int level );

        // tree

        static Status _parseTreeList( const BSONObj& arr, ListOfMatchExpression* out, int level );

        static StatusWithMatchExpression _parseNot( const char* name,
                                                    const BSONElement& e,
                                                    int level );

        // The maximum allowed depth of a query tree. Just to guard against stack overflow.
        static const int kMaximumTreeDepth;
    };

    typedef boost::function<StatusWithMatchExpression(const char* name, const BSONObj& section)> MatchExpressionParserGeoCallback;
    extern MatchExpressionParserGeoCallback expressionParserGeoCallback;

    typedef boost::function<StatusWithMatchExpression(const BSONElement& where)> MatchExpressionParserWhereCallback;
    extern MatchExpressionParserWhereCallback expressionParserWhereCallback;

}
