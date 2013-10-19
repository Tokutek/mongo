// matcher.h

/* Matcher is our boolean expression evaluator for "where" clauses */

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

#pragma once

<<<<<<< HEAD
#include "jsobj.h"
#include "pcrecpp.h"
#include "geo/shapes.h"
=======
#include "mongo/db/diskloc.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/geo/geoquery.h"
>>>>>>> 38a311a... SERVER-6400: prep to have both MatcherOld and Matcher2 in the code base
#include "mongo/db/matcher/match_details.h"

namespace mongo {

    class Cursor;
    class CoveredIndexMatcher;
<<<<<<< HEAD
    class ElementMatcher;
    class Matcher;
    class FieldRangeVector;

    class RegexMatcher {
    public:
        /**
         * Maximum pattern size which pcre v8.3 can do matches correctly with
         * LINK_SIZE define macro set to 2 @ pcre's config.h (based on
         * experiments)
         */
        static const size_t MaxPatternSize = 32764;

        const char *_fieldName;
        const char *_regex;
        const char *_flags;
        string _prefix;
        shared_ptr< pcrecpp::RE > _re;
        bool _isNot;
        RegexMatcher() : _isNot() {}
    };

    class GeoMatcher {
    private:
        GeoMatcher(const string& field) : _isBox(false), _isCircle(false), _isPolygon(false),
                                          _fieldName(field) {}
        bool _isBox;
        Box _box;

        bool _isCircle;
        Point _center;
        double _radius;

        bool _isPolygon;
        Polygon _polygon;

        string _fieldName;
    public:
        const string& getFieldName() const { return _fieldName; }

        static GeoMatcher makeBox(const string& field, const BSONObj &min, const BSONObj &max) {
            GeoMatcher m(field);
            m._isBox = true;
            uassert(16511, "Malformed coord: " + min.toString(), pointFrom(min, &m._box._min));
            uassert(16512, "Malformed coord: " + max.toString(), pointFrom(max, &m._box._max));
            return m;
        }

        static GeoMatcher makeCircle(const string& field, const BSONObj &center, double rad) {
            GeoMatcher m(field);
            m._isCircle = true;
            uassert(16513, "Malformed coord: " + center.toString(), pointFrom(center, &m._center));
            m._radius = rad;
            return m;
        }

        static GeoMatcher makePolygon(const string& field, const BSONObj &poly) {
            GeoMatcher m(field);
            vector<Point> points;

            m._isPolygon = true;
            BSONObjIterator coordIt(poly);
            while (coordIt.more()) {
                BSONElement coord = coordIt.next();
                BSONObj obj = coord.Obj();
                Point p;
                uassert(16514, "Malformed coord: " + obj.toString(), pointFrom(obj, &p));
                points.push_back(p);
            }
            m._polygon = Polygon(points);
            return m;
        }

        bool containsPoint(Point p) const {
            if (_isBox) {
                return _box.inside(p, 0);
            } else if (_isCircle) {
                return p.distance(_center) <= _radius;
            } else if (_isPolygon) {
                return _polygon.contains(p);
            } else {
                return false;
            }
        }

        string toString() const {
            stringstream ss;
            if (_isBox) {
                ss << "GeoMatcher Box: " << _box.toString();
            } else if (_isCircle) {
                ss << "GeoMatcher Circle @ " << _center.toString() << " r = " << _radius;
            } else {
                ss << "GeoMatcher UNKNOWN";
            }
            return ss.str();
        }

        static bool pointFrom(const BSONObj o, Point *p) {
            BSONObjIterator i(o);
            if (!i.more()) { return false; }
            BSONElement xe = i.next();
            if (!i.more()) { return false; }
            BSONElement ye = i.next();
            if (!xe.isNumber() || !ye.isNumber()) { return false; }
            p->_x = xe.number();
            p->_y = ye.number();
            return true;
        }
    };

=======
    class FieldRangeVector;

>>>>>>> 38a311a... SERVER-6400: prep to have both MatcherOld and Matcher2 in the code base
    struct element_lt {
        bool operator()(const BSONElement& l, const BSONElement& r) const {
            int x = (int) l.canonicalType() - (int) r.canonicalType();
            if ( x < 0 ) return true;
            else if ( x > 0 ) return false;
            return compareElementValues(l,r) < 0;
        }
    };

<<<<<<< HEAD
    /**
     * An interface for visiting a Matcher and all of its nested Matchers and ElementMatchers.
     * RegexMatchers are not visited.
     */
    class MatcherVisitor {
    public:
        virtual ~MatcherVisitor() {}
        virtual void visitMatcher( const Matcher& matcher ) {}
        virtual void visitElementMatcher( const ElementMatcher& elementMatcher ) {}
    };
=======
#include "matcher_old.h"
#include "mongo/db/matcher/matcher.h"
>>>>>>> 38a311a... SERVER-6400: prep to have both MatcherOld and Matcher2 in the code base

    class ElementMatcher {
    public:

        ElementMatcher() {
        }

        ElementMatcher( BSONElement e , int op, bool isNot );

        ElementMatcher( BSONElement e , int op , const BSONObj& array, bool isNot );

        ~ElementMatcher() { }
        
        bool negativeCompareOp() const { return _compareOp == BSONObj::NE || _compareOp == BSONObj::NIN; }
        int inverseOfNegativeCompareOp() const;
        bool negativeCompareOpContainsNull() const;
        
        void visit( MatcherVisitor& visitor ) const;
        
        BSONElement _toMatch;
        int _compareOp;
        bool _isNot;
        shared_ptr< set<BSONElement,element_lt> > _myset;
        shared_ptr< vector<RegexMatcher> > _myregex;

        // these are for specific operators
        int _mod;
        int _modm;
        BSONType _type;

        shared_ptr<Matcher> _subMatcher;
        bool _subMatcherOnPrimitives ;

        vector< shared_ptr<Matcher> > _allMatchers;
    };

    class Where; // used for $where javascript eval


    /* Match BSON objects against a query pattern.

       e.g.
           db.foo.find( { a : 3 } );

<<<<<<< HEAD
       { a : 3 } is the pattern object.  See manual for full info.

       GT/LT:
         { a : { $gt : 3 } }
       Not equal:
         { a : { $ne : 3 } }

       TODO: we should rewrite the matcher to be more an AST style.
    */
    class Matcher : boost::noncopyable {
        int matchesDotted(
            const char *fieldName,
            const BSONElement& toMatch, const BSONObj& obj,
            int compareOp, const ElementMatcher& bm, bool isArr , MatchDetails * details ) const;

        /**
         * Perform a NE or NIN match by returning the inverse of the opposite matching operation.
         * Missing values are considered matches unless the match must not equal null.
         */
        int inverseMatch(
            const char *fieldName,
            const BSONElement &toMatch, const BSONObj &obj,
            const ElementMatcher&bm, MatchDetails * details ) const;

    public:
        static int opDirection(int op) {
            return op <= BSONObj::LTE ? -1 : 1;
        }

        Matcher(const BSONObj &pattern, bool nested=false);

        ~Matcher();

        bool matches(const BSONObj& j, MatchDetails * details = 0 ) const;

#ifdef MONGO_LATER_SERVER_4644
        class FieldSink {
        public:
            virtual ~FieldSink() {};
            virtual void referenceField(const string &fieldPath) = 0;
        };

        /**
           Visit all of the fields that are referenced by this Matcher
           (and any descendants).

           This can be used to gather a list of all the references made by
           this matcher.  The implementation of this parallels that of
           matches() above.

           @param pSink a FieldSink that the caller will use to gather or
             process the references
         */
        void visitReferences(FieldSink *pSink) const;
#endif /* MONGO_LATER_SERVER_4644 */

        /**
         * Visit this Matcher and all of its nested Matchers and ElementMatchers.  All top level
         * ElementMatchers of a Matcher are visited immediately after the Matcher itself (before any
         * other Matcher is visited).
         */
        void visit( MatcherVisitor& visitor ) const;
        
        string toString() const {
            return _jsobj.toString();
        }

        /**
         * @return true if this key matcher will return the same true/false
         * value as the provided doc matcher.
         */
        bool keyMatch( const Matcher &docMatcher ) const;
        
        bool singleSimpleCriterion() const {
            return false; // TODO SERVER-958
//            // TODO Really check, especially if all basics are ok.
//            // $all, etc
//            // _orConstraints?
//            return ( ( basics.size() + nRegex ) < 2 ) && !where && !_orMatchers.size() && !_norMatchers.size();
        }

        const BSONObj *getQuery() const { return &_jsobj; };

        /**
         * Generate a matcher for the provided index key format using the
         * provided full doc matcher.
         */
        Matcher( const Matcher &docMatcher, const BSONObj &constrainIndexKey );

    private:

        void addBasic(const BSONElement &e, int c, bool isNot) {
            // TODO May want to selectively ignore these element types based on op type.
            if ( e.type() == MinKey || e.type() == MaxKey )
                return;
            _basics.push_back( ElementMatcher( e , c, isNot ) );
        }

        void addRegex(const char *fieldName, const char *regex, const char *flags, bool isNot = false);
        bool addOp( const BSONElement &e, const BSONElement &fe, bool isNot, const char *& regex, const char *&flags );

        int valuesMatch(const BSONElement& l, const BSONElement& r, int op, const ElementMatcher& bm) const;

        bool parseClause( const BSONElement &e );
        void parseExtractedClause( const BSONElement &e, list< shared_ptr< Matcher > > &matchers );

        void parseWhere( const BSONElement &e );
        void parseMatchExpressionElement( const BSONElement &e, bool nested );

        Where *_where;                    // set if query uses $where
        BSONObj _jsobj;                  // the query pattern.  e.g., { name: "joe" }
        BSONObj _constrainIndexKey;
        vector<ElementMatcher> _basics;
        bool _haveSize;
        bool _all;
        bool _hasArray;
        bool _haveNeg;

        vector<RegexMatcher> _regexs;
        vector<GeoMatcher> _geo;

        // so we delete the mem when we're done:
        vector< shared_ptr< BSONObjBuilder > > _builders;
        list< shared_ptr< Matcher > > _andMatchers;
        list< shared_ptr< Matcher > > _orMatchers;
        list< shared_ptr< Matcher > > _norMatchers;

        friend class CoveredIndexMatcher;
    };
=======
    typedef MatcherOld Matcher;
    //typedef Matcher2 Matcher;
>>>>>>> 38a311a... SERVER-6400: prep to have both MatcherOld and Matcher2 in the code base

    // If match succeeds on index key, then attempt to match full document.
    class CoveredIndexMatcher : boost::noncopyable {
    public:
        CoveredIndexMatcher(const BSONObj &pattern, const BSONObj &indexKeyPattern);
        /**
         * This is the preferred method for matching against a cursor, as it
         * can handle both multi and single key cursors.
         */
        bool matchesCurrent( Cursor * cursor , MatchDetails * details = 0 ) const;
        bool needRecord() const { return _needRecord; }

        const Matcher &docMatcher() const { return *_docMatcher; }

        /**
         * @return a matcher for a following $or clause.
         * @param prevClauseFrs The index range scanned by the previous $or clause.  May be empty.
         * @param nextClauseIndexKeyPattern The index key of the following $or clause.
         */
        CoveredIndexMatcher *nextClauseMatcher( const shared_ptr<FieldRangeVector>& prevClauseFrv,
                                                const BSONObj& nextClauseIndexKeyPattern ) const {
            return new CoveredIndexMatcher( *this, prevClauseFrv, nextClauseIndexKeyPattern );
        }

        string toString() const;

    private:
        bool isOrClauseDup( const BSONObj &obj ) const;
        CoveredIndexMatcher( const CoveredIndexMatcher &prevClauseMatcher,
                            const shared_ptr<FieldRangeVector> &prevClauseFrv,
                            const BSONObj &nextClauseIndexKeyPattern );
        void init();
        shared_ptr< Matcher > _docMatcher;
        Matcher _keyMatcher;
        vector<shared_ptr<FieldRangeVector> > _orDedupConstraints;

        bool _needRecord; // if the key itself isn't good enough to determine a positive match
    };

} // namespace mongo
