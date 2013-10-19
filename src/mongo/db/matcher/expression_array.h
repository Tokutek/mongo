// expression_array.h

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

#include <vector>

#include "mongo/base/status.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/db/matcher/expression_leaf.h"

namespace mongo {

    /**
     * ALL and ELEM_MATCH inherit from this.
     */
    class ArrayMatchingMatchExpression : public MatchExpression {
    public:
        ArrayMatchingMatchExpression( MatchType matchType ) : MatchExpression( matchType ){}
        virtual ~ArrayMatchingMatchExpression(){}

        Status initPath( const StringData& path );

        virtual bool matches( const MatchableDocument* doc, MatchDetails* details ) const;

        /**
         * @param e - has to be an array.  calls matchesArray with e as an array
         */
        virtual bool matchesSingleElement( const BSONElement& e ) const;

        virtual bool matchesArray( const BSONObj& anArray, MatchDetails* details ) const = 0;

        bool equivalent( const MatchExpression* other ) const;

        const StringData path() const { return _path; }
    private:
        StringData _path;
        ElementPath _elementPath;
    };

    class ElemMatchObjectMatchExpression : public ArrayMatchingMatchExpression {
    public:
        ElemMatchObjectMatchExpression() : ArrayMatchingMatchExpression( ELEM_MATCH_OBJECT ){}
        Status init( const StringData& path, const MatchExpression* sub );

        bool matchesArray( const BSONObj& anArray, MatchDetails* details ) const;

        virtual ElemMatchObjectMatchExpression* shallowClone() const {
            ElemMatchObjectMatchExpression* e = new ElemMatchObjectMatchExpression();
            e->init(path(), _sub->shallowClone());
            return e;
        }

        virtual void debugString( StringBuilder& debug, int level ) const;

        virtual size_t numChildren() const { return 1; }
        virtual const MatchExpression* getChild( size_t i ) const { return _sub.get(); }

    private:
        boost::scoped_ptr<const MatchExpression> _sub;
    };

    class ElemMatchValueMatchExpression : public ArrayMatchingMatchExpression {
    public:
        ElemMatchValueMatchExpression() : ArrayMatchingMatchExpression( ELEM_MATCH_VALUE ){}
        virtual ~ElemMatchValueMatchExpression();

        Status init( const StringData& path );
        Status init( const StringData& path, const MatchExpression* sub );
        void add( const MatchExpression* sub );

        bool matchesArray( const BSONObj& anArray, MatchDetails* details ) const;

        virtual ElemMatchValueMatchExpression* shallowClone() const {
            ElemMatchValueMatchExpression* e = new ElemMatchValueMatchExpression();
            e->init(path());
            for (size_t i = 0; i < _subs.size(); ++i) {
                e->add(_subs[i]->shallowClone());
            }
            return e;
        }

        virtual void debugString( StringBuilder& debug, int level ) const;

        virtual size_t numChildren() const { return _subs.size(); }
        virtual const MatchExpression* getChild( size_t i ) const { return _subs[i]; }

    private:
        bool _arrayElementMatchesAll( const BSONElement& e ) const;

        std::vector< const MatchExpression* > _subs;
    };

    class SizeMatchExpression : public ArrayMatchingMatchExpression {
    public:
        SizeMatchExpression() : ArrayMatchingMatchExpression( SIZE ){}
        Status init( const StringData& path, int size );

        virtual SizeMatchExpression* shallowClone() const {
            SizeMatchExpression* e = new SizeMatchExpression();
            e->init(path(), _size);
            return e;
        }

        virtual bool matchesArray( const BSONObj& anArray, MatchDetails* details ) const;

        virtual void debugString( StringBuilder& debug, int level ) const;

        virtual bool equivalent( const MatchExpression* other ) const;

        int getData() const { return _size; }

    private:
        int _size; // >= 0 real, < 0, nothing will match
    };

    /**
     * i'm suprised this isn't a regular AllMatchExpression
     */
    class AllElemMatchOp : public MatchExpression {
    public:
        AllElemMatchOp() : MatchExpression( ALL ){}
        virtual ~AllElemMatchOp();

        Status init( const StringData& path );
        void add( const ArrayMatchingMatchExpression* expr );

        virtual MatchExpression* shallowClone() const {
            AllElemMatchOp* e = new AllElemMatchOp();
            e->init(path());
            for (size_t i = 0; i < _list.size(); ++i) {
                e->add(reinterpret_cast<const ArrayMatchingMatchExpression*>(
                    _list[i]->shallowClone()));
            }
            return e;
        }

        virtual bool matches( const MatchableDocument* doc, MatchDetails* details ) const;

        /**
         * @param e has to be an array
         */
        virtual bool matchesSingleElement( const BSONElement& e ) const;

        virtual void debugString( StringBuilder& debug, int level ) const;

        virtual bool equivalent( const MatchExpression* other ) const;

<<<<<<< HEAD
=======
        virtual size_t numChildren() const { return _list.size(); }
        virtual const ArrayMatchingMatchExpression* getChild( size_t i ) const { return _list[i]; }

        const StringData path() const { return _path; }

>>>>>>> 399c4e2... SERVER-10026 SERVER-10471 begin more sustainable planning approach
    private:
        bool _allMatch( const BSONObj& anArray ) const;

        StringData _path;
        ElementPath _elementPath;
        std::vector< const ArrayMatchingMatchExpression* > _list;
    };

<<<<<<< HEAD
    class SizeMatchExpression : public ArrayMatchingMatchExpression {
    public:
        SizeMatchExpression() : ArrayMatchingMatchExpression( SIZE ){}
        Status init( const StringData& path, int size );

        virtual bool matchesArray( const BSONObj& anArray, MatchDetails* details ) const;

        virtual void debugString( StringBuilder& debug, int level ) const;

        virtual bool equivalent( const MatchExpression* other ) const;

    private:
        int _size; // >= 0 real, < 0, nothing will match
    };

=======
>>>>>>> 399c4e2... SERVER-10026 SERVER-10471 begin more sustainable planning approach
}
