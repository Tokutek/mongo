// matcher_covered.cpp

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

#include "pch.h"

#include "mongo/db/matcher.h"

#include "mongo/db/cursor.h"
#include "mongo/db/queryutil.h"

namespace mongo {

    CoveredIndexMatcher::CoveredIndexMatcher( const BSONObj &jsobj,
                                             const BSONObj &indexKeyPattern ) :
        _docMatcher( new Matcher( jsobj ) ),
        _keyMatcher( *_docMatcher, indexKeyPattern ) {
        init();
    }

    CoveredIndexMatcher::CoveredIndexMatcher( const CoveredIndexMatcher &prevClauseMatcher,
                                             const shared_ptr<FieldRangeVector> &prevClauseFrv,
                                             const BSONObj &nextClauseIndexKeyPattern ) :
        _docMatcher( prevClauseMatcher._docMatcher ),
        _keyMatcher( *_docMatcher, nextClauseIndexKeyPattern ),
        _orDedupConstraints( prevClauseMatcher._orDedupConstraints ) {
        if ( prevClauseFrv ) {
            _orDedupConstraints.push_back( prevClauseFrv );
        }
        init();
    }

    void CoveredIndexMatcher::init() {
        _needRecord =
            !_keyMatcher.keyMatch( *_docMatcher ) ||
            !_orDedupConstraints.empty();
    }

    // Very ugly.
    //
    // This is essentially a copy/paste of CoveredIndexMatcher::matchesCurrent except 
    // that we know ahread of time that a MatchDetails must exist and that keyUsable = true
    // (and of course that the object-to-match is already loaded / avail, due to limitations
    // in the way the geo code is organized)
    bool CoveredIndexMatcher::matchesWithSingleKeyIndex(const BSONObj &key, const BSONObj &obj,
                                                        MatchDetails *details) const {
        dassert( key.isValid() );
        verify( details );

        details->resetOutput();
        if ( !_keyMatcher.matches(key, details ) ) {
            return false;
        }
        bool needRecordForDetails = details && details->needRecord();
        if ( !_needRecord && !needRecordForDetails ) {
            return true;
        }
        details->setLoadedRecord( true );
        return _docMatcher->matches( obj, details ) && !isOrClauseDup( obj );
    }

    bool CoveredIndexMatcher::matchesCurrent( Cursor * cursor , MatchDetails * details ) const {
        bool keyUsable = true;
        if ( cursor->indexKeyPattern().isEmpty() ) { // unindexed cursor
            keyUsable = false;
        }
        else if ( cursor->isMultiKey() ) {
            keyUsable =
                _keyMatcher.singleSimpleCriterion() &&
                ( ! _docMatcher || _docMatcher->singleSimpleCriterion() );
        }

        const BSONObj key = cursor->currKey();
        dassert( key.isValid() );

        if ( details )
            details->resetOutput();

        if ( keyUsable ) {
            if ( !_keyMatcher.matches(key, details ) ) {
                return false;
            }
            bool needRecordForDetails = details && details->needRecord();
            if ( !_needRecord && !needRecordForDetails ) {
                return true;
            }
        }

        if ( details )
            details->setLoadedRecord( true );

        // Couldn't match off key, need to read full document.
        BSONObj obj = cursor->current();
        bool res =
            _docMatcher->matches( obj, details ) &&
            !isOrClauseDup( obj );
        LOG(5) << "CoveredIndexMatcher _docMatcher->matches() returns " << res << endl;
        return res;
    }
    
    bool CoveredIndexMatcher::isOrClauseDup( const BSONObj &obj ) const {
        for( vector<shared_ptr<FieldRangeVector> >::const_iterator i = _orDedupConstraints.begin();
            i != _orDedupConstraints.end(); ++i ) {
            if ( (*i)->matches( obj ) ) {
                // If a document matches a prior $or clause index range, generally it would have
                // been returned while scanning that range and so is reported as a dup.
                return true;
            }
        }
        return false;
    }

    string CoveredIndexMatcher::toString() const {
        StringBuilder buf;
        buf << "(CoveredIndexMatcher ";
        
        if ( _needRecord )
            buf << "needRecord ";
        
        buf << "keyMatcher: " << _keyMatcher.toString() << " ";
        
        if ( _docMatcher )
            buf << "docMatcher: " << _docMatcher->toString() << " ";
        
        buf << ")";
        return buf.str();
    }
}
