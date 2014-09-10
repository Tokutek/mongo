/**
*    Copyright (C) 2008 10gen Inc.
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

#include <vector>

#include "mongo/db/collection.h"
#include "mongo/db/cursor.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/index.h"
#include "mongo/db/index/2d.h"
#include "mongo/db/commands.h"
#include "mongo/db/matcher.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/geo/core.h"
#include "mongo/db/geo/geonear.h"
#include "mongo/db/geo/hash.h"
#include "mongo/db/geo/shapes.h"

// Note: we use indexinterface herein to talk to the index code. In the future it would be nice to 
//       be able to use the V1 key class (see key.h) instead of toBson() which has some cost.
//       toBson() is new with v1 so this could be slower than it used to be?  a quick profiling
//       might make sense.

namespace mongo {

    class GeoKeyNode { 
        GeoKeyNode();
    public:
        GeoKeyNode(const BSONObj &pk, const BSONObj &k, const BSONObj &o)
            : _pk(pk.getOwned()), _key(k.getOwned()), _obj(o.getOwned()) {
        }
        const BSONObj _pk;
        const BSONObj _key;
        const BSONObj _obj;
    };

    enum GeoDistType {
        GEO_PLAIN,
        GEO_SPHERE
    };

    inline double computeXScanDistance(double y, double maxDistDegrees) {
        // TODO: this overestimates for large madDistDegrees far from the equator
        return maxDistDegrees / min(cos(deg2rad(min(+89.0, y + maxDistDegrees))),
                                    cos(deg2rad(max(-89.0, y - maxDistDegrees))));
    }

    class GeoHopper;

    class GeoPoint {
    public:
        GeoPoint() : _distance(-1), _exact(false) { }

        //// Distance not used ////

        GeoPoint(const GeoKeyNode& node)
            : _key(node._key.getOwned()), _pk(node._pk.getOwned()), _o(node._obj.getOwned()), _distance(-1), _exact(false) { }

        //// Immediate initialization of distance ////

        GeoPoint(const GeoKeyNode& node, double distance, bool exact)
            : _key(node._key.getOwned()), _pk(node._pk.getOwned()), _o(node._obj.getOwned()), _distance(distance), _exact(exact) {
        }

        GeoPoint(const GeoPoint& pt, double distance, bool exact)
            : _key(pt.key().getOwned()), _pk(pt.pk().getOwned()), _o(pt.obj().getOwned()), _distance(distance), _exact(exact) {
        }

        bool operator<(const GeoPoint& other) const {
            if(_distance != other._distance) return _distance < other._distance;
            if(_exact != other._exact) return _exact < other._exact;
            return _pk < other._pk;
        }

        double distance() const {
            return _distance;
        }

        bool isExact() const {
            return _exact;
        }

        BSONObj key() const {
            return _key;
        }

        const BSONObj &pk() const {
            return _pk;
        }

        BSONObj obj() const {
            return _o;
        }

        BSONObj pt() const {
            return _pt;
        }

        bool isEmpty() {
            return _o.isEmpty();
        }

        bool isCleanAndEmpty() {
            return isEmpty();
        }

        string toString() const {
            return str::stream() << "Point from " << _key << " - " << _o << " dist : " << _distance << (_exact ? " (ex)" : " (app)");
        }

        BSONObj _key;
        BSONObj _pk;
        BSONObj _o;
        BSONObj _pt;

        double _distance;
        bool _exact;
    };

    // GeoBrowse subclasses this
    class GeoAccumulator {
    public:
        GeoAccumulator(const TwoDIndex * g, const BSONObj& filter, bool uniqueDocs, bool needDistance)
            : _g(g),
              _lookedAt(0),
              _matchesPerfd(0),
              _objectsLoaded(0),
              _pointsLoaded(0),
              _found(0),
              _uniqueDocs(uniqueDocs),
              _needDistance(needDistance)
        {
            if (! filter.isEmpty()) {
                _matcher.reset(new CoveredIndexMatcher(filter, g->keyPattern()));
                GEODEBUG("Matcher is now " << _matcher->docMatcher().toString());
            }
        }

        virtual ~GeoAccumulator() { }

        enum KeyResult { BAD, BORDER, GOOD };

        virtual void add(const GeoKeyNode& node) {

            GEODEBUG("\t\t\t\t checking key " << node._key.toString())

            _lookedAt++;

            ////
            // Approximate distance check using key data
            ////
            double keyD = 0;
            //Point keyP(_g, GeoHash(node._key.firstElement(), _g->_bits));
            Point keyP(_g->getConverter().unhashToPoint(node._key.firstElement()));
            KeyResult keyOk = approxKeyCheck(keyP, keyD);
            if (keyOk == BAD) {
                GEODEBUG("\t\t\t\t bad distance : " << node._obj << "\t" << keyD);
                return;
            }
            GEODEBUG("\t\t\t\t good distance : " << node._obj << "\t" << keyD);

            ////
            // Check for match using other key (and potentially doc) criteria
            ////
            // Remember match results for each object
            map<BSONObj, bool>::iterator match = _matched.find(node._pk.getOwned());
            bool newDoc = match == _matched.end();
            if(newDoc) {

                GEODEBUG("\t\t\t\t matching new doc with " << (_matcher ? _matcher->docMatcher().toString() : "(empty)"));

                // matcher
                MatchDetails details;
                if (_matcher.get()) {
                    bool good = _matcher->matchesWithSingleKeyIndex(node._key, node._obj, &details);

                    _matchesPerfd++;

                    if (details.hasLoadedRecord())
                        _objectsLoaded++;

                    if (! good) {
                        GEODEBUG("\t\t\t\t didn't match : " << node._obj["_id"]);
                        _matched[ node._pk.getOwned() ] = false;
                        return;
                    }
                }

                _matched[ node._pk.getOwned() ] = true;

                if (! details.hasLoadedRecord()) // don't double count
                    _objectsLoaded++;

            }
            else if(!((*match).second)) {
                GEODEBUG("\t\t\t\t previously didn't match : " << node._obj["_id"]);
                return;
            }

            ////
            // Exact check with particular data fields
            ////
            // Can add multiple points
            int diff = addSpecific(node, keyP, keyOk == BORDER, keyD, newDoc);
            if(diff > 0) _found += diff;
            else _found -= -diff;

        }

        virtual void getPointsFor(const BSONObj& key, const BSONObj& obj,
                                  vector<BSONObj> &locsForNode, bool allPoints = false) {
            // Find all the location objects from the keys
            vector<BSONObj> locs;
            _g->getKeyGenerator()->getGeoLocs(obj, allPoints ? locsForNode : locs);
            ++_pointsLoaded;

            if (allPoints) return;
            if (locs.size() == 1){
                locsForNode.push_back(locs[0]);
                return;
            }

            // Find the particular location we want
            GeoHash keyHash(key.firstElement(), _g->getConverter().getBits());

            for(vector< BSONObj >::iterator i = locs.begin(); i != locs.end(); ++i) {
                // Ignore all locations not hashed to the key's hash, since we may see
                // those later
                if(_g->getConverter().hash(*i) != keyHash) continue;
                locsForNode.push_back(*i);
            }
        }

        virtual int addSpecific(const GeoKeyNode& node, const Point& p, bool inBounds, double d,
                                bool newDoc) = 0;
        virtual KeyResult approxKeyCheck(const Point& p, double& keyD) = 0;
        virtual bool exactDocCheck(const Point& p, double& d) = 0;
        virtual bool expensiveExactCheck(){ return false; }

        long long found() const { return _found; }

        const TwoDIndex * _g;
        map<BSONObj, bool> _matched;
        shared_ptr<CoveredIndexMatcher> _matcher;

        long long _lookedAt;
        long long _matchesPerfd;
        long long _objectsLoaded;
        long long _pointsLoaded;
        long long _found;

        bool _uniqueDocs;
        bool _needDistance;
    };

    struct IndexLocation {
        IndexLocation() { }

        shared_ptr<Cursor> _cursor;
        scoped_ptr<FieldRangeSet> _frs;

        BSONObj key() {
            return _cursor->currKey();
        }

        bool hasPrefix(const GeoHash& hash) {
            BSONObj k = key();
            BSONElement e = k.firstElement();
            if (e.eoo())
                return false;
            return GeoHash(e).hasPrefix(hash);
        }

        bool checkAndAdvance(const GeoHash& hash, int& totalFound, GeoAccumulator* all){
            if(! _cursor->ok() || ! hasPrefix(hash)) return false;

            if(all){
                totalFound++;
                GeoKeyNode n(_cursor->currPK(), _cursor->currKey(), _cursor->current());
                all->add(n);
            }
            _cursor->advance();

            return true;
        }

        string toString() {
            stringstream ss;
            ss << (_cursor->ok() ? (str::stream() << " k: " << _cursor->currKey() << " o : " << _cursor->current()["_id"]) : (string)"[none]") << endl;
            return ss.str();
        }

        // Returns the min and max keys which bound a particular location.
        // The only time these may be equal is when we actually equal the location
        // itself, otherwise our expanding algorithm will fail.
        static bool initial(const IndexDetails& id, const TwoDIndex * spec,
                             IndexLocation& min, IndexLocation&  max,
                             GeoHash start,
                             int & found, GeoAccumulator * hopper) {

            //Ordering ordering = Ordering::make(spec->_order);

            // Would be nice to build this directly, but bug in max/min queries SERVER-3766 and lack of interface
            // makes this easiest for now.
            BSONObj minQuery = BSON(spec->_geo << BSON("$gt" << MINKEY << start.wrap("$lte").firstElement()));
            BSONObj maxQuery = BSON(spec->_geo << BSON("$lt" << MAXKEY << start.wrap("$gt").firstElement()));

            // log() << "MinQuery: " << minQuery << endl;
            // log() << "MaxQuery: " << maxQuery << endl;

            min._frs.reset(new FieldRangeSet(spec->parentNS().c_str(),
                                  minQuery,
                                  true,
                                  false));

            max._frs.reset(new FieldRangeSet(spec->parentNS().c_str(),
                                  maxQuery,
                                  true,
                                  false));


            BSONObjBuilder bob;
            bob.append(spec->_geo, 1);
            for(vector<pair<string, int> >::const_iterator i = spec->_other.begin();
                i != spec->_other.end(); i++){
                bob.append(i->first, i->second);
            }
            BSONObj iSpec = bob.obj();

            shared_ptr<FieldRangeVector> frvMin(new FieldRangeVector(*(min._frs), iSpec, spec->getKeyGenerator(), -1));
            shared_ptr<FieldRangeVector> frvMax(new FieldRangeVector(*(max._frs), iSpec, spec->getKeyGenerator(), 1));

            min._cursor = 
                IndexCursor::make(getCollection(spec->parentNS()),
                                  id,
                                  frvMin,
                                  0,
                                  -1);

            max._cursor =
                IndexCursor::make(getCollection(spec->parentNS()),
                                  id,
                                  frvMax,
                                  0,
                                  1);

            return min._cursor->ok() || max._cursor->ok();
        }
    };


    class GeoCursorBase : public Cursor {
    public:
        static const shared_ptr< CoveredIndexMatcher > emptyMatcher;

        GeoCursorBase(const TwoDIndex * spec)
            : _spec(spec), _id(_spec) {
        }

        virtual BSONObj indexKeyPattern() const {
            return _spec->keyPattern();
        }

        virtual bool getsetdup(const BSONObj &pk) { return false; }
        virtual bool modifiedKeys() const { return true; }
        virtual bool isMultiKey() const { return false; }

        virtual bool autoDedup() const { return false; }

        // TODO: What do we do about this _spec? What goes in its place?
        const TwoDIndex * _spec;
        const IndexDetails * _id;
    };

    const shared_ptr< CoveredIndexMatcher > GeoCursorBase::emptyMatcher(new CoveredIndexMatcher(BSONObj(), BSONObj()));

    // TODO: Pull out the cursor bit from the browse, have GeoBrowse as field of cursor to clean up
    // this hierarchy a bit.  Also probably useful to look at whether GeoAccumulator can be a member instead
    // of a superclass.
    class GeoBrowse : public GeoCursorBase, public GeoAccumulator {
    public:

        // The max points which should be added to an expanding box at one time
        static const int maxPointsHeuristic = 50;

        // Expand states
        enum State {
            START,
            DOING_EXPAND,
            DONE_NEIGHBOR,
            DONE
        } _state;

        GeoBrowse(const TwoDIndex * g, string type, BSONObj filter = BSONObj(), bool uniqueDocs = true, bool needDistance = false)
            : GeoCursorBase(g), GeoAccumulator(g, filter, uniqueDocs, needDistance),
              _type(type), _filter(filter), _firstCall(true), _noted(false), _nscanned(), _nDirtied(0), _centerPrefix(0, 0, 0) {

            // Set up the initial expand state
            _state = START;
            _neighbor = -1;
            _foundInExp = 0;

        }

        virtual string toString() const {
            return (string)"GeoBrowse-" + _type;
        }

        virtual bool ok() {

            LOG(CDEBUG) << "Checking cursor, in state " << (int) _state << ", first call " << _firstCall <<
                             ", empty : " << _cur.isEmpty() << ", stack : " << _stack.size() << endl;

            bool first = _firstCall;
            if (_firstCall) {
                fillStack(maxPointsHeuristic);
                _firstCall = false;
            }
            if (! _cur.isCleanAndEmpty() || _stack.size()) {
                if (first) {
                    ++_nscanned;
                }

                return true;
            }

            while (moreToDo()) {

                LOG(CDEBUG) << "Refilling stack..." << endl;

                fillStack(maxPointsHeuristic);

                if (! _cur.isCleanAndEmpty()) {
                    if (first) {
                        ++_nscanned;
                    }

                    return true;
                }
            }

            return false;
        }

        virtual bool advance() {
            _cur._o = BSONObj();

            if (_stack.size()) {
                _cur = _stack.front();
                _stack.pop_front();
                ++_nscanned;
                return true;
            }

            if (! moreToDo())
                return false;

            while (_cur.isCleanAndEmpty() && moreToDo()){
                fillStack(maxPointsHeuristic);
            }

            return ! _cur.isCleanAndEmpty() && ++_nscanned;
        }

        void fixMatches(const BSONObj &oldPK, const BSONObj &newPK) {
            map<BSONObj, bool>::iterator match = _matched.find(oldPK);
            if (match != _matched.end()) {
                bool val = match->second;
                _matched.erase(oldPK);
                _matched[newPK] = val;
            }
        }

        virtual BSONObj current() { LOG(CDEBUG + 1) << "current " << _cur._o << endl; return _cur._o; }
        virtual BSONObj currPK() const { LOG(CDEBUG + 1) << "currPK " << _cur._pk << endl; return _cur._pk; }
        virtual BSONObj currKey() const { return _cur._key; }

        virtual CoveredIndexMatcher* matcher() const {
            if(_matcher.get()) return _matcher.get();
            else return GeoCursorBase::emptyMatcher.get();
        }

        // Are we finished getting points?
        virtual bool moreToDo() {
            return _state != DONE;
        }

        // XXX: make private, move into geohashconverter
        Box makeBox(const GeoHash &hash) const {
            double sizeEdge = _g->getConverter().sizeEdge(hash);
            Point min(_g->getConverter().unhashToPoint(hash));
            Point max(min.x + sizeEdge, min.y + sizeEdge);
            return Box(min, max);
        }

        // Fills the stack, but only checks a maximum number of maxToCheck points at a time.
        // Further calls to this function will continue the expand/check neighbors algorithm.
        virtual void fillStack(int maxToCheck, int maxToAdd = -1, bool onlyExpand = false) {

#ifdef GEODEBUGGING
            log() << "Filling stack with maximum of " << maxToCheck << ", state : " << (int) _state << endl;
#endif

            if(maxToAdd < 0) maxToAdd = maxToCheck;
            int maxFound = _foundInExp + maxToCheck;
            verify(maxToCheck > 0);
            verify(maxFound > 0);
            verify(_found <= 0x7fffffff); // conversion to int
            int maxAdded = static_cast<int>(_found) + maxToAdd;
            verify(maxAdded >= 0); // overflow check

            bool isNeighbor = _centerPrefix.constrains();

            // Starting a box expansion
            if (_state == START) {

                // Get the very first hash point, if required
                if(! isNeighbor)
                    _prefix = expandStartHash();
                GEODEBUG("initializing index");

#ifdef GEODEBUGGING
                log() << "Initializing from b-tree with hash of " << _prefix << " @ " << Box(_g, _prefix) << endl;
#endif

                if (! IndexLocation::initial(*_id, _spec, _min, _max, _prefix, _foundInExp, this)) {
                    _state = isNeighbor ? DONE_NEIGHBOR : DONE;
                } else {
                    _state = DOING_EXPAND;
                    _lastPrefix.reset();
                }

                GEODEBUG((_state == DONE_NEIGHBOR || _state == DONE ? "not initialized" : "initializedFig"));

            }

            // Doing the actual box expansion
            if (_state == DOING_EXPAND) {
                while (true) {

                    GEODEBUG("box prefix [" << _prefix << "]");
#ifdef GEODEBUGGING
                    if(_prefix.constrains()) {
                        log() << "current expand box : " << Box(_g, _prefix).toString() << endl;
                    }
                    else {
                        log() << "max expand box." << endl;
                    }
#endif

                    GEODEBUG("expanding box points... ");

                    // Record the prefix we're actively exploring...
                    _expPrefix.reset(new GeoHash(_prefix));

                    // Find points inside this prefix
                    while (_min.checkAndAdvance(_prefix, _foundInExp, this) && _foundInExp < maxFound && _found < maxAdded) {}
                    while (_max.checkAndAdvance(_prefix, _foundInExp, this) && _foundInExp < maxFound && _found < maxAdded) {}

#ifdef GEODEBUGGING

                    log() << "finished expand, checked : " << (maxToCheck - (maxFound - _foundInExp))
                          << " found : " << (maxToAdd - (maxAdded - _found))
                          << " max : " << maxToCheck << " / " << maxToAdd << endl;

#endif

                    GEODEBUG("finished expand, found : " << (maxToAdd - (maxAdded - _found)));
                    if(_foundInExp >= maxFound || _found >= maxAdded) return;

                    // We've searched this prefix fully, remember
                    _lastPrefix.reset(new GeoHash(_prefix));

                    // If we've searched the entire space, we're finished.
                    if (! _prefix.constrains()) {
                        GEODEBUG("box exhausted");
                        _state = DONE;
                        notePrefix();
                        return;
                    }

                    // If we won't fit in the box, and we're not doing a sub-scan, increase the size
                    if (! fitsInBox(_g->getConverter().sizeEdge(_prefix)) && _fringe.size() == 0) {
                        // If we're still not expanded bigger than the box size, expand again
                        // TODO: Is there an advantage to scanning prior to expanding?
                        _prefix = _prefix.up();
                        continue;
                    }

                    // log() << "finished box prefix [" << _prefix << "]" << endl;

                    // We're done and our size is large enough
                    _state = DONE_NEIGHBOR;

                    // Go to the next sub-box, if applicable
                    if(_fringe.size() > 0) _fringe.pop_back();
                    // Go to the next neighbor if this was the last sub-search
                    if(_fringe.size() == 0) _neighbor++;

                    break;
                }

                notePrefix();
            }

            // If we doeighbors
            if(onlyExpand) return;

            // If we're done expanding the current box...
            if(_state == DONE_NEIGHBOR) {
                // Iterate to the next neighbor
                // Loop is useful for cases where we want to skip over boxes entirely,
                // otherwise recursion increments the neighbors.
                for (; _neighbor < 9; _neighbor++) {
                    // If we have no fringe for the neighbor, make sure we have the default fringe
                    if(_fringe.size() == 0) _fringe.push_back("");

                    if(! isNeighbor) {
                        _centerPrefix = _prefix;
                        _centerBox = makeBox(_centerPrefix);
                        isNeighbor = true;
                    }

                    int i = (_neighbor / 3) - 1;
                    int j = (_neighbor % 3) - 1;

                    if ((i == 0 && j == 0) ||
                         (i < 0 && _centerPrefix.atMinX()) ||
                         (i > 0 && _centerPrefix.atMaxX()) ||
                         (j < 0 && _centerPrefix.atMinY()) ||
                         (j > 0 && _centerPrefix.atMaxY())) {

                        //log() << "not moving to neighbor " << _neighbor << " @ " << i << ", " << j << " fringe : " << _fringe.size() << " " << _centerPrefix << endl;
                        //log() << _centerPrefix.atMinX() << " "
                        //        << _centerPrefix.atMinY() << " "
                        //        << _centerPrefix.atMaxX() << " "
                        //        << _centerPrefix.atMaxY() << " " << endl;

                        continue; // main box or wrapped edge
                        // TODO:  We may want to enable wrapping in future, probably best as layer on top of
                        // this search.
                    }

                    // Make sure we've got a reasonable center
                    verify(_centerPrefix.constrains());

                    GeoHash _neighborPrefix = _centerPrefix;
                    _neighborPrefix.move(i, j);

                    //log() << "moving to neighbor " << _neighbor << " @ " << i << ", " << j << " fringe : " << _fringe.size() << " " << _centerPrefix << " " << _neighborPrefix << endl;

                    GEODEBUG("moving to neighbor " << _neighbor << " @ " << i << ", " << j
                                                   << " fringe : " << _fringe.size());
                    PREFIXDEBUG(_centerPrefix, _g);
                    PREFIXDEBUG(_neighborPrefix, _g);

                    while(_fringe.size() > 0) {
                        _prefix = _neighborPrefix + _fringe.back();
                        Box cur(makeBox(_prefix));

                        PREFIXDEBUG(_prefix, _g);

                        double intAmt = intersectsBox(cur);

                        // No intersection
                        if(intAmt <= 0) {
                            GEODEBUG("skipping box" << cur.toString());
                            _fringe.pop_back();
                            continue;
                        }
                        // Small intersection, refine search
                        else if(intAmt < 0.5 && _prefix.canRefine() && _fringe.back().size() < 4 /* two bits */) {

                            GEODEBUG("Intersection small : " << intAmt << ", adding to fringe: " << _fringe.back() << " curr prefix : " << _prefix << " bits : " << _prefix.getBits());

                            // log() << "Diving to level : " << (_fringe.back().size() / 2 + 1) << endl;

                            string lastSuffix = _fringe.back();
                            _fringe.pop_back();
                            _fringe.push_back(lastSuffix + "00");
                            _fringe.push_back(lastSuffix + "01");
                            _fringe.push_back(lastSuffix + "11");
                            _fringe.push_back(lastSuffix + "10");

                            continue;
                        }

                        // Restart our search from a diff box.
                        _state = START;

                        verify(! onlyExpand);

                        verify(_found <= 0x7fffffff);
                        fillStack(maxFound - _foundInExp, maxAdded - static_cast<int>(_found));

                        // When we return from the recursive fillStack call, we'll either have checked enough points or
                        // be entirely done.  Max recurse depth is < 8 * 16.

                        // If we're maxed out on points, return
                        if(_foundInExp >= maxFound || _found >= maxAdded) {
                            // Make sure we'll come back to add more points
                            verify(_state == DOING_EXPAND);
                            return;
                        }

                        // Otherwise we must be finished to return
                        verify(_state == DONE);
                        return;

                    }

                }

                // Finished with neighbors
                _state = DONE;
            }

        }

        // The initial geo hash box for our first expansion
        virtual GeoHash expandStartHash() = 0;

        // Whether the current box width is big enough for our search area
        virtual bool fitsInBox(double width) = 0;

        // The amount the current box overlaps our search area
        virtual double intersectsBox(Box& cur) = 0;

        bool remembered(BSONObj o){
            BSONObj seenId = o["_id"].wrap("").getOwned();
            if(_seenIds.find(seenId) != _seenIds.end()){
                LOG(CDEBUG + 1) << "Object " << o["_id"] << " already seen." << endl;
                return true;
            }
            else{
                _seenIds.insert(seenId);
                LOG(CDEBUG + 1) << "Object " << o["_id"] << " remembered." << endl;
                return false;
            }
        }

        virtual int addSpecific(const GeoKeyNode& node, const Point& keyP, bool onBounds, double keyD, bool potentiallyNewDoc) {

            int found = 0;

            // We need to handle every possible point in this method, even those not in the key value, to
            // avoid us tracking which hashes we've already seen.
            if(! potentiallyNewDoc){
                // log() << "Already handled doc!" << endl;
                return 0;
            }

            // Final check for new doc
            // OK to touch, since we're probably returning this object now
            if(remembered(node._obj)) return 0;

            if(_uniqueDocs && ! onBounds) {
                //log() << "Added ind to " << _type << endl;
                _stack.push_front(GeoPoint(node));
                found++;
            }
            else {
                // We now handle every possible point in the document, even those not in the key value,
                // since we're iterating through them anyway - prevents us from having to save the hashes
                // we've seen per-doc

                // If we're filtering by hash, get the original
                bool expensiveExact = expensiveExactCheck();

                vector< BSONObj > locs;
                getPointsFor(node._key, node._obj, locs, true);
                for(vector< BSONObj >::iterator i = locs.begin(); i != locs.end(); ++i){

                    double d = -1;
                    Point p(*i);

                    // We can avoid exact document checks by redoing approx checks,
                    // if the exact checks are more expensive.
                    bool needExact = true;
                    if(expensiveExact){
                        verify(false);
                        KeyResult result = approxKeyCheck(p, d);
                        if(result == BAD) continue;
                        else if(result == GOOD) needExact = false;
                    }

                    if(! needExact || exactDocCheck(p, d)){
                        //log() << "Added mult to " << _type << endl;
                        _stack.push_front(GeoPoint(node));
                        found++;
                        // If returning unique, just exit after first point is added
                        if(_uniqueDocs) break;
                    }
                }
            }

            while(_cur.isCleanAndEmpty() && _stack.size() > 0){
                _cur = _stack.front();
                _stack.pop_front();
            }

            return found;
        }

        virtual long long nscanned() const {
            if (_firstCall) {
                // Forgive me.
                const_cast<GeoBrowse *>(this)->ok();
            }
            return _nscanned;
        }

        virtual void explainDetails(BSONObjBuilder& b) const {
            b << "lookedAt" << _lookedAt;
            b << "matchesPerfd" << _matchesPerfd;
            b << "objectsLoaded" << _objectsLoaded;
            b << "pointsLoaded" << _pointsLoaded;
        }

        virtual BSONObj prettyIndexBounds() const {

            vector<GeoHash>::const_iterator i = _expPrefixes.end();
            if(_expPrefixes.size() > 0 && *(--i) != *(_expPrefix.get()))
                _expPrefixes.push_back(*(_expPrefix.get()));

            BSONObjBuilder bob;
            BSONArrayBuilder bab;
            for(i = _expPrefixes.begin(); i != _expPrefixes.end(); ++i){
                bab << makeBox(*i).toBSON();
            }
            bob << _g->_geo << bab.arr();

            return bob.obj();

        }

        void notePrefix() {
            _expPrefixes.push_back(_prefix);
        }

        string _type;
        BSONObj _filter;
        list<GeoPoint> _stack;
        set<BSONObj> _seenIds;

        GeoPoint _cur;
        bool _firstCall;
        bool _noted;

        long long _nscanned;
        long long _nDirtied;

        // The current box we're expanding (-1 is first/center box)
        int _neighbor;

        // The points we've found so far
        // TODO:  Long long?
        int _foundInExp;

        // The current hash prefix we're expanding and the center-box hash prefix
        GeoHash _prefix;
        shared_ptr<GeoHash> _lastPrefix;
        GeoHash _centerPrefix;
        list<string> _fringe;
        int recurseDepth;
        Box _centerBox;

        // Start and end of our search range in the current box
        IndexLocation _min;
        IndexLocation _max;

        shared_ptr<GeoHash> _expPrefix;
        mutable vector<GeoHash> _expPrefixes;

    };

    class GeoHopper : public GeoBrowse {
    public:
        typedef multiset<GeoPoint> Holder;

        GeoHopper(const TwoDIndex * g,
                  unsigned max,
                  const Point& n,
                  const BSONObj& filter = BSONObj(),
                  double maxDistance = numeric_limits<double>::max(),
                  GeoDistType type = GEO_PLAIN,
                  bool uniqueDocs = false,
                  bool needDistance = true)
            : GeoBrowse(g, "search", filter, uniqueDocs, needDistance),
              _max(max),
              _near(n),
              _maxDistance(maxDistance),
              _type(type),
              _distError(type == GEO_PLAIN ? g->getConverter().getError() 
                                           : g->getConverter().getErrorSphere()),
              _farthest(0)
        {}

        virtual KeyResult approxKeyCheck(const Point& p, double& d) {
            // Always check approximate distance, since it lets us avoid doing
            // checks of the rest of the object if it succeeds
            switch (_type) {
            case GEO_PLAIN:
                d = distance(_near, p);
                break;
            case GEO_SPHERE:
                checkEarthBounds(p);
                d = spheredist_deg(_near, p);
                break;
            default: verify(false);
            }
            verify(d >= 0);

            GEODEBUG("\t\t\t\t\t\t\t checkDistance " << _near.toString()
                      << "\t" << p.toString() << "\t" << d
                      << " farthest: " << farthest());

            // If we need more points
            double borderDist = (_points.size() < _max ? _maxDistance : farthest());

            if (d >= borderDist - 2 * _distError && d <= borderDist + 2 * _distError) return BORDER;
            else return d < borderDist ? GOOD : BAD;
        }

        virtual bool exactDocCheck(const Point& p, double& d){
            bool within = false;

            // Get the appropriate distance for the type
            switch (_type) {
            case GEO_PLAIN:
                d = distance(_near, p);
                within = distanceWithin(_near, p, _maxDistance);
                break;
            case GEO_SPHERE:
                checkEarthBounds(p);
                d = spheredist_deg(_near, p);
                within = (d <= _maxDistance);
                break;
            default: verify(false);
            }

            return within;
        }

        // Always in distance units, whether radians or normal
        double farthest() const {
            return _farthest;
        }

        virtual int addSpecific(const GeoKeyNode& node, const Point& keyP, bool onBounds, double keyD, bool potentiallyNewDoc) {

            // Unique documents

            GeoPoint newPoint(node, keyD, false);

            int prevSize = _points.size();

            // STEP 1 : Remove old duplicate points from the set if needed
            if(_uniqueDocs){

                // Lookup old point with same doc
                map< BSONObj, Holder::iterator >::iterator oldPointIt = _seenPts.find(newPoint.pk().getOwned());

                if(oldPointIt != _seenPts.end()){
                    const GeoPoint& oldPoint = *(oldPointIt->second);
                    // We don't need to care if we've already seen this same approx pt or better,
                    // or we've already gone to disk once for the point
                    if(oldPoint < newPoint){
                        GEODEBUG("\t\tOld point closer than new point");
                        return 0;
                    }
                    GEODEBUG("\t\tErasing old point " << oldPointIt->first.obj());
                    _points.erase(oldPointIt->second);
                }
            }

            Holder::iterator newIt = _points.insert(newPoint);
            if(_uniqueDocs) _seenPts[ newPoint.pk().getOwned() ] = newIt;

            GEODEBUG("\t\tInserted new point " << newPoint.toString() << " approx : " << keyD);

            verify(_max > 0);

            Holder::iterator lastPtIt = _points.end();
            lastPtIt--;
            _farthest = lastPtIt->distance() + 2 * _distError;

            return _points.size() - prevSize;

        }

        // Removes extra points from end of _points set.
        // Check can be a bit costly if we have lots of exact points near borders,
        // so we'll do this every once and awhile.
        void processExtraPoints(){

            if(_points.size() == 0) return;

            int prevSize = _points.size();

            // Erase all points from the set with a position >= _max *and*
            // whose distance isn't close to the _max - 1 position distance

            int numToErase = _points.size() - _max;
            if(numToErase < 0) numToErase = 0;

            // Get the first point definitely in the _points array
            Holder::iterator startErase = _points.end();
            for(int i = 0; i < numToErase + 1; i++) startErase--;
            _farthest = startErase->distance() + 2 * _distError;

            GEODEBUG("\t\tPotentially erasing " << numToErase << " points, " << " size : " << _points.size() << " max : " << _max << " dist : " << startErase->distance() << " farthest dist : " << _farthest << " from error : " << _distError);

            startErase++;
            while(numToErase > 0 && startErase->distance() <= _farthest){
                GEODEBUG("\t\tNot erasing point " << startErase->toString());
                numToErase--;
                startErase++;
                verify(startErase != _points.end() || numToErase == 0);
            }

            if(_uniqueDocs){
                for(Holder::iterator i = startErase; i != _points.end(); ++i)
                    _seenPts.erase(i->pk());
            }

            _points.erase(startErase, _points.end());

            int diff = _points.size() - prevSize;
            if(diff > 0) _found += diff;
            else _found -= -diff;

        }

        unsigned _max;
        Point _near;
        Holder _points;
        double _maxDistance;
        GeoDistType _type;
        double _distError;
        double _farthest;

        map< BSONObj, Holder::iterator > _seenPts;
    };



    class GeoSearch : public GeoHopper {
    public:
        GeoSearch(const TwoDIndex * g,
                  const Point& startPt,
                  int numWanted = 100,
                  BSONObj filter = BSONObj(),
                  double maxDistance = numeric_limits<double>::max(),
                  GeoDistType type = GEO_PLAIN,
                  bool uniqueDocs = false,
                  bool needDistance = false)
           : GeoHopper(g, numWanted, startPt, filter, maxDistance, type, uniqueDocs, needDistance),
             _start(g->getConverter().hash(startPt.x, startPt.y)),
             // TODO:  Remove numWanted...
             _numWanted(numWanted),
             _type(type)
        {

            _nscanned = 0;
            _found = 0;

            if(_maxDistance < 0){
               _scanDistance = numeric_limits<double>::max();
            }
            else if (type == GEO_PLAIN) {
                _scanDistance = maxDistance + _spec->getConverter().getError();
            }
            else if (type == GEO_SPHERE) {
                checkEarthBounds(startPt);
                // TODO: consider splitting into x and y scan distances
                _scanDistance = computeXScanDistance(startPt.y,
                    rad2deg(_maxDistance) + _spec->getConverter().getError());
            }

            verify(_scanDistance > 0);

        }


        /** Check if we've already looked at a key.  ALSO marks as seen, anticipating a follow-up call
            to add().  This is broken out to avoid some work extracting the key bson if it's an
            already seen point.
        */
    private:
        set< pair<BSONObj,int> > _seen;
    public:

        void exec() {

            if(_numWanted == 0) return;

            /*
             * Search algorithm
             * 1) use geohash prefix to find X items
             * 2) compute max distance from want to an item
             * 3) find optimal set of boxes that complete circle
             * 4) use regular index cursors to scan those boxes
             */

#ifdef GEODEBUGGING

           log() << "start near search for " << _numWanted << " points near " << _near << " (max dist " << _maxDistance << ")" << endl;

#endif

           // Part 1
           {
               do {
                   long long f = found();
                   verify(f <= 0x7fffffff);
                   fillStack(maxPointsHeuristic, _numWanted - static_cast<int>(f), true);
                   processExtraPoints();
               } while(_state != DONE && _state != DONE_NEIGHBOR &&
                        found() < _numWanted &&
                        (!_prefix.constrains() ||
                         _g->getConverter().sizeEdge(_prefix) <= _scanDistance));

               // If we couldn't scan or scanned everything, we're done
               if(_state == DONE){
                   expandEndPoints();
                   return;
               }
           }

#ifdef GEODEBUGGING

           log() << "part 1 of near search completed, found " << found() << " points (out of " << _foundInExp << " scanned)"
                 << " in expanded region " << _prefix << " @ " << Box(_g, _prefix)
                 << " with furthest distance " << farthest() << endl;

#endif

           // Part 2
            {
               // Find farthest distance for completion scan
                double farDist = farthest();
                if(found() < _numWanted) {
                    // Not enough found in Phase 1
                    farDist = _scanDistance;
                }
                else if (_type == GEO_PLAIN) {
                   // Enough found, but need to search neighbor boxes
                    farDist += _spec->getConverter().getError();
                }
                else if (_type == GEO_SPHERE) {
                   // Enough found, but need to search neighbor boxes
                    farDist = std::min(_scanDistance,
                                       computeXScanDistance(_near.y,
                                         rad2deg(farDist)) + 2 * _spec->getConverter().getError());
                }
                verify(farDist >= 0);
                GEODEBUGPRINT(farDist);

                // Find the box that includes all the points we need to return
                _want = Box(_near.x - farDist, _near.y - farDist, farDist * 2);
                GEODEBUGPRINT(_want.toString());

                // log() << "Found : " << found() << " wanted : " << _numWanted << " Far distance : " << farDist << " box : " << _want << endl;

                // Remember the far distance for further scans
                _scanDistance = farDist;

                // Reset the search, our distances have probably changed
                if(_state == DONE_NEIGHBOR){
                   _state = DOING_EXPAND;
                   _neighbor = -1;
                }

#ifdef GEODEBUGGING

                log() << "resetting search with start at " << _start << " (edge length " << _g->sizeEdge(_start) << ")" << endl;

#endif

                // Do regular search in the full region
                do {
                   fillStack(maxPointsHeuristic);
                   processExtraPoints();
                }
                while(_state != DONE);

            }

            GEODEBUG("done near search with " << _points.size() << " points ");

            expandEndPoints();

        }

        void addExactPoints(const GeoPoint& pt, Holder& points, bool force){
            int before, after;
            addExactPoints(pt, points, before, after, force);
        }

        void addExactPoints(const GeoPoint& pt, Holder& points, int& before, int& after, bool force){

            before = 0;
            after = 0;

            GEODEBUG("Adding exact points for " << pt.toString());

            if(pt.isExact()){
                if(force) points.insert(pt);
                return;
            }

            vector<BSONObj> locs;
            getPointsFor(pt.key(), pt.obj(), locs, _uniqueDocs);

            GeoPoint nearestPt(pt, -1, true);

            for(vector<BSONObj>::iterator i = locs.begin(); i != locs.end(); i++){

                Point loc(*i);

                double d;
                if(! exactDocCheck(loc, d)) continue;

                if(_uniqueDocs && (nearestPt.distance() < 0 || d < nearestPt.distance())){
                    nearestPt._distance = d;
                    nearestPt._pt = i->getOwned();
                    continue;
                }
                else if(! _uniqueDocs){
                    GeoPoint exactPt(pt, d, true);
                    exactPt._pt = i->getOwned();
                    GEODEBUG("Inserting exact pt " << exactPt.toString() << " for " << pt.toString() << " exact : " << d << " is less? " << (exactPt < pt) << " bits : " << _g->_bits);
                    points.insert(exactPt);
                    exactPt < pt ? before++ : after++;
                }

            }

            if(_uniqueDocs && nearestPt.distance() >= 0){
                GEODEBUG("Inserting unique exact pt " << nearestPt.toString() << " for " << pt.toString() << " exact : " << nearestPt.distance() << " is less? " << (nearestPt < pt) << " bits : " << _g->_bits);
                points.insert(nearestPt);
                if(nearestPt < pt) before++;
                else after++;
            }

        }

        // TODO: Refactor this back into holder class, allow to run periodically when we are seeing a lot of pts
        void expandEndPoints(bool finish = true){

            processExtraPoints();

            // All points in array *could* be in maxDistance

            // Step 1 : Trim points to max size
            // TODO:  This check will do little for now, but is skeleton for future work in incremental $near
            // searches
            if(_max > 0){

                int numToErase = _points.size() - _max;

                if(numToErase > 0){

                    Holder tested;

                    // Work backward through all points we're not sure belong in the set
                    Holder::iterator maybePointIt = _points.end();
                    maybePointIt--;
                    double approxMin = maybePointIt->distance() - 2 * _distError;

                    GEODEBUG("\t\tNeed to erase " << numToErase << " max : " << _max << " min dist " << approxMin << " error : " << _distError << " starting from : " << (*maybePointIt).toString());

                    // Insert all
                    int erased = 0;
                    while(_points.size() > 0 && (maybePointIt->distance() >= approxMin || erased < numToErase)){

                        Holder::iterator current = maybePointIt--;

                        addExactPoints(*current, tested, true);
                        _points.erase(current);
                        erased++;

                        if(tested.size())
                            approxMin = tested.begin()->distance() - 2 * _distError;

                    }

                    GEODEBUG("\t\tEnding search at point " << (_points.size() == 0 ? "(beginning)" : maybePointIt->toString()));

                    int numToAddBack = erased - numToErase;
                    verify(numToAddBack >= 0);

                    GEODEBUG("\t\tNum tested valid : " << tested.size() << " erased : " << erased << " added back : " << numToAddBack);

#ifdef GEODEBUGGING
                    for(Holder::iterator it = tested.begin(); it != tested.end(); it++){
                        log() << "Tested Point: " << *it << endl;
                    }
#endif
                    Holder::iterator testedIt = tested.begin();
                    for(int i = 0; i < numToAddBack && testedIt != tested.end(); i++){
                        _points.insert(*testedIt);
                        testedIt++;
                    }
                }
            }

#ifdef GEODEBUGGING
            for(Holder::iterator it = _points.begin(); it != _points.end(); it++){
                log() << "Point: " << *it << endl;
            }
#endif
            // We've now trimmed first set of unneeded points

            GEODEBUG("\t\t Start expanding, num points : " << _points.size() << " max : " << _max);

            // Step 2: iterate through all points and add as needed
            unsigned expandedPoints = 0;
            Holder::iterator it = _points.begin();
            double expandWindowEnd = -1;

            while(it != _points.end()){
                const GeoPoint& currPt = *it;
                // TODO: If one point is exact, maybe not 2 * _distError

                // See if we're in an expand window
                bool inWindow = currPt.distance() <= expandWindowEnd;
                // If we're not, and we're done with points, break
                if(! inWindow && expandedPoints >= _max) break;

                bool expandApprox = !currPt.isExact() &&
                                    (!_uniqueDocs || (finish && _needDistance) || inWindow);

                if (expandApprox) {
                    // Add new point(s). These will only be added in a radius of 2 * _distError
                    // around the current point, so should not affect previously valid points.
                    int before, after;
                    addExactPoints(currPt, _points, before, after, false);
                    expandedPoints += before;

                    if(_max > 0 && expandedPoints < _max)
                        expandWindowEnd = currPt.distance() + 2 * _distError;

                    // Iterate to the next point
                    Holder::iterator current = it++;
                    // Erase the current point
                    _points.erase(current);

                }
                else{
                    expandedPoints++;
                    it++;
                }
            }

            GEODEBUG("\t\tFinished expanding, num points : " << _points.size()
                     << " max : " << _max);

            // Finish
            // TODO:  Don't really need to trim?
            for(; expandedPoints > _max; expandedPoints--) it--;
            _points.erase(it, _points.end());

#ifdef GEODEBUGGING
            for(Holder::iterator it = _points.begin(); it != _points.end(); it++){
                log() << "Point: " << *it << endl;
            }
#endif
        }

        virtual GeoHash expandStartHash(){
           return _start;
        }

        // Whether the current box width is big enough for our search area
        virtual bool fitsInBox(double width){
           return width >= _scanDistance;
        }

        // Whether the current box overlaps our search area
        virtual double intersectsBox(Box& cur){
            return cur.intersects(_want);
        }

        GeoHash _start;
        int _numWanted;
        double _scanDistance;

        long long _nscanned;
        int _found;
        GeoDistType _type;

        Box _want;
    };

    class GeoSearchCursor : public GeoCursorBase {
    public:
        GeoSearchCursor(shared_ptr<GeoSearch> s)
            : GeoCursorBase(s->_spec),
              _s(s), _cur(s->_points.begin()), _end(s->_points.end()), _nscanned() {
            if (_cur != _end) {
                ++_nscanned;
            }
        }

        virtual ~GeoSearchCursor() {}

        virtual bool ok() {
            return _cur != _end;
        }

        virtual BSONObj current() { verify(ok()); return _cur->_o; }
        virtual BSONObj currPK() const { return _cur->_pk; }
        virtual bool advance() {
            if(ok()){
                _cur++;
                incNscanned();
                return ok();
            }
            return false;
        }
        virtual BSONObj currKey() const { return _cur->_key; }

        virtual string toString() const {
            return "GeoSearchCursor";
        }


        virtual BSONObj prettyStartKey() const {
            return BSON(_s->_g->_geo << _s->_prefix.toString());
        }
        virtual BSONObj prettyEndKey() const {
            GeoHash temp = _s->_prefix;
            temp.move(1, 1);
            return BSON(_s->_g->_geo << temp.toString());
        }

        virtual long long nscanned() const { return _nscanned; }

        virtual CoveredIndexMatcher* matcher() const {
            if(_s->_matcher.get()) return _s->_matcher.get();
            else return emptyMatcher.get();
        }

        shared_ptr<GeoSearch> _s;
        GeoHopper::Holder::iterator _cur;
        GeoHopper::Holder::iterator _end;

        void incNscanned() { if (ok()) { ++_nscanned; } }
        long long _nscanned;
    };

    class GeoCircleBrowse : public GeoBrowse {
    public:
        GeoCircleBrowse(const TwoDIndex * g, const BSONObj& circle, BSONObj filter = BSONObj(),
                        const string& type = "$center", bool uniqueDocs = true)
            : GeoBrowse(g, "circle", filter, uniqueDocs) {

            uassert(13060, "$center needs 2 fields (middle,max distance)", circle.nFields() == 2);

            BSONObjIterator i(circle);
            BSONElement center = i.next();

            uassert(13656, "the first field of $center object must be a location object",
                    center.isABSONObj());

            // Get geohash and exact center point
            // TODO: For wrapping search, may be useful to allow center points outside-of-bounds
            // here.  Calculating the nearest point as a hash start inside the region would then be
            // required.
            _start = g->getConverter().hash(center);
            _startPt = Point(center);

            _maxDistance = i.next().numberDouble();
            uassert(13061, "need a max distance >= 0 ", _maxDistance >= 0);

            if (type == "$center") {
                // Look in box with bounds of maxDistance in either direction
                _type = GEO_PLAIN;
                xScanDistance = _maxDistance + _g->getConverter().getError();
                yScanDistance = _maxDistance + _g->getConverter().getError();
            }
            else if (type == "$centerSphere") {
                // Same, but compute maxDistance using spherical transform
                uassert(13461, "Spherical MaxDistance > PI. Are you sure you are using radians?",
                        _maxDistance < M_PI);
                checkEarthBounds(_startPt);

                _type = GEO_SPHERE;
                // should this be sphere error?
                yScanDistance = rad2deg(_maxDistance) + _g->getConverter().getError();
                xScanDistance = computeXScanDistance(_startPt.y, yScanDistance);

                uassert(13462, "Spherical distance would require (unimplemented) wrapping",
                        (_startPt.x + xScanDistance < 180) &&
                        (_startPt.x - xScanDistance > -180) &&
                        (_startPt.y + yScanDistance < 90) &&
                        (_startPt.y - yScanDistance > -90));
            }
            else {
                uassert(13460, "invalid $center query type: " + type, false);
            }

            // Bounding box includes fudge factor.
            // TODO:  Is this correct, since fudge factor may be spherically transformed?
            _bBox._min = Point(_startPt.x - xScanDistance, _startPt.y - yScanDistance);
            _bBox._max = Point(_startPt.x + xScanDistance, _startPt.y + yScanDistance);

            GEODEBUG("Bounding box for circle query : " << _bBox.toString()
                     << " (max distance : " << _maxDistance << ")"
                     << " starting from " << _startPt.toString());
            ok();
        }

        virtual GeoHash expandStartHash() {
            return _start;
        }

        virtual bool fitsInBox(double width) {
            return width >= std::max(xScanDistance, yScanDistance);
        }

        virtual double intersectsBox(Box& cur) {
            return cur.intersects(_bBox);
        }

        virtual KeyResult approxKeyCheck(const Point& p, double& d) {
            // Inexact hash distance checks.
            double error = 0;
            switch (_type) {
            case GEO_PLAIN:
                d = distance(_startPt, p);
                error = _g->getConverter().getError();
                break;
            case GEO_SPHERE: {
                checkEarthBounds(p);
                d = spheredist_deg(_startPt, p);
                error = _g->getConverter().getErrorSphere();
                break;
            }
            default: verify(false);
            }

            // If our distance is in the error bounds...
            if(d >= _maxDistance - error && d <= _maxDistance + error) return BORDER;
            return d > _maxDistance ? BAD : GOOD;
        }

        virtual bool exactDocCheck(const Point& p, double& d){
            switch (_type) {
            case GEO_PLAIN: {
                if(distanceWithin(_startPt, p, _maxDistance)) return true;
                break;
            }
            case GEO_SPHERE:
                checkEarthBounds(p);
                if(spheredist_deg(_startPt, p) <= _maxDistance) return true;
                break;
            default: verify(false);
            }

            return false;
        }

        GeoDistType _type;
        GeoHash _start;
        Point _startPt;
        double _maxDistance; // user input
        double xScanDistance; // effected by GeoDistType
        double yScanDistance; // effected by GeoDistType
        Box _bBox;

    };

    class GeoBoxBrowse : public GeoBrowse {
    public:
        GeoBoxBrowse(const TwoDIndex * g, const BSONObj& box, BSONObj filter = BSONObj(),
                     bool uniqueDocs = true)
            : GeoBrowse(g, "box", filter, uniqueDocs) {

            uassert(13063, "$box needs 2 fields (bottomLeft,topRight)", box.nFields() == 2);

            // Initialize an *exact* box from the given obj.
            BSONObjIterator i(box);
            _want._min = Point(i.next());
            _want._max = Point(i.next());

            _wantRegion = _want;
            // Need to make sure we're checking regions within error bounds of where we want
            _wantRegion.fudge(g->getConverter().getError());
            fixBox(g, _wantRegion);
            fixBox(g, _want);

            // XXX: why do we use _g and g, i think they're the same...

            uassert(13064, "need an area > 0 ", _want.area() > 0);

            Point center = _want.center();
            _start = _g->getConverter().hash(center.x, center.y);

            GEODEBUG("center : " << center.toString() << "\t" << _prefix);

            _fudge = _g->getConverter().getError();
            _wantLen = _fudge +
                       std::max((_want._max.x - _want._min.x),
                                 (_want._max.y - _want._min.y)) / 2;

            ok();
        }

        void fixBox(const TwoDIndex* g, Box& box) {
            if(box._min.x > box._max.x)
                swap(box._min.x, box._max.x);
            if(box._min.y > box._max.y)
                swap(box._min.y, box._max.y);

            double gMin = g->getConverter().getMin();
            double gMax = g->getConverter().getMax();

            if(box._min.x < gMin) box._min.x = gMin;
            if(box._min.y < gMin) box._min.y = gMin;
            if(box._max.x > gMax) box._max.x = gMax;
            if(box._max.y > gMax) box._max.y = gMax;
        }

        void swap(double& a, double& b) {
            double swap = a;
            a = b;
            b = swap;
        }

        virtual GeoHash expandStartHash() {
            return _start;
        }

        virtual bool fitsInBox(double width) {
            return width >= _wantLen;
        }

        virtual double intersectsBox(Box& cur) {
            return cur.intersects(_wantRegion);
        }

        virtual KeyResult approxKeyCheck(const Point& p, double& d) {
            if(_want.onBoundary(p, _fudge)) return BORDER;
            else return _want.inside(p, _fudge) ? GOOD : BAD;

        }

        virtual bool exactDocCheck(const Point& p, double& d){
            return _want.inside(p);
        }

        Box _want;
        Box _wantRegion;
        double _wantLen;
        double _fudge;
        GeoHash _start;
    };

    class GeoPolygonBrowse : public GeoBrowse {
    public:
        GeoPolygonBrowse(const TwoDIndex* g, const BSONObj& polyPoints,
                         BSONObj filter = BSONObj(), bool uniqueDocs = true)
            : GeoBrowse(g, "polygon", filter, uniqueDocs) {

            GEODEBUG("In Polygon")

            BSONObjIterator i(polyPoints);
            BSONElement first = i.next();
            _poly.add(Point(first));

            while (i.more()) {
                _poly.add(Point(i.next()));
            }

            uassert(14030, "polygon must be defined by three points or more", _poly.size() >= 3);

            _bounds = _poly.bounds();
            // We need to check regions within the error bounds of these bounds
            _bounds.fudge(g->getConverter().getError()); 
            // We don't need to look anywhere outside the space
            _bounds.truncate(g->getConverter().getMin(), g->getConverter().getMax()); 
            _maxDim = g->getConverter().getError() + _bounds.maxDim() / 2;

            ok();
        }

        // The initial geo hash box for our first expansion
        virtual GeoHash expandStartHash() {
            return _g->getConverter().hash(_bounds.center());
        }

        // Whether the current box width is big enough for our search area
        virtual bool fitsInBox(double width) {
            return _maxDim <= width;
        }

        // Whether the current box overlaps our search area
        virtual double intersectsBox(Box& cur) {
            return cur.intersects(_bounds);
        }

        virtual KeyResult approxKeyCheck(const Point& p, double& d) {
            int in = _poly.contains(p, _g->getConverter().getError());
            if(in == 0) return BORDER;
            else return in > 0 ? GOOD : BAD;
        }

        virtual bool exactDocCheck(const Point& p, double& d){
            return _poly.contains(p);
        }

    private:
        Polygon _poly;
        Box _bounds;
        double _maxDim;
        GeoHash _start;
    };

    // We implement this here because all of the 2d geo code is in this file.
    // It makes it so that index/2d.cpp's implementation of TwoDIndex::newCursor
    // doesn't have to make all of this 2d stuff externally visible in order to work.
    shared_ptr<Cursor> new2DCursor(const TwoDIndex *idx, const string &geo, const BSONObj& query,
                                   const BSONObj& order, int numWanted) {
        if (numWanted < 0)
            numWanted = numWanted * -1;
        else if (numWanted == 0)
            numWanted = 100;

        // false means we want to filter OUT geoFieldsToNuke, not filter to include only that.
        BSONObj filteredQuery = query.filterFieldsUndotted(BSON(geo << ""), false);

        BSONObjIterator i(query);
        while (i.more()) {
            BSONElement e = i.next();

            if (geo != e.fieldName())
                continue;

            if (e.type() == Array) {
                // If we get an array query, assume it is a location, and do a $within { $center :
                // [[x, y], 0] } search
                BSONObj circle = BSON("0" << e.embeddedObjectUserCheck() << "1" << 0);
                shared_ptr<Cursor> c(new GeoCircleBrowse(idx, circle, filteredQuery, "$center", true));
                return c;
            }
            else if (e.type() == Object) {
                // TODO:  Filter out geo : { $special... } field so it doesn't get matched
                // accidentally, if matcher changes

                switch (e.embeddedObject().firstElement().getGtLtOp()) {
                case BSONObj::opNEAR: {
                    BSONObj n = e.embeddedObject();
                    e = n.firstElement();

                    const char* suffix = e.fieldName() + 5; // strlen("$near") == 5;
                    GeoDistType type;
                    if (suffix[0] == '\0') {
                        type = GEO_PLAIN;
                    }
                    else if (strcmp(suffix, "Sphere") == 0) {
                        type = GEO_SPHERE;
                    }
                    else {
                        uassert(13464, string("invalid $near search type: ") + e.fieldName(), false);
                        type = GEO_PLAIN; // prevents uninitialized warning
                    }

                    double maxDistance = numeric_limits<double>::max();
                    if (e.isABSONObj() && e.embeddedObject().nFields() > 2) {
                        BSONObjIterator i(e.embeddedObject());
                        i.next();
                        i.next();
                        BSONElement e = i.next();
                        if (e.isNumber())
                            maxDistance = e.numberDouble();
                    }
                    {
                        BSONElement e = n["$maxDistance"];
                        if (e.isNumber())
                            maxDistance = e.numberDouble();
                    }

                    bool uniqueDocs = false;
                    if(! n["$uniqueDocs"].eoo()) uniqueDocs = n["$uniqueDocs"].trueValue();

                    shared_ptr<GeoSearch> s(new GeoSearch(idx, Point(e), numWanted, filteredQuery,
                                                          maxDistance, type, uniqueDocs));
                    s->exec();
                    shared_ptr<Cursor> c;
                    c.reset(new GeoSearchCursor(s));
                    return c;
                }
                case BSONObj::opWITHIN: {

                    e = e.embeddedObject().firstElement();
                    uassert(13057, "$within has to take an object or array", e.isABSONObj());

                    BSONObj context = e.embeddedObject();
                    e = e.embeddedObject().firstElement();
                    string type = e.fieldName();

                    bool uniqueDocs = true;
                    if (!context["$uniqueDocs"].eoo())
                            uniqueDocs = context["$uniqueDocs"].trueValue();

                    if (startsWith(type,  "$center")) {
                        uassert(13059, "$center has to take an object or array", e.isABSONObj());
                        shared_ptr<Cursor> c(new GeoCircleBrowse(idx, e.embeddedObjectUserCheck(), 
                                                                 filteredQuery, type, uniqueDocs));
                        return c;
                    }
                    else if (type == "$box") {
                        uassert(13065, "$box has to take an object or array", e.isABSONObj());
                        shared_ptr<Cursor> c(new GeoBoxBrowse(idx, e.embeddedObjectUserCheck(),
                                                              filteredQuery, uniqueDocs));
                        return c;
                    }
                    else if (startsWith(type, "$poly")) {
                        uassert(14029, "$polygon has to take an object or array", e.isABSONObj());
                        shared_ptr<Cursor> c(new GeoPolygonBrowse(idx, e.embeddedObjectUserCheck(),
                                                                  filteredQuery, uniqueDocs));
                        return c;
                    }
                    throw UserException(13058, str::stream() << "unknown $within information : "
                                                             << context
                                                             << ", a shape must be specified.");
                }
                default:
                    // Otherwise... assume the object defines a point, and we want to do a
                    // zero-radius $within $center

                    shared_ptr<Cursor> c(new GeoCircleBrowse(idx, BSON("0" << e.embeddedObjectUserCheck() << "1" << 0), filteredQuery));

                    return c;
                }
            }
        }

        throw UserException(13042, (string)"missing geo field (" + geo + ") in : " + query.toString());
    }

    // ------
    // commands
    // ------
    bool run2DGeoNear(const IndexDetails &id, const BSONObj& cmdObj,
                      const GeoNearArguments &parsedArgs, string& errmsg, BSONObjBuilder& result) {
        TwoDIndex * g = dynamic_cast<TwoDIndex*>(const_cast<IndexDetails *>(&id));

        uassert(13046, "'near' param missing/invalid", !cmdObj["near"].eoo());
        const Point n(cmdObj["near"]);
        result.append("near", g->getConverter().hash(cmdObj["near"]).toString());

        double maxDistance = numeric_limits<double>::max();
        if (cmdObj["maxDistance"].isNumber())
            maxDistance = cmdObj["maxDistance"].number();

        GeoDistType type = parsedArgs.isSpherical ? GEO_SPHERE : GEO_PLAIN;

        GeoSearch gs(g, n, parsedArgs.numWanted, parsedArgs.query, maxDistance, type,
                     parsedArgs.uniqueDocs, true);

        if (cmdObj["start"].type() == String) {
            GeoHash start ((string) cmdObj["start"].valuestr());
            gs._start = start;
        }

        gs.exec();

        double totalDistance = 0;

        BSONObjBuilder arr(result.subarrayStart("results"));
        int x = 0;
        for (GeoHopper::Holder::iterator i=gs._points.begin(); i!=gs._points.end(); i++) {

            const GeoPoint& p = *i;
            double dis = parsedArgs.distanceMultiplier * p.distance();
            totalDistance += dis;

            BSONObjBuilder bb(arr.subobjStart(BSONObjBuilder::numStr(x++)));
            bb.append("dis", dis);
            if (parsedArgs.includeLocs) {
                if(p._pt.couldBeArray()) bb.append("loc", BSONArray(p._pt));
                else bb.append("loc", p._pt);
            }
            bb.append("obj", p._o);
            bb.done();

            if (arr.len() > BSONObjMaxUserSize) {
                warning() << "Too many results to fit in single document. Truncating..." << endl;
                break;
            }
        }
        arr.done();

        BSONObjBuilder stats(result.subobjStart("stats"));
        stats.append("time", cc().curop()->elapsedMillis());
        stats.appendNumber("indexlocs", gs._nscanned);
        stats.appendNumber("nscanned", gs._lookedAt);
        stats.appendNumber("objectsLoaded", gs._objectsLoaded);
        stats.append("avgDistance", totalDistance / x);
        stats.append("maxDistance", gs.farthest());
        stats.done();

        return true;
    }

    class GeoWalkCmd : public QueryCommand {
    public:
        GeoWalkCmd() : QueryCommand("geoWalk") {}
        virtual LockType locktype() const { return READ; }
        bool slaveOk() const { return true; }
        bool slaveOverrideOk() const { return true; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseNs(dbname, cmdObj), actions));
        }
        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            string ns = dbname + "." + cmdObj.firstElement().valuestr();

            Collection * cl = getCollection(ns);
            if (! cl) {
                errmsg = "can't find ns";
                return false;
            }

            int geoIdx = -1;
            {
                vector<int> idxs;
                cl->findIndexByType("2d", idxs);
                if (idxs.size() > 1) {
                    errmsg = "2 geo indexes :(";
                    return false;
                }
                geoIdx = idxs[0];
            }

            if (geoIdx < 0) {
                errmsg = "no geo index :(";
                return false;
            }


            IndexDetails& id = cl->idx(geoIdx);
            TwoDIndex *g = dynamic_cast<TwoDIndex *>(const_cast<IndexDetails *>(&id));

            int max = 100000;

            shared_ptr<Cursor> c = Cursor::make(cl, id, BSONObj(), BSONObj(), true, 1);
            while (c->ok() && max--) {
                GeoHash h(c->currKey().firstElement());
                int len;
                cout << "\t" << h.toString()
                     << "\t" << c->current()[g->_geo]
                     << "\t" << hex << h.getHash()
                     << "\t" << hex << ((long long*)c->currKey().firstElement().binData(len))[0]
                     << "\t" << c->current()["_id"]
                     << endl;
                c->advance();
            }

            return true;
        }
    } geoWalkCmd;

}
