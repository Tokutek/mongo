// progress_meter.h

/*    Copyright 2009 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#pragma once

#include "mongo/util/goodies.h"
#include <boost/noncopyable.hpp>

#include <string>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/util/builder.h"

namespace mongo {

    class ProgressMeter : boost::noncopyable {
    public:
        ProgressMeter(unsigned long long total,
                      int secondsBetween = 3,
                      int checkInterval = 100,
                      std::string units = "",
                      std::string name = "Progress",
                      const ProgressMeter *parent = NULL)
                : _showTotal(true),
                  _units(units),
                  _parent(parent) {
            reset( total , secondsBetween , checkInterval );
            _name = name.c_str();
        }

        ProgressMeter() : _active(0), _showTotal(true), _units(""), _parent(NULL) {
            _name = "Progress";
        }

        // typically you do ProgressMeterHolder
        void reset( unsigned long long total , int secondsBetween = 3 , int checkInterval = 100 );

        void finished() { _active = 0; }
        bool isActive() const { return _active; }

        /**
         * @param n how far along we are relative to the total # we set in CurOp::setMessage
         * @return if row was printed
         */
        bool hit( int n = 1 );

        void setUnits( const std::string& units ) { _units = units; }
        std::string getUnit() const { return _units; }

        void setName(std::string name) { _name = name.c_str(); }
        std::string getName() const { return _name.toString(); }

        void setTotalWhileRunning( unsigned long long total ) {
            _total = total;
        }

        unsigned long long done() const { return _done; }

        unsigned long long hits() const { return _hits; }

        unsigned long long total() const { return _total; }

        void showTotal(bool doShow) {
            _showTotal = doShow;
        }

        std::string toString() const;

        void treeString(std::stringstream &ss) const;
        std::string treeString() const;

        void treeObj(BSONObjBuilder &b) const;
        BSONObj treeObj() const;

        bool operator==( const ProgressMeter& other ) const { return this == &other; }

    private:

        // This will return a BufBuilder where the child of this tree can put its information.
        BufBuilder& treeObjForSubtree(BSONObjBuilder &b) const;
        void appendInfo(BSONObjBuilder &b) const;

        bool _active;

        unsigned long long _total;
        bool _showTotal;
        int _secondsBetween;
        int _checkInterval;

        unsigned long long _done;
        unsigned long long _hits;
        int _lastTime;

        std::string _units;
        ThreadSafeString _name;
        const ProgressMeter *_parent;
    };

    // e.g.: 
    // CurOp * op = cc().curop();
    // ProgressMeterHolder pm(op->setMessage("index: (1/3) external sort", "Index: External Sort Progress", d->stats.nrecords, 10));
    // loop { pm.hit(); }
    class ProgressMeterHolder : boost::noncopyable {
    public:
        ProgressMeterHolder( ProgressMeter& pm )
            : _pm( pm ) {
        }

        ~ProgressMeterHolder() {
            _pm.finished();
        }

        ProgressMeter* operator->() { return &_pm; }
        
        bool hit( int n = 1 ) { return _pm.hit( n ); }

        void finished() { _pm.finished(); }

        bool operator==( const ProgressMeter& other ) { return _pm == other; }

    private:
        ProgressMeter& _pm;
    };


}
