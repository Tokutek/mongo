// progress_meter.cpp

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

#include "mongo/util/progress_meter.h"

#include "mongo/pch.h"

#include <iomanip>

#include "mongo/db/jsobj.h"
#include "mongo/util/log.h"

using namespace std;

namespace mongo {

    void ProgressMeter::reset( unsigned long long total , int secondsBetween , int checkInterval) {
        _total = total;
        _secondsBetween = secondsBetween;
        _checkInterval = checkInterval;
        
        _done = 0;
        _hits = 0;
        _lastTime = (int)time(0);
        
        _active = 1;
    }


    bool ProgressMeter::hit( int n ) {
        if ( ! _active ) {
            warning() << "hit an inactive ProgressMeter" << endl;
            return false;
        }
        
        _done += n;
        _hits++;
        if ( _hits % _checkInterval )
            return false;
        
        int t = (int) time(0);
        if ( t - _lastTime < _secondsBetween )
            return false;
        
        if ( _total > 0 ) {
            int per = (int)( ( (double)_done * 100.0 ) / (double)_total );
            LogstreamBuilder out = log();
            if (_parent != NULL) {
                out << "\t\t" << treeString() << endl;
            } else {
                out << "\t\t" << _name << ": " << _done;
            
                if (_showTotal) {
                    out << '/' << _total << '\t' << per << '%';
                }

                if ( ! _units.empty() ) {
                    out << "\t(" << _units << ")";
                }
            
                out << endl;
            }
        }
        _lastTime = t;
        return true;
    }
    
    std::string ProgressMeter::toString() const {
        if ( ! _active )
            return "";
        std::stringstream buf;
        buf << _name << ": " << _done << '/' << _total << ' ' << (_done*100)/_total << '%';
        
        if ( ! _units.empty() ) {
            buf << "\t(" << _units << ")" << endl;
        }
        
        return buf.str();
    }

    void ProgressMeter::treeString(std::stringstream &ss) const {
        if (_parent == NULL) {
            ss << _name << ": ";
        } else {
            _parent->treeString(ss);
            ss << ", ";
        }
        ss << _done << "/" << _total << " "
           << std::fixed << std::setprecision(1) << (_done*100.0)/_total << "%";
        if (!_units.empty()) {
            ss << " " << _units;
        }
    }

    std::string ProgressMeter::treeString() const {
        std::stringstream ss;
        treeString(ss);
        return ss.str();
    }

    void ProgressMeter::appendInfo(BSONObjBuilder &b) const {
        b.append("name", getName());
        b.append("units", _units);
        b.append("done", _done);
        b.append("total", _total);
    }

    BufBuilder& ProgressMeter::treeObjForSubtree(BSONObjBuilder &b) const {
        if (_parent != NULL) {
            BSONObjBuilder sub(_parent->treeObjForSubtree(b));
            appendInfo(sub);
            return sub.subobjStart("child");
        } else {
            appendInfo(b);
            return b.subobjStart("child");
        }
    }

    void ProgressMeter::treeObj(BSONObjBuilder &b) const {
        if (_parent != NULL) {
            BSONObjBuilder sub(_parent->treeObjForSubtree(b));
            appendInfo(sub);
        } else {
            appendInfo(b);
        }
    }

    BSONObj ProgressMeter::treeObj() const {
        BSONObjBuilder b;
        treeObj(b);
        return b.obj();
    }

}
