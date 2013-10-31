#!/usr/bin/env python2

import sys
import time
import re
import collections
import pymongo
from pymongo import MongoClient



def usage():
    print "diff the tokumx engine status"
    print "--host=HOSTNAME (default: localhost)"
    print "--port=PORT (default: 27017)"
    print "--sleeptime=SLEEPTIME (default: 10 seconds)"

    return 1

def convert(v):
    if type(v) == type('str'):
        try:
            v = int(v)
        except:
            v = float(v)
    return v

def printit(stats, es, sleeptime):
    # print es

    od = collections.OrderedDict(sorted(es.items()))
    
    for k, v in od.iteritems():
        #print k, v
    
        try:
            v = convert(v)
        except:
            pass
        if stats.has_key(k):
            oldv = stats[k]
            if v != oldv:
                print k, "|", oldv, "|", v,
                try:
                    d = v - oldv
                    if sleeptime != 1:
                        if d >= sleeptime:
                            e = d / sleeptime
                        else:
                            e = float(d) / sleeptime
                        print "|", d, "|", e
                    else:
                        print "|", d
                except:
                    print
        stats[k] = v
    print

def main():
    host = 'localhost'
    port = 27017
    sleeptime = 10

    for a in sys.argv[1:]:
        if a == "-h" or a == "-?" or a == "--help":
            return usage()
        match = re.match("--(.*)=(.*)", a)
        if match:
            exec "%s='%s'" % (match.group(1),match.group(2))
            continue
        return usage()

    connect_parameters = {}
    if host is not None:
        if host[0] == '/':
            connect_parameters['unix_socket'] = host
        else:
            connect_parameters['host'] = host
            if port is not None:
                connect_parameters['port'] = int(port)

    try:
        client = MongoClient(host,port)
        db = client['test']
    except:
        print sys.exc_info()
        return 1

    print "connected"

    stats = {}
    while 1:
        try:
            es = db.command('engineStatus')
        except:
            print "db", sys.exc_info()
            return 2

        try:
            printit(stats, es, int(sleeptime))
        except:
            print "printit", sys.exc_info()
            return 3

        time.sleep(int(sleeptime))

    return 0

sys.exit(main())
