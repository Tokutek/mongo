#!/usr/bin/env python2

"""tokumxstat.py watches the engineStatus of a TokuMX instance and periodically
prints when any variables changed, and by how much.  It is typically used for
monitoring a system.
"""

import collections
import logging
import re
import sys
import time
import optparse

import pymongo


def convert(v):
    if type(v) == type('str'):
        try:
            v = int(v)
        except:
            v = float(v)
    return v

def printit(stats, es, sleeptime):
    logging.info(time.strftime('%c'))
    od = collections.OrderedDict(sorted(es.items()))
    
    for k, v in od.iteritems():
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
    parser = optparse.OptionParser(usage="usage: %prog [options] <host:port>\n\n" + __doc__)
    parser.add_option("-s", "--sleeptime", dest="sleeptime", default=10,
                      help="duration to sleep between reports [default: %default]", metavar="TIME")
    (opts, args) = parser.parse_args()
    if len(args) > 1:
        parser.error("too many arguments")
    if opts.sleeptime < 1:
        parser.error("invalid --sleeptime: %d" % opts.sleeptime)

    logging.basicConfig(level=logging.INFO)

    host = "localhost:27017"
    if len(args) == 1:
        host = args[0]

    try:
        logging.debug('connecting to %s...', host)
        client = pymongo.MongoClient(host)
        db = client['test']
    except:
        logging.exception('error connecting to %s', host)
        return 1

    logging.info('connected to %s', host)

    stats = {}
    try:
        while 1:
            try:
                es = db.command('engineStatus')
            except:
                logging.exception('error running engineStatus command')
                return 2

            try:
                printit(stats, es, int(opts.sleeptime))
            except:
                logging.exception('error printing info')
                return 3

            time.sleep(int(opts.sleeptime))

    except KeyboardInterrupt:
        logging.info('disconnecting')

    return 0

sys.exit(main())
