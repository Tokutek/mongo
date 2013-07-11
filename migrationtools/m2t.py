#!/usr/bin/env python

# read a mongodb oplog, translate oplog entries to pymongo calls on another mongodb or tokumx database

import sys
import re
import time
from pymongo import MongoClient
import bson

def main():
    ts = None
    fromhost = 'localhost:33000'
    tohost = 'localhost:55000'
    verbose = 0;
    for arg in sys.argv[1:]:
        if arg == '--verbose':
            verbose += 1
            continue
        match = re.match("--ts=(.*):(.*)", arg)
        if match:
            ts = bson.Timestamp(int(match.group(1)), int(match.group(2)))
            if verbose: print("start after", ts)
            continue
        match = re.match("--fromhost=(.*)", arg)
        if match:
            fromhost = match.group(1)
            continue
        match = re.match("--tohost=(.*)", arg)
        if match:
            tohost = match.group(1)
            continue

    # connect to fromhost and tohost
    fromv = fromhost.split(':')
    assert len(fromv) == 2
    fromc = MongoClient(fromv[0], int(fromv[1]))
    tov = tohost.split(':')
    assert len(tov) == 2
    toc = MongoClient(tov[0], int(tov[1]))

    # run a tailable cursor over the from host connection's  oplog from a point in time
    # and replay each oplog entry on the to host connection
    db = fromc.local
    oplog = db.oplog.rs
    while 1:
        if ts is None:
            qs = {}
        else:
            qs = { 'ts': { '$gt': ts }}
        if verbose: print(qs)
        c = oplog.find(qs, tailable=True, await_data=True)
        if verbose: print(c)
        if c.count() == 0:
            time.sleep(1)
        else:
            for oploge in c:
                if verbose: print(oploge)
                op = oploge['op']
                ts = oploge['ts']
                replay(toc, op, oploge, verbose)
    return 0
def replay(toc, op, oploge, verbose):
    if op == 'i':
        ns = oploge['ns'].split('.',1)
        assert len(ns) == 2
        o = oploge['o']
        if verbose: print("insert", ns, o)
        db = toc[ns[0]]
        col = db[ns[1]]
        col.insert(o)
    elif op == 'd':
        ns = oploge['ns'].split('.',1)
        assert len(ns) == 2
        o = oploge['o']
        if verbose: print("delete", ns, o)
        db = toc[ns[0]]
        col = db[ns[1]]
        col.remove(o)
    elif op == 'u':
        ns = oploge['ns'].split('.',1)
        assert len(ns) == 2
        o = oploge['o']
        o2 = oploge['o2']
        if verbose: print("update", ns, o, o2)
        db = toc[ns[0]]
        col = db[ns[1]]
        col.update(o2, o)
    elif op == 'c':
        ns = oploge['ns'].split('.',1)
        assert len(ns) == 2
        assert ns[1] == '$cmd'
        o = oploge['o']
        if verbose: print("command", ns, o)
        db = toc[ns[0]]
        db.command(o)
    elif op == 'n':
        if verbose: print("nop", oploge)
    else:
        print("unknown", oploge)
        assert 0
sys.exit(main())
