#!/usr/bin/python2

import sys
js2c_dir = sys.argv[1]
sys.path.append(js2c_dir)
import js2c

srcs = sys.argv[2]
natives = sys.argv[3].split(',')
type = sys.argv[4]
compression = sys.argv[5]

js2c.JS2C(natives, [srcs], {'TYPE': type, 'COMPRESSION': compression})
