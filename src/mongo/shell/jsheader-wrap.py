#!/usr/bin/python2

import sys

jsheader_path = sys.argv[1]
sys.path.append(jsheader_path)
from jsheader import jsToH

target = sys.argv[2]
sources = sys.argv[3:]

jsToH([target], sources, None)
