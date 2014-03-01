#!/usr/bin/env python2

from itertools import imap
import multiprocessing as mp
import os
import sys

try:
    import clang.cindex as ci
except ImportError as e:
    print >> sys.stderr, 'Error importing libclang bindings:', e
    sys.exit(1)

def walk(node, fun, acc):
    fun(node, acc)
    for c in node.get_children():
        walk(c, fun, acc)

def base_class(node):
    for c in node.get_children():
        if c.kind == ci.CursorKind.CXX_BASE_SPECIFIER:
            return c.type.get_declaration()
    return None

def find_root_class(node):
    while True:
        base = base_class(node)
        if base is None:
            return node
        node = base

def text_for(loc):
    with open(loc.file.name, 'r') as f:
        for ln, line in enumerate(f):
            if (ln + 1) == loc.line:
                end = line.find('"', loc.column)
                return line[loc.column:end]

def string_literals(n, names):
    if n.kind == ci.CursorKind.STRING_LITERAL:
        text = text_for(n.location)
        names[text] = True

def find_command_names(n, names):
    if n.kind == ci.CursorKind.CONSTRUCTOR:
        walk(n, string_literals, names)

def collect_command(n, names):
    if n.kind == ci.CursorKind.CLASS_DECL:
        if find_root_class(n).spelling == 'Command':
            walk(n, find_command_names, names)

index = ci.Index.create()

include_dirs = [
    'src',
    'src/mongo',
    'src/mongo/client',
    'src/mongo/db',
    'src/mongo/s'
]

parse_args = ['-I%s' % d for d in include_dirs]

def parse_file(tup):
    f, names = tup
    print >> sys.stderr, 'Processing', f
    tu = index.parse(f, args=parse_args)
    walk(tu.cursor, collect_command, names)

def main(argv):
    manager = mp.Manager()
    all_command_names = manager.dict()
    pool = mp.Pool()

    for root, dirs, files in os.walk('src/mongo'):
        cpp_files = filter(lambda f: f.endswith('.cpp'), files)
        pool.map(parse_file, imap(lambda f: (os.path.join(root, f), all_command_names), cpp_files))

    names = sorted(all_command_names.keys())
    for name in names:
        print name

if __name__ == '__main__':
    main(sys.argv)
