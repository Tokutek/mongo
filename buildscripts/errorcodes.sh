#!/bin/sh
# Redirect output to stderr.
exec 1>&2

output=$(python2 -c 'import sys; import buildscripts.errorcodes as ec; sys.exit(0 if ec.checkErrorCodes() else 1)')

if [ x"${output}" = x"" ]; then
    exit 0
else
    echo "${output}"
    exit 1
fi

