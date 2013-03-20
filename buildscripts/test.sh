#!/usr/bin/env bash
# dummy comment to test
set -e
set -u

function usage() {
    echo 1>&2 "test.sh"
    echo 1>&2 "[--source_tarball=$source_tarball] [--binary_tarball=$binary_tarball]"
    echo 1>&2 "[--commit=$commit]"
    return 1
}

function retry() {
    local cmd
    local retries
    local exitcode
    cmd=$*
    let retries=0
    while [ $retries -le 10 ] ; do
        echo `date` $cmd
        set +e
        bash -c "$cmd"
        exitcode=$?
        set -e
        echo `date` $cmd $exitcode $retries
        let retries=retries+1
        if [ $exitcode -eq 0 ] ; then break; fi
        sleep 10
    done
    test $exitcode = 0
}

function runsuite() {
    local python=python
    if command -v python2.7 &>/dev/null; then
        python=python2.7
    elif command -v python26 &>/dev/null; then
        python=python26
    elif command -v python2 &>/dev/null; then
        python=python2
    fi
    if ! $python <<EOF ; then
import sys
try:
    import pymongo
    sys.exit(0)
except ImportError:
    sys.exit(1)
EOF
        if [ -d /usr/lib64/python2.4/site-packages/pymongo/ ]; then
            PYTHONPATH=/usr/lib64/python2.4/site-packages/:$PYTHONPATH
            export PYTHONPATH
        fi
    fi
    local dir=$1
    local suite=$2
    mkdir -p $dir/smokedata-$suite
    set +e
    # smoke.py (by way of cleanbb.py) will kill anything running in the test directory (where we extracted)
    # so if this bash script is running with that PWD, smoke.py will kill us
    # but it won't kill itself, so we just make sure that *only* it runs in that directory and we're safe
    (cd $dir ; \
        exec $python buildscripts/smoke.py \
            --with-cleanbb \
            --continue-on-failure \
            --smoke-db-prefix="smokedata-$suite" \
            --quiet \
            $suite
        )
    set -e
}

# checkout the mongodb tests and run them against a mongodb tarball
function test_mongodb() {
    extracted=$(basename ${source_tarball%.tar.gz})
    if [ -d $extracted ] ; then
        echo 1>&2 "directory $extracted already exists, won't overwrite.  Exiting..."
        exit 1
    fi
    tar --extract \
        --gzip \
        --file $origdir/$source_tarball
    test -d $extracted
    wildcardopt=''
    if [ $(uname -s) != "Darwin" ] ; then
        wildcardopt=--wildcards
    fi
    tar --extract \
        --gzip \
        --strip-components 2 \
        --directory $extracted \
        --file $origdir/$binary_tarball \
        $wildcardopt '*/bin/'
    for suite in js aggregation #dur parallel perf tool
    do
        runsuite $extracted $suite
    done
}

PATH=$HOME/bin:$PATH

binary_tarball=''
source_tarball=''
svnserver=https://svn.tokutek.com/tokudb
basedir=$HOME/svn.build
builddir=$basedir/tokumon.build
commit=0

while [ $# -gt 0 ] ; do
    arg=$1; shift
    if [[ $arg =~ --(.*)=(.*) ]] ; then
        eval ${BASH_REMATCH[1]}=${BASH_REMATCH[2]}
    else
        usage; exit 1;
    fi
done

if [ ! -f $source_tarball ] ; then
    echo "Need tarball containing tests"
    exit 1
fi
if [ ! -f $binary_tarball ] ; then
    echo "Need tarball to test"
    exit 1
fi

origdir=$PWD

# update the build directory
if [ ! -d $basedir ] ; then mkdir $basedir ; fi

pushd $basedir
if [ $? != 0 ] ; then exit 1; fi

if [ ! -d $builddir ] ; then mkdir $builddir; fi

date=`date +%Y%m%d`
testresultsdir=$builddir/$date
pushd $builddir
if [ $? = 0 ] ; then
    set +e
    while [ ! -d $date ] ; do
        svn mkdir $svnserver/tokumon.build/$date -m ""
        svn checkout -q $svnserver/tokumon.build/$date
        if [ $? -ne 0 ] ; then rm -rf $date; fi
    done
    set -e
    popd
fi

tracefile=tokumon-test-$(basename ${binary_tarball%.tar.gz})
echo >$testresultsdir/$tracefile

test_mongodb 2>&1 | tee -a $testresultsdir/$tracefile

set +e
let tests_failed=0
let tests_passed=0
while read line ; do
    if [[ "$line" =~ ([0-9]+)\ tests\ succeeded ]] ; then
        let tests_passed=tests_passed+${BASH_REMATCH[1]}
    elif [[ "$line" =~ '[_a-zA-Z][_a-zA-Z0-9]*\.js : fail' ]] ; then
        let tests_failed=tests_failed+1
    fi
done <$testresultsdir/$tracefile

if [ $tests_failed = 0 ] ; then
    testresult="PASS=$tests_passed"
else
    testresult="FAIL=$tests_failed PASS=$tests_passed"
fi
pushd $testresultsdir
if [ $? = 0 ] ; then
    if [ $commit != 0 ] ; then
        svn add $tracefile
        retry svn commit -m \"$testresult tokumon-test $(basename ${binary_tarball%.tar.gz})\" $tracefile
    fi
    popd
fi

popd

exit 0
