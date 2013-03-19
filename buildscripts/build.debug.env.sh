#!/usr/bin/env bash

set -e
set -u

branch="master"

function usage() {
    echo "build.debug.env.sh - build mongo dev environment"
    echo "[--branch=$branch]"
    return 1
}

while [ $# -gt 0 ] ; do
    arg=$1; shift
    if [[ $arg =~ --(.*)=(.*) ]] ; then
        eval ${BASH_REMATCH[1]}=${BASH_REMATCH[2]}
    else
        usage; exit 1;
    fi
done

BUILD_DIR=$PWD

echo "Checking out MongoDB"
git clone git@github.com:Tokutek/mongo.git
pushd mongo
    git checkout $branch
    mkdir data
popd

echo "Checking out TokuDB"
svn co -q https://svn.tokutek.com/tokudb/toku/tokudb
echo "Checking out jemalloc"
svn co -q https://svn.tokutek.com/tokudb/jemalloc-3.3.0
echo "Checking out lzma"
svn co -q https://svn.tokutek.com/tokudb/xz-4.999.9beta

mkdir tokudb/dbg
pushd tokudb/dbg
  CC=gcc47 CXX=g++47 cmake \
      -D CMAKE_INSTALL_PREFIX=$BUILD_DIR/mongo/src/third_party/tokudb \
      -D CMAKE_BUILD_TYPE=Debug \
      -D BUILD_TESTING=OFF \
      -D USE_BDB=OFF \
      -D TOKU_SVNROOT=$BUILD_DIR \
      ..
  cmake --build . --target install
popd

pushd $BUILD_DIR/mongo
  scons --dd -j5
popd

echo "MongoDB built in $BUILD_DIR/mongo."
