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
git clone git@github.com:Tokutek/ft-index.git
pushd ft-index
    git clone git@github.com:Tokutek/jemalloc.git third_party/jemalloc
popd

echo "Checking out backup"
git clone git@github.com:Tokutek/backup-community.get

mkdir ft-index/dbg
pushd ft-index/dbg
  CC=gcc47 CXX=g++47 cmake \
      -D CMAKE_INSTALL_PREFIX=$BUILD_DIR/mongo/src/third_party/tokukv \
      -D CMAKE_BUILD_TYPE=Debug \
      -D BUILD_TESTING=OFF \
      -D USE_BDB=OFF \
      ..
  cmake --build . --target install
popd

mkdir backup-community/dbg
pushd backup-community/dbg
  CC=gcc47 CXX=g++47 cmake \
      -D CMAKE_INSTALL_PREFIX=$BUILD_DIR/mongo/src/third_party/tokubackup \
      -D CMAKE_BUILD_TYPE=Debug \
      -D HOT_BACKUP_LIBNAME=HotBackup \
      -D BUILD_TESTING=OFF \
      ..
  cmake --build . --target install
popd

pushd $BUILD_DIR/mongo
  scons --dd -j5
popd

echo "MongoDB built in $BUILD_DIR/mongo."
