#!/bin/bash

pushd $(dirname $0)
  TOP=$PWD/..
popd

mongod=$TOP/mongod
mongo=$TOP/mongo
jstests=$TOP/jstests

pushd $jstests

  for t in *.js
  do
      $mongod --dbpath=data &>/dev/null & p=$!
      $mongo $t &>/dev/null
      if pgrep -f mongod &>/dev/null
      then
          $mongo admin --eval "db.shutdownServer();" &>/dev/null
      else
          echo $t | tee -a crashingtests.txt
      fi
  done

popd
