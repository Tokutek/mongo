#!/bin/bash

tool="valgrind"
for arg in $@; do 
    if [ $arg = '--tool=drd' ] ; then
        tool="drd"
    fi
done

(valgrind --suppressions=${tool}.suppressions --soname-synonyms=somalloc=NONE "$@" 2>&1 | tee __valgrind.log) &
pid=$!
function cleanup {
    # send sigint to valgrind while it's still alive
    while [ $(ps aux | grep $pid | grep valgrind | wc -l) -gt 0 ] ; do
        kill -SIGINT $pid &>/dev/null
        sleep 1;
    done
    sleep 1;
    # now strip out the useless warnings about big allocations
    # and produce the final logfile, valgrind.log
    #
    # note that cleanup can get called more than once so we check
    # for the existence of __valgrind.log before doing anything
    if [ -e __valgrind.log ] ; then
        grep -v "Warning: set address range perms" < __valgrind.log > valgrind.log
        rm -f __valgrind.log
    fi
}

# wait for valgrind, trap interrupt signals to cleanup
trap "cleanup" SIGINT SIGTERM
wait $pid
# if valgrind exits on its own, we still need to cleanup
cleanup
