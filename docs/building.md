Building TokuMX
===============

Starting in TokuMX 1.4.0, TokuMX is built entirely with [CMake](http://www.cmake.org).  For help building earlier versions of TokuMX, see below.

Building with CMake
-------------------

You will need:

* git
* cmake >= 2.8.10
* zlib development files
* gcc and g++ >= 4.7

To begin, clone [Tokutek/ft-index](http://github.com/Tokutek/ft-index), [Tokutek/jemalloc](http://github.com/Tokutek/jemalloc) and [Tokutek/backup-community](http://github.com/Tokutek/backup-community), as well as this repo:

    $ git clone https://github.com/Tokutek/mongo
    $ git clone https://github.com/Tokutek/ft-index
    $ git clone https://github.com/Tokutek/jemalloc
    $ git clone https://github.com/Tokutek/backup-community

If you are building TokuMX, you should probably be building from a tagged version.  Most git branches are expected to be in flux and possibly unstable.  So check out the most recent stable tag for each repo, for example `tokumx-1.4.0`:

    $ (cd mongo; git checkout tokumx-1.4.0)
    $ (cd ft-index; git checkout tokumx-1.4.0)
    $ (cd jemalloc; git checkout tokumx-1.4.0)
    $ (cd backup-community; git checkout tokumx-1.4.0)

Next, add symlinks in `src/third_party`, and make a directory in which you would like to build:

    $ ln -snf ../../jemalloc ft-index/third_party/jemalloc
    $ cd mongo
    $ ln -snf ../../../ft-index src/third_party/ft-index
    $ ln -snf ../../../backup-community/backup src/third_party/backup
    $ mkdir build
    $ cd build

Next, configure the build directory _either_ for a release build:

    $ cmake -D CMAKE_BUILD_TYPE=Release -D TOKU_DEBUG_PARANOID=OFF -D USE_VALGRIND=OFF -D USE_BDB=OFF -D BUILD_TESTING=OFF -D TOKUMX_DISTNAME=1.4.0 ..
    ...
    -- Building TokuMX Community 1.4.0
    -- Package name: tokumx-1.4.0-linux-x86_64
    -- Configuring done
    -- Generating done

_or_ for a debug build:

    $ cmake -D CMAKE_BUILD_TYPE=Debug -D TOKU_DEBUG_PARANOID=ON -D USE_VALGRIND=OFF -D USE_BDB=OFF -D BUILD_TESTING=OFF -D TOKUMX_DISTNAME=1.4.0 ..

Finally, build the tarballs:

    $ make -j4 package


Earlier (1.3.3 and below) versions of TokuMX:
---------------------------------------------

### TOKUKV

To build TokuMX, you must first build TokuKV and install it to src/third_party/tokukv (if you have TokuKV installed elsewhere, you can set the environment variable `TOKUKV_PATH` when running scons).

For instructions on building TokuKV, see [Tokutek/ft-index](http://github.com/Tokutek/ft-index).  Make sure you configure with `-D CMAKE_INSTALL_PREFIX=/path/to/mongo/src/third_party/tokukv`.

### SCONS

For detail information about building, please see [the manual](http://dochub.mongodb.org/core/building).

If you want to build everything (mongod, mongo, tools, etc):

    $ scons all

If you only want to build the database:

    $ scons

To install

    $ scons --prefix=/opt/mongo install

Please note that prebuilt binaries are available at [tokutek.com](http://www.tokutek.com/) and may be the easiest way to get started.

### SCONS TARGETS

* mongod
* mongos
* mongo
* mongoclient
* all

### COMPILER VERSIONS

Mongo has been tested with GCC 4.x and Visual Studio 2008 and 2010.  Older versions
of GCC may not be happy.

### WINDOWS

TokuMX is not supported on Windows, because TokuKV is not supported on Windows.

### UBUNTU

To install dependencies on Ubuntu systems:

    # aptitude install scons build-essential
    # aptitude install libboost-filesystem-dev libboost-program-options-dev libboost-system-dev libboost-thread-dev

To run tests as well, you will need PyMongo:

    # aptitude install python-pymongo

Then build as usual with `scons`:

    $ scons all


### OS X

Try [Homebrew](http://mxcl.github.com/homebrew/):

    $ brew install mongodb


### FREEBSD

Install the following ports:

  * devel/boost
  * devel/libexecinfo
  * devel/pcre
  * lang/spidermonkey


### Special Build Notes

  * [debian etch on ec2](building.debian.etch.ec2.html)
  * [open solaris on ec2](building.opensolaris.ec2.html)

