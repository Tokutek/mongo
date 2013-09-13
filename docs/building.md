
Building TokuMX
===============

TOKUKV
---------------

To build TokuMX, you must first build TokuKV and install it to src/third_party/tokukv (if you have TokuKV installed elsewhere, you can set the environment variable `TOKUKV_PATH` when running scons).

For instructions on building TokuKV, see [Tokutek/ft-index](http://github.com/Tokutek/ft-index).  Make sure you configure with `-D CMAKE_INSTALL_PREFIX=/path/to/mongo/src/third_party/tokukv`.

SCONS
---------------

For detail information about building, please see [the manual](http://dochub.mongodb.org/core/building).

If you want to build everything (mongod, mongo, tools, etc):

    $ scons .

If you only want to build the database:

    $ scons

To install

    $ scons --prefix=/opt/mongo install

Please note that prebuilt binaries are available at [tokutek.com](http://www.tokutek.com/) and may be the easiest way to get started.

SCONS TARGETS
--------------

* mongod
* mongos
* mongo
* mongoclient
* all

COMPILER VERSIONS
--------------

Mongo has been tested with GCC 4.x and Visual Studio 2008 and 2010.  Older versions
of GCC may not be happy.

WINDOWS
--------------

TokuMX is not supported on Windows, because TokuKV is not supported on Windows.

UBUNTU
--------------

To install dependencies on Ubuntu systems:

    # aptitude install scons build-essential
    # aptitude install libboost-filesystem-dev libboost-program-options-dev libboost-system-dev libboost-thread-dev

To run tests as well, you will need PyMongo:

    # aptitude install python-pymongo

Then build as usual with `scons`:

    $ scons all


OS X
--------------

Try [Homebrew](http://mxcl.github.com/homebrew/):

    $ brew install mongodb


FREEBSD
--------------

Install the following ports:

  * devel/boost
  * devel/libexecinfo
  * devel/pcre
  * lang/spidermonkey


Special Build Notes
--------------
  * [debian etch on ec2](building.debian.etch.ec2.html)
  * [open solaris on ec2](building.opensolaris.ec2.html)

