TokuMX
======

TokuMX is a high-performance distribution of MongoDB, a document-oriented
database with built-in sharding and replication, built on Tokutek's
[Fractal Tree indexes][ft-index].

TokuMX has the same binaries, supports the same drivers, data model, and
features of MongoDB, because it shares much of its code with MongoDB.  The
benefits of using TokuMX include:

 * [Faster writes][iibench] and less I/O on large data sets.
 * [Concurrent read/write access][sysbench] to collections with
   document-level locking.
 * [Compression][compression] of all data by default.
 * [MVCC][transactions] transactional semantics.
 * Zero fragmentation of data or indexes, removing the
   need for maintenance like `compact` or `repairDatabase`.

TokuMX is fully compatible with MongoDB 2.4, except that TokuMX does not
support:

 * Text indexes and search
 * Geospatial indexing and queries

[ft-index]: https://github.com/Tokutek/ft-index
[iibench]: http://www.tokutek.com/resources/benchmark-results/tokumx-benchmark-hdd/#iiBench
[sysbench]: http://www.tokutek.com/resources/benchmark-results/tokumx-benchmark-hdd/#sysbench
[compression]: http://www.tokutek.com/2013/02/mongodb-fractal-tree-indexes-high-compression/
[transactions]: http://www.tokutek.com/2013/10/introducing-tokumx-transactions-for-mongodb-applications/


Documentation
-------------

For the most part, the [MongoDB documentation][mongo-docs] applies to
TokuMX as well.  For exceptions, additional features, and TokuMX specific
strategies and behaviors, the [TokuMX Users Guide][users-guide] takes
precedence.

[mongo-docs]: http://docs.mongodb.org/
[users-guide]: http://www.tokutek.com/resources/product-docs/


Packaging
---------

TokuMX is available for 64-bit Linux systems.  Tokutek builds and provides
packages for Centos 5 and 6, Debian 7, Fedora 20, and Ubuntu 12.04, 12.10,
13.04, 13.10, and 14.04.  For other distributions of Linux, Tokutek
provides a binary tarball that can be installed standalone.

Community downloads can be found [here][community].

Enterprise downloads, including the hot backup utility, can be found
[here][enterprise] and come with 30 days of free support for evaluation.

Installation instructions for Linux distributions and OSX can be found
[here][install-docs].

[community]: http://www.tokutek.com/products/downloads/tokumx-ce-downloads/
[enterprise]: http://www.tokutek.com/products/downloads/tokumx-ee-downloads/
[install-docs]: https://github.com/Tokutek/mongo/wiki/Installing-TokuMX


Migrating from MongoDB
----------------------

TokuMX stores data on disk completely differently from MongoDB.  Therefore
it is necessary to export any existing data from MongoDB and import it
into TokuMX.  Depending on your existing MongoDB database and your
application's availability requirements, you can choose from a number of
strategies for converting your data set to TokuMX.

 * Single server
 * Replica set
   - Offline (with downtime)
   - Online (with no downtime)
 * Sharded cluster
   - Offline, individual shards
   - Offline, all data at once
   - Online

For details of how to perform these types of data migrations, see the
[documentation][migrating-docs].

[migrating-docs]: https://github.com/Tokutek/mongo/wiki/Migrating-from-MongoDB


Building TokuMX
---------------

See
[docs/building.md](https://github.com/Tokutek/mongo/blob/master/docs/building.md).


Drivers
-------

All [MongoDB drivers][drivers] work seamlessly with TokuMX.

[drivers]: http://docs.mongodb.org/ecosystem/drivers/


Replication
-----------

Replication in TokuMX is managed similarly to MongoDB and uses much of the
same terminology, administrative commands, and election mechanisms, and
[write concern][write-concern] works the same.  There are some significant
improvements to replication performance in TokuMX, including significantly
reduced I/O load due to writes on secondaries.

The details of the replication oplog are different in TokuMX.
Applications that read the oplog and rely on the format its contents will
need to change.  Therefore, "mixed" replica sets containing MongoDB and
TokuMX nodes do not work.

[write-concern]: http://docs.mongodb.org/manual/core/write-concern/


Sharding
--------

Sharding in TokuMX is managed similarly to MongoDB and uses much of the
same terminology and administrative commands.  There are some significant
improvements to sharding, including significantly less intrusive chunk
migrations, and clustering shard keys that make range queries much faster.

The internal mechanisms used to migrate chunks between shards are
different in TokuMX from MongoDB, and therefore "mixed" clusters
containing MongoDB and TokuMX shards do not work.

Multi-statement transactions (`beginTransaction`, `rollbackTransaction`,
and `commitTransaction` commands) do not work through `mongos`.


Monitoring
----------

[MongoDB Management Service][mms] monitoring partially works with TokuMX.
Some monitored statistics work normally (e.g. opcounters), and some report
things that are MongoDB-specific, and therefore report nothing for TokuMX
(e.g. btree misses).  MMS can be used but is an incomplete solution.

Other on-site and hosted solutions can be used.  `db.serverStatus()`
reports TokuMX equivalent metrics for many commonly-tracked MongoDB
metrics, and `db.engineStatus()` has an exhaustive list of many metrics
that can be tracked.

[mms]: https://www.mongodb.com/products/mongodb-management-service


Support
-------

Enterprise support subscriptions are available from [Tokutek][support].
This includes an enterprise hot backup tool.

[support]: http://www.tokutek.com/subscriptions/


Community
---------

Mailing lists:

 * [tokumx-user@googlegroups.com][tokumx-user] for news and help running a
   TokuMX database and developing applications using TokuMX.
 * [tokumx-dev@googlegroups.com][tokumx-dev] for discussion about
   developing TokuMX itself.

IRC:

 * [irc.freenode.net/tokutek][irc] for discussion about TokuMX and TokuDB.

Issue tracker:

 * [TokuMX JIRA][jira-mx] for problems with TokuMX.
 * [TokuFT JIRA][jira-ft] for problems with [TokuFT][ft-index].

[tokumx-user]: https://groups.google.com/forum/#!forum/tokumx-user
[tokumx-dev]: https://groups.google.com/forum/#!forum/tokumx-dev
[irc]: http://webchat.freenode.net/?channels=tokutek
[jira-mx]: https://tokutek.atlassian.net/browse/MX/
[jira-ft]: https://tokutek.atlassian.net/browse/FT/


License
-------

Most TokuMX source files are made available under the terms of the GNU
Affero General Public License (AGPL).  See individual files for details.

As an exception, the files in the client/, debian/, rpm/,
utils/mongoutils, and all subdirectories thereof are made available under
the terms of the Apache License, version 2.0.

The TokuKV Fractal Tree Indexing library is made available under the terms
of the GNU General Public License (GPL) version 2, with an additional
grant of a patent license.  See [README-TOKUKV][README-TOKUDB] for details.

[README-TOKUDB]: https://github.com/Tokutek/ft-index/blob/master/README-TOKUDB
