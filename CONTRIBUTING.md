Contributing to TokuMX
======================

As a TokuMX user, you can contribute to its growth and stability in several ways.  This document
gives some guidelines for making that process quick, painless, and effective.

Bug Reports
-----------

The easiest way to contribute to TokuMX is to find something that's broken and tell us about it.

### What you'll need ###

You'll need as much of the following information as you can gather.  Don't worry about sending too
much information, the more the better.

1. A detailed description of what you did, what happened, and what you expected to happen.

   Ex: "I started the server, connected with the `mongo` shell, and ran `db.foo.insert({chef:
   'bork'})`.  Then I ran `db.foo.find()` and it printed `{_id:
   ObjectId("505bd76785ebb509fc183733"), chef: 'bork bork'}`.  Why did the string change from
   `'bork'` to `'bork bork'`?

2. A description of all the hardware and operating system details you can easily collect, and the
   TokuMX version you're running.

   Ex: "I'm running TokuMX community edition 1.4.2 (from the Ubuntu packages) on Ubuntu 14.04 on an
   Amazon EC2 m1.xlarge instance with my `--dbpath` on an EBS volume with XFS."

3. Server log files (the `--logpath` option or sometimes from `--syslog`, usually in
   `/var/log/tokumx.log`).  If you're using sharding or replication, make sure you include log files
   from all the servers, including config servers and `mongos` routers.

4. The output of `db.adminCommand('getCmdLineOpts')` on all servers.

5. If your problem is a stall or operations being blocked, the output of `db.currentOp({$all:
   true})` on the affected servers.

6. If your problem is a crash, it's best if you can get a core dump.

7. If your problem is poor performance, the output of `db.serverStatus()` several times during your
   workload, as well as the associated performance you see in your application at those times.

### Where to send it ###

The best place to send bug reports is to the [tokumx-user][tokumx-user] google group.  Once we
figure out what the underlying problem is, we can create issues in the github issue tracker, and
we'll let you know what they are so you can follow development progress.

If you have sensitive data in your log files or for some other reason don't want to send this
information publicly, you can always email our support team directly too at <support@tokutek.com>.

Helping Other Users
-------------------

Helping other TokuMX users is a great way to support the community and to learn some new things
about TokuMX and MongoDB for yourself.  Even just reading other users' questions can be a great
learning experience.

### What you'll need ###

1. Some initial experience with TokuMX and/or basic MongoDB.

2. A little time.

3. An email account or an IRC client (a web browser works fine).

### Where to contribute ###

You can join the [tokumx-user][tokumx-user] google group and start posting right away.

You can also join the [#tokutek][tokutek-irc] IRC channel on Freenode, which includes discussion of
both TokuMX and [TokuDB][tokudb].  If you don't have an IRC client, you can also use Freenode's
[web client][tokutek-irc].

Contributing Code
-----------------

You can also contribute to TokuMX directly, by sending us pull requests.

### What you'll need ###

1. A [github account][github-account].

2. A contributor agreement.  Send mail to <support@tokutek.com>, it's not too complicated.

### Where to send it ###

Contributing is as simple as [forking TokuMX][fork-tokumx], developing your patch, and sending us a
[pull request][pull-request].  We'll work with you to make sure it's correct and well tested and
then we can merge it.

### Branching ###

You will need to know a little bit about our release cycle to do this effectively.

If you want your patch to get in to the next patch release of TokuMX, you'll need to branch from our
[bugfixes/tokumx-1.4][bugfixes-14] branch; this branch is regularly merged into master and the 1.4.x
releases branch.  When you create your pull request, make sure the target branch is our
[bugfixes/tokumx-1.4][bugfixes-14] branch there too.  You can do this on the command line as
follows:

```sh
$ git clone git@github.com:username/mongo
$ cd mongo
$ git remote add upstream https://github.com/Tokutek/mongo
$ git fetch upstream
$ git checkout -b my-feature-branch upstream/bugfixes/tokumx-1.4
$ # commit some changes...
$ git push -u origin my-feature-branch
```

Then you can create your pull request by using a URL like this:
<https://github.com/username/mongo/compare/Tokutek:bugfixes/tokumx-1.4...username:my-feature-branch>.

If you just want your patch to be part of the next minor release (currently that would be 1.5), you
can just use the master branch.

[bugfixes-14]: https://github.com/Tokutek/mongo/tree/bugfixes/tokumx-1.4
[fork-tokumx]: https://github.com/Tokutek/mongo/fork
[github-account]: https://github.com/signup/free
[pull-request]: https://github.com/Tokutek/mongo/compare/
[tokudb]: http://www.tokutek.com/products/tokudb-for-mysql/
[tokumx-user]: https://groups.google.com/forum/#!forum/tokumx-user
[tokutek-irc]: http://webchat.freenode.net/?channels=tokutek
