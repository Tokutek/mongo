Name: tokumx
Conflicts: mongo, mongo-10gen, mongo-10gen-unstable, mongo-stable
Requires: tokumx-libs
Version: 1.3.3
Release: 1%{?dist}
Summary: tokumx client shell and tools
License: AGPLv3 and GPLv2
Vendor: Tokutek, Inc.
URL: http://www.tokutek.com/products/tokumx-for-mongodb
Group: Applications/Databases

Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description
Mongo (from "huMONGOus") is a schema-free document-oriented database.
It features dynamic profileable queries, full indexing, replication
and fail-over support, efficient storage of large binary data objects,
and auto-sharding.

This package provides the mongo shell, import/export tools, and other
client utilities.

%package libs
Summary: tokumx shared libraries
Group: Applications/Databases

%description libs
Mongo (from "huMONGOus") is a schema-free document-oriented database.

This package provides the libraries shared among the server and tools,
including a portability layer and the Fractal Tree indexing library.

%package server
Summary: tokumx server, sharding server, and support scripts
Group: Applications/Databases
Requires: tokumx-libs

%description server
Mongo (from "huMONGOus") is a schema-free document-oriented database.

This package provides the mongo server software, mongo sharding server
softwware, default configuration files, and init.d scripts.

%package devel
Summary: Headers and libraries for mongo development. 
Group: Applications/Databases

%description devel
Mongo (from "huMONGOus") is a schema-free document-oriented database.

This package provides the mongo static library and header files needed
to develop mongo client software.

%prep
%setup

%build
mkdir -p opt
(cd opt; \
  cmake \
    -D CMAKE_BUILD_TYPE=Release \
    -D TOKU_DEBUG_PARANOID=OFF \
    -D USE_VALGRIND=OFF \
    -D USE_CTAGS=OFF \
    -D USE_ETAGS=OFF \
    -D USE_GTAGS=OFF \
    -D USE_CSCOPE=OFF \
    -D USE_BDB=OFF \
    -D USE_SYSTEM_BOOST=OFF \
    -D USE_SYSTEM_PCRE=ON \
    -D TOKUMX_STRIP_BINARIES=OFF \
    -D TOKUMX_SET_RPATH=OFF \
    -D CMAKE_INSTALL_PREFIX=$RPM_BUILD_ROOT/usr \
    -D BUILD_TESTING=OFF \
    -D INSTALL_LIBDIR=lib64/%{name} \
    -D CMAKE_INSTALL_RPATH=/usr/lib64/%{name} \
    ..)
make -C opt %{?_smp_mflags}

%install
(cd opt; \
  cmake -D COMPONENT=tokumx_server -P cmake_install.cmake && \
  cmake -D COMPONENT=tokumx_misc -P cmake_install.cmake && \
  cmake -D COMPONENT=tokumx_tools -P cmake_install.cmake && \
  cmake -D COMPONENT=tokumx_plugins -P cmake_install.cmake && \
  cmake -D COMPONENT=tokukv_libs_shared -P cmake_install.cmake && \
  cmake -D COMPONENT=tokubackup_libs_shared -P cmake_install.cmake)

mkdir -p ${RPM_BUILD_ROOT}%{_defaultdocdir}/tokumx/licenses
mv $RPM_BUILD_ROOT/usr/GNU-AGPL-3.0 ${RPM_BUILD_ROOT}%{_defaultdocdir}/%{name}/licenses
mv $RPM_BUILD_ROOT/usr/README-TOKUKV ${RPM_BUILD_ROOT}%{_defaultdocdir}/%{name}/licenses
mv $RPM_BUILD_ROOT/usr/THIRD-PARTY-NOTICES ${RPM_BUILD_ROOT}%{_defaultdocdir}/%{name}/licenses
mv $RPM_BUILD_ROOT/usr/NEWS ${RPM_BUILD_ROOT}%{_defaultdocdir}/%{name}
mv $RPM_BUILD_ROOT/usr/README ${RPM_BUILD_ROOT}%{_defaultdocdir}/%{name}

mkdir -p ${RPM_BUILD_ROOT}%{_datadir}/%{name}/scripts
mv $RPM_BUILD_ROOT/usr/scripts/tokumxstat.py ${RPM_BUILD_ROOT}%{_datadir}/%{name}/scripts/
rmdir $RPM_BUILD_ROOT/usr/scripts

mkdir -p $RPM_BUILD_ROOT/usr/share/man/man1
cp debian/*.1 $RPM_BUILD_ROOT/usr/share/man/man1/
## FIXME: remove this rm when mongosniff is back in the package
rm -v $RPM_BUILD_ROOT/usr/share/man/man1/mongosniff.1*
mkdir -p $RPM_BUILD_ROOT/etc/rc.d/init.d
cp -v rpm/init.d-tokumx $RPM_BUILD_ROOT/etc/rc.d/init.d/tokumx
chmod a+x $RPM_BUILD_ROOT/etc/rc.d/init.d/tokumx
mkdir -p $RPM_BUILD_ROOT/etc
cp -v rpm/tokumx.conf $RPM_BUILD_ROOT/etc/tokumx.conf
mkdir -p $RPM_BUILD_ROOT/etc/sysconfig
cp -v rpm/tokumx.sysconfig $RPM_BUILD_ROOT/etc/sysconfig/tokumx
mkdir -p $RPM_BUILD_ROOT/var/lib/tokumx
mkdir -p $RPM_BUILD_ROOT/var/log/tokumx
touch $RPM_BUILD_ROOT/var/log/tokumx/tokumx.log

%clean
rm -rf $RPM_BUILD_ROOT

%pre server
if ! /usr/bin/id -g tokumx &>/dev/null; then
    /usr/sbin/groupadd -r tokumx
fi
if ! /usr/bin/id tokumx &>/dev/null; then
    /usr/sbin/useradd -M -r -g tokumx -d /var/lib/tokumx -s /bin/false 	-c tokumx tokumx > /dev/null 2>&1
fi

%post server
if test $1 = 1
then
  /sbin/chkconfig --add tokumx
fi

%preun server
if test $1 = 0
then
  /sbin/chkconfig --del tokumx
fi

%postun server
if test $1 -ge 1
then
  /sbin/service tokumx condrestart >/dev/null 2>&1 || :
fi

%files
%defattr(-,root,root,-)
#%doc README GNU-AGPL-3.0.txt

%{_bindir}/bsondump
%{_bindir}/mongo
%{_bindir}/mongo2toku
%{_bindir}/mongodump
%{_bindir}/mongoexport
%{_bindir}/mongofiles
%{_bindir}/mongoimport
#%{_bindir}/mongooplog
#%{_bindir}/mongoperf
%{_bindir}/mongorestore
%{_bindir}/mongotop
%{_bindir}/mongostat
# FIXME: uncomment when mongosniff is back in the package
#%{_bindir}/mongosniff

# FIXME: uncomment this when there's a stable release whose source
# tree contains a bsondump man page.
%{_mandir}/man1/bsondump.1*
%{_mandir}/man1/mongo.1*
%{_mandir}/man1/mongodump.1*
%{_mandir}/man1/mongoexport.1*
%{_mandir}/man1/mongofiles.1*
%{_mandir}/man1/mongoimport.1*
%{_mandir}/man1/mongorestore.1*
%{_mandir}/man1/mongotop.1*
%{_mandir}/man1/mongostat.1*
# FIXME: uncomment when mongosniff is back in the package
#%{_mandir}/man1/mongosniff.1*

%{_defaultdocdir}/%{name}/licenses/GNU-AGPL-3.0
%{_defaultdocdir}/%{name}/licenses/README-TOKUKV
%{_defaultdocdir}/%{name}/licenses/THIRD-PARTY-NOTICES
%{_defaultdocdir}/%{name}/README
%{_defaultdocdir}/%{name}/NEWS
%{_datadir}/%{name}/scripts/tokumxstat.py*

%files libs
%defattr(-,root,root,-)
/usr/lib64/tokumx/libHotBackup.so
/usr/lib64/tokumx/libtokufractaltree.so
/usr/lib64/tokumx/libtokuportability.so

%files server
%defattr(-,root,root,-)
%config(noreplace) %{_sysconfdir}/tokumx.conf
%{_bindir}/mongod
%{_bindir}/mongos
%{_mandir}/man1/mongod.1*
%{_mandir}/man1/mongos.1*
%{_sysconfdir}/rc.d/init.d/tokumx
%{_sysconfdir}/sysconfig/tokumx
#%{_sysconfdir}/rc.d/init.d/mongos
%attr(0755,tokumx,tokumx) %dir %{_sharedstatedir}/tokumx
%attr(0755,tokumx,tokumx) %dir /var/log/tokumx
%attr(0640,tokumx,tokumx) %config(noreplace) %verify(not md5 size mtime) /var/log/tokumx/tokumx.log

%changelog
* Wed Jan 08 2013 Leif Walsh <leif.walsh@gmail.com>
- Update for tokumx.

* Thu Jan 28 2010 Richard M Kreuter <richard@10gen.com>
- Minor fixes.

* Sat Oct 24 2009 Joe Miklojcik <jmiklojcik@shopwiki.com> - 
- Wrote mongo.spec.
