%global daemon tokumx

Name: tokumx
Conflicts: mongo, mongo-10gen, mongo-10gen-unstable, mongo-stable, mongodb, mongodb-server
Requires: tokumx-common
Version: 1.3.3
Release: 1%{?dist}
Summary: TokuMX client shell and tools
License: AGPLv3 and zlib and ASL 2.0 and GPLv2
Vendor: Tokutek, Inc.
URL: http://www.tokutek.com/products/tokumx-for-mongodb
Group: Applications/Databases

Source0: %{name}-%{version}.tar.gz
Source1: %{name}.init
Source2: %{name}.logrotate
Source3: %{name}.conf
Source4: %{daemon}.sysconf
Source5: %{name}-tmpfile
Source6: %{daemon}.service

%if 0%{?fedora} >= 15
BuildRequires: boost-devel
%endif
BuildRequires: pcre-devel
BuildRequires: readline-devel
BuildRequires: libpcap-devel
%if 0%{?fedora} >= 15
BuildRequires: systemd
%endif

%description
TokuMX is a high-performance version of MongoDB using Fractal
Tree indexes to store indexes and data.  TokuMX is
driver-compatible with MongoDB and has the same simple
management and operations interface as MongoDB, and adds:

   * Much higher performance on out-of-memory write workloads.
   * Compression of all data, up to 10x in many cases.
   * Document-level concurrency for all read and write
     operations.
   * Transactional semantics including snapshot reads and
     multi-statement transactions on a single shard basis.

This package provides the mongo shell, import/export tools, and other
client utilities.

%package common
Summary: TokuMX common files
Group: Applications/Databases

%description common
TokuMX is a high-performance version of MongoDB using Fractal
Tree indexes to store indexes and data.

This package provides the libraries shared among the server and tools,
including a portability layer and the Fractal Tree indexing library.

%package server
Summary: tokumx server, sharding server, and support scripts
Group: Applications/Databases
Requires: tokumx-common
Requires(pre): shadow-utils
%if 0%{?fedora} >= 15
Requires(post): systemd-units
Requires(preun): systemd-units
Requires(postun): systemd-units
%else
Requires(post): chkconfig
Requires(preun): chkconfig
Requires(postun): initscripts
%endif

%description server
TokuMX is a high-performance version of MongoDB using Fractal
Tree indexes to store indexes and data.

This package provides the mongo server software, mongo sharding server
softwware, default configuration files, and init.d scripts.

%package devel
Summary: Headers and libraries for mongo development. 
Group: Applications/Databases

%description devel
TokuMX is a high-performance version of MongoDB using Fractal
Tree indexes to store indexes and data.

This package provides the mongo static library and header files needed
to develop tokumx client software.

%prep
%setup

%clean
rm -rf %{buildroot}

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
%if 0%{?fedora} >= 15
    -D USE_SYSTEM_BOOST=ON \
%else
    -D USE_SYSTEM_BOOST=OFF \
%endif
    -D USE_SYSTEM_PCRE=ON \
    -D TOKUMX_STRIP_BINARIES=OFF \
    -D TOKUMX_SET_RPATH=OFF \
    -D CMAKE_INSTALL_PREFIX=%{buildroot}/%{_prefix} \
    -D BUILD_TESTING=OFF \
    -D INSTALL_LIBDIR=%{_lib}/%{name} \
    -D CMAKE_INSTALL_RPATH=%{_libdir}/%{name} \
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

install -p -dm755 %{buildroot}%{_docdir}/%{name}/licenses
mv %{buildroot}%{_prefix}/GNU-AGPL-3.0        %{buildroot}%{_docdir}/%{name}/licenses
mv %{buildroot}%{_prefix}/README-TOKUKV       %{buildroot}%{_docdir}/%{name}/licenses
mv %{buildroot}%{_prefix}/THIRD-PARTY-NOTICES %{buildroot}%{_docdir}/%{name}/licenses
mv %{buildroot}%{_prefix}/NEWS                %{buildroot}%{_docdir}/%{name}
mv %{buildroot}%{_prefix}/README              %{buildroot}%{_docdir}/%{name}

install -p -Dm755 %{buildroot}%{_prefix}/scripts/tokumxstat.py %{buildroot}%{_datadir}/%{name}/scripts/tokumxstat.py
rm -rf %{buildroot}%{_prefix}/scripts

mkdir -p %{buildroot}%{_sharedstatedir}/%{name}
mkdir -p %{buildroot}%{_localstatedir}/log/%{name}
mkdir -p %{buildroot}%{_localstatedir}/run/%{name}

%if 0%{?fedora} >= 15
install -p -dm755 %{buildroot}%{_unitdir}
install -p -Dm644 %{SOURCE5} %{buildroot}%{_libdir}/../lib/tmpfiles.d/%{name}.conf
install -p -Dm644 %{SOURCE6} %{buildroot}%{_unitdir}/%{daemon}.service
%else
install -p -Dm755 %{SOURCE1} $RPM_BUILD_ROOT%{_initddir}/%{name}
%endif

install -p -Dm644 %{SOURCE2} %{buildroot}%{_sysconfdir}/logrotate.d/%{name}
install -p -Dm644 %{SOURCE3} %{buildroot}%{_sysconfdir}/%{name}.conf
install -p -Dm644 %{SOURCE4} %{buildroot}%{_sysconfdir}/sysconfig/%{daemon}

install -dm755 %{buildroot}%{_mandir}/man1
install -p -m644 -t %{buildroot}%{_mandir}/man1 debian/*.1

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%pre server
getent group %{name} >/dev/null || groupadd -r %{name}
getent passwd %{name} >/dev/null || \
    useradd -r -g %{name} -u 184 -d %{_sharedstatedir}/%{name} -s /sbin/nologin \
    -c "TokuMX Database Server" %{name}
exit 0

%post server
%if 0%{?fedora} >= 15
/bin/systemd-tmpfiles --create %{daemon}.conf
/bin/systemctl daemon-reload &>/dev/null || :
%else
/sbin/chkconfig --add %{daemon}
%endif

echo ""
echo "********************************************************************************"
echo ""
echo "Edit /etc/tokumx.conf as needed."
%if 0%{?fedora} >= 15
echo "TokuMX can be run with systemctl:"
echo "  systemctl start tokumx"
echo "  systemctl stop tokumx"
%else
echo "TokuMX can be run with service:"
echo "  service tokumx start"
echo "  service tokumx stop"
%endif
echo ""
echo "Please note that TokuMX will not run with transparent huge pages enabled."
echo "To disable them manually, run (as root)"
echo "  echo never > /sys/kernel/mm/transparent_hugepage/enabled"
echo "or to use sudo, you can run"
echo "  echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled"
echo ""
%if 0%{?fedora} >= 15
echo "A tmpfiles.d (see systemd-tmpfiles(8)) configuration has been installed to"
echo %{_prefix}"/lib/tmpfiles.d/"%{name}".conf, which will disable transparent huge pages"
echo "on startup."
echo ""
echo "To invoke it now without rebooting, run"
echo "  systemd-tmpfiles --create "%{name}".conf"
echo ""
echo "To disable this behavior, you can create a symlink to /dev/null"
echo "  ln -s /dev/null "%{_sysconfdir}"/tmpfiles.d/"%{name}".conf"
%else
echo "This will be done for you automatically by the initscripts."
%endif
echo ""
echo "********************************************************************************"
echo ""

%preun server
if [ $1 = 0 ] ; then
%if 0%{?fedora} >= 15
    /bin/systemctl --no-reload disable %{daemon}.service &>/dev/null
    /bin/systemctl stop %{daemon}.service &>/dev/null
%else
    /sbin/service %{daemon} stop >/dev/null 2>&1
    /sbin/chkconfig --del %{daemon}
%endif
fi

%postun server
%if 0%{?fedora} >= 15
/bin/systemctl daemon-reload &>/dev/null
%endif
if [ "$1" -ge "1" ] ; then
%if 0%{?fedora} >= 15
    /bin/systemctl try-restart %{daemon}.service &>/dev/null
%else
    /sbin/service %{daemon} condrestart >/dev/null 2>&1 || :
%endif
fi

%files
%{_bindir}/bsondump
%{_bindir}/mongo
%{_bindir}/mongo2toku
%{_bindir}/mongodump
%{_bindir}/mongoexport
%{_bindir}/mongofiles
%{_bindir}/mongoimport
%{_bindir}/mongorestore
%{_bindir}/mongosniff
%{_bindir}/mongostat
%{_bindir}/mongotop

%{_mandir}/man1/bsondump.1*
%{_mandir}/man1/mongo.1*
%{_mandir}/man1/mongodump.1*
%{_mandir}/man1/mongoexport.1*
%{_mandir}/man1/mongofiles.1*
%{_mandir}/man1/mongoimport.1*
%{_mandir}/man1/mongorestore.1*
%{_mandir}/man1/mongosniff.1*
%{_mandir}/man1/mongostat.1*
%{_mandir}/man1/mongotop.1*

%{_datadir}/%{name}/scripts/tokumxstat.py*

%files common
%doc %{_docdir}/%{name}/licenses/GNU-AGPL-3.0
%doc %{_docdir}/%{name}/licenses/README-TOKUKV
%doc %{_docdir}/%{name}/licenses/THIRD-PARTY-NOTICES
%doc %{_docdir}/%{name}/README
%doc %{_docdir}/%{name}/NEWS

%{_libdir}/%{name}/libHotBackup.so
%{_libdir}/%{name}/libtokufractaltree.so
%{_libdir}/%{name}/libtokuportability.so

%files server
%{_bindir}/mongod
%{_bindir}/mongos
%{_mandir}/man1/mongod.1*
%{_mandir}/man1/mongos.1*
%dir %attr(0755, %{name}, root) %{_sharedstatedir}/%{name}
%dir %attr(0755, %{name}, root) %{_localstatedir}/log/%{name}
%dir %attr(0755, %{name}, root) %{_localstatedir}/run/%{name}
%config(noreplace) %{_sysconfdir}/logrotate.d/%{name}
%config(noreplace) %{_sysconfdir}/%{name}.conf
%config(noreplace) %{_sysconfdir}/sysconfig/%{daemon}
%if 0%{?fedora} >= 15
%{_unitdir}/%{daemon}.service
%{_libdir}/../lib/tmpfiles.d/%{name}.conf
%else
%{_initddir}/%{daemon}
%endif

%changelog
* Wed Jan 08 2014 Leif Walsh <leif.walsh@gmail.com>
- Update for tokumx.

* Thu Jan 28 2010 Richard M Kreuter <richard@10gen.com>
- Minor fixes.

* Sat Oct 24 2009 Joe Miklojcik <jmiklojcik@shopwiki.com> - 
- Wrote mongo.spec.
