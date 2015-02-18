%global product_name tokumx
%if 0%{?rhel:1}
%if %{rhel} <= 5
%define _sharedstatedir /var/lib
%endif
%endif

%if 0%{tokumx_enterprise}
Name: tokumx-enterprise
Conflicts: mongo, mongodb, mongo-stable, mongo-10gen, mongo-10gen-enterprise, mongo-10gen-unstable, mongo-10gen-unstable-shell, mongo-10gen-unstable-tools, mongodb-org, mongodb-org-shell, mongodb-org-tools, mongodb-org-unstable, mongodb-org-unstable-shell, mongodb-org-unstable-tools, tokumx
%else
Name: tokumx
Conflicts: mongo, mongodb, mongo-stable, mongo-10gen, mongo-10gen-enterprise, mongo-10gen-unstable, mongo-10gen-unstable-shell, mongo-10gen-unstable-tools, mongodb-org, mongodb-org-shell, mongodb-org-tools, mongodb-org-unstable, mongodb-org-unstable-shell, mongodb-org-unstable-tools, tokumx-enterprise
%endif
Version: %{?tokumx_version}%{!?tokumx_version:1.4.0}
Release: %{?tokumx_rpm_release_version}%{!?tokumx_rpm_release_version:1}%{?dist}
Summary: TokuMX client shell and tools
License: AGPLv3 and zlib and ASL 2.0 and GPLv2
Vendor: Tokutek, Inc.
URL: http://www.tokutek.com/products/tokumx-for-mongodb
Group: Applications/Databases
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Source0: %{name}-%{version}.tar.gz
Source1: %{product_name}.init
Source2: %{product_name}.logrotate
Source3: %{product_name}.conf
Source4: %{product_name}.sysconf
Source5: %{product_name}-tmpfile
Source6: %{product_name}.service

%if 0%{?fedora} >= 15
BuildRequires: boost-devel
%endif
BuildRequires: readline-devel
BuildRequires: libpcap-devel
%if 0%{?fedora} >= 15
BuildRequires: systemd
%endif
Requires: %{name}-common = %{version}-%{release}


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
%if 0%{tokumx_enterprise}
Conflicts: tokumx-common
%else
Conflicts: tokumx-enterprise-common
%endif

%description common
TokuMX is a high-performance version of MongoDB using Fractal
Tree indexes to store indexes and data.

This package provides the libraries shared among the server and tools,
including a portability layer and the Fractal Tree indexing library.

%package server
Summary: tokumx server, sharding server, and support scripts
Group: Applications/Databases
Requires: %{name}-common = %{version}-%{release}
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
%if 0%{tokumx_enterprise}
Conflicts: mongodb-server, mongo-10gen-server, mongo-10gen-unstable-server, mongodb-org-mongos, mongodb-org-server, mongodb-org-unstable-mongos, mongodb-org-unstable-server, tokumx-server
%else
Conflicts: mongodb-server, mongo-10gen-server, mongo-10gen-unstable-server, mongodb-org-mongos, mongodb-org-server, mongodb-org-unstable-mongos, mongodb-org-unstable-server, tokumx-enterprise-server
%endif

%description server
TokuMX is a high-performance version of MongoDB using Fractal
Tree indexes to store indexes and data.

This package provides the mongo server software, mongo sharding server
softwware, default configuration files, and init.d scripts.

%package -n lib%{name}
Summary: TokuMX shared libraries
Group: Development/Libraries
Requires: %{name}-common = %{version}-%{release}
%if 0%{tokumx_enterprise}
Conflicts: libtokumx, libmongodb
%else
Conflicts: libtokumx-enterprise, libmongodb
%endif

%description -n lib%{name}
TokuMX is a high-performance version of MongoDB using Fractal
Tree indexes to store indexes and data.

This package provides the shared library for the TokuMX client.

%package -n lib%{name}-devel
Summary: TokuMX header files
Group: Development/Libraries
Requires: lib%{name} = %{version}-%{release}
Requires: boost-devel
Provides: tokumx-devel = %{version}-%{release}
%if 0%{tokumx_enterprise}
Conflicts: libtokumx-devel, libmongodb-devel, mongodb-devel
%else
Conflicts: libtokumx-enterprise, libmongodb-devel, mongodb-devel
%endif

%description -n lib%{name}-devel
TokuMX is a high-performance version of MongoDB using Fractal
Tree indexes to store indexes and data.

This package provides the header files and C++ driver for TokuMX.

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
    -D USE_SYSTEM_PCRE=OFF \
    -D TOKUMX_STRIP_BINARIES=OFF \
    -D TOKUMX_SET_RPATH=OFF \
    -D CMAKE_INSTALL_PREFIX=%{buildroot}/%{_prefix} \
    -D BUILD_TESTING=OFF \
    -D INSTALL_LIBDIR=%{_lib}/%{product_name} \
    -D CMAKE_INSTALL_RPATH=%{_libdir}/%{product_name} \
    -D TOKUMX_CLIENT_LIB_SHARED=ON \
%if 0%{?tokumx_revision:1}
    -D TOKUMX_GIT_VERSION=%{tokumx_revision} \
%endif
%if 0%{?tokukv_revision:1}
    -D TOKUKV_GIT_VERSION=%{tokukv_revision} \
%endif
%if 0%{?tokumx_audit_enterprise_revision:1}
    -D TOKUMX_AUDIT_ENTERPRISE_GIT_VERSION=%{tokumx_audit_enterprise_revision} \
%endif
    ..)
make -C opt %{?_smp_mflags}

%install
(cd opt; \
  cmake -D COMPONENT=tokumx_server -P cmake_install.cmake && \
  cmake -D COMPONENT=tokumx_misc -P cmake_install.cmake && \
  cmake -D COMPONENT=tokumx_tools -P cmake_install.cmake && \
  cmake -D COMPONENT=tokumx_plugins -P cmake_install.cmake && \
  cmake -D COMPONENT=tokukv_libs_shared -P cmake_install.cmake && \
  cmake -D COMPONENT=tokubackup_libs_shared -P cmake_install.cmake && \
  cmake -D COMPONENT=tokumx_audit_libs_shared -P cmake_install.cmake && \
  cmake -D COMPONENT=tokumx_client_headers -P cmake_install.cmake && \
  cmake -D COMPONENT=tokumx_client_libs -P cmake_install.cmake)

install -p -dm755 %{buildroot}%{_docdir}/%{product_name}/licenses
mv %{buildroot}%{_prefix}/GNU-AGPL-3.0        %{buildroot}%{_docdir}/%{product_name}/licenses
mv %{buildroot}%{_prefix}/README-TOKUKV       %{buildroot}%{_docdir}/%{product_name}/licenses
mv %{buildroot}%{_prefix}/THIRD-PARTY-NOTICES %{buildroot}%{_docdir}/%{product_name}/licenses
mv %{buildroot}%{_prefix}/NEWS                %{buildroot}%{_docdir}/%{product_name}
mv %{buildroot}%{_prefix}/README              %{buildroot}%{_docdir}/%{product_name}

install -p -Dm755 %{buildroot}%{_prefix}/scripts/tokumxstat.py %{buildroot}%{_datadir}/%{product_name}/scripts/tokumxstat.py
rm -rf %{buildroot}%{_prefix}/scripts

mkdir -p %{buildroot}%{_sharedstatedir}/%{product_name}
mkdir -p %{buildroot}%{_localstatedir}/log/%{product_name}
mkdir -p %{buildroot}%{_localstatedir}/run/%{product_name}

%if 0%{?fedora} >= 15
install -p -dm755 %{buildroot}%{_unitdir}
install -p -Dm644 %{SOURCE5} %{buildroot}%{_libdir}/../lib/tmpfiles.d/%{product_name}.conf
install -p -Dm644 %{SOURCE6} %{buildroot}%{_unitdir}/%{product_name}.service
%else
%if 0%{?rhel} >= 6
install -p -Dm755 %{SOURCE1} %{buildroot}%{_initddir}/%{product_name}
%else
install -p -Dm755 %{SOURCE1} %{buildroot}%{_initrddir}/%{product_name}
%endif
%endif

install -p -Dm644 %{SOURCE2} %{buildroot}%{_sysconfdir}/logrotate.d/%{product_name}
install -p -Dm644 %{SOURCE3} %{buildroot}%{_sysconfdir}/%{product_name}.conf
sed -i'' -e "s#@LIBDIR@#"%{_libdir}"#" %{buildroot}%{_sysconfdir}/%{product_name}.conf
install -p -Dm644 %{SOURCE4} %{buildroot}%{_sysconfdir}/sysconfig/%{product_name}

install -dm755 %{buildroot}%{_mandir}/man1
install -p -m644 -t %{buildroot}%{_mandir}/man1 debian/*.1

mv %{buildroot}%{_libdir}/%{product_name}/libmongoclient.so %{buildroot}%{_libdir}
install -p -dm755 %{buildroot}%{_libdir}/%{product_name}/plugins

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%pre server
getent group %{product_name} >/dev/null || groupadd -r %{product_name}
getent passwd %{product_name} >/dev/null || \
    useradd -r -g %{product_name} -u 184 -d %{_sharedstatedir}/%{product_name} -s /sbin/nologin \
    -c "TokuMX Database Server" %{product_name}
exit 0

%post server
%if 0%{?fedora} >= 15
/bin/systemd-tmpfiles --create %{product_name}.conf
/bin/systemctl product_name-reload &>/dev/null || :
%else
/sbin/chkconfig --add %{product_name}
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
echo %{_prefix}"/lib/tmpfiles.d/"%{product_name}".conf, which will disable transparent huge pages"
echo "on startup."
echo ""
echo "To invoke it now without rebooting, run"
echo "  systemd-tmpfiles --create "%{product_name}".conf"
echo ""
echo "To disable this behavior, you can create a symlink to /dev/null"
echo "  ln -s /dev/null "%{_sysconfdir}"/tmpfiles.d/"%{product_name}".conf"
%else
echo "This will be done for you automatically by the initscripts."
%endif
echo ""
echo "********************************************************************************"
echo ""

%preun server
if [ $1 = 0 ] ; then
%if 0%{?fedora} >= 15
    /bin/systemctl --no-reload disable %{product_name}.service &>/dev/null
    /bin/systemctl stop %{product_name}.service &>/dev/null
%else
    /sbin/service %{product_name} stop >/dev/null 2>&1
    /sbin/chkconfig --del %{product_name}
%endif
fi

%postun server
%if 0%{?fedora} >= 15
/bin/systemctl daemon-reload &>/dev/null
%endif
if [ "$1" -ge "1" ] ; then
%if 0%{?fedora} >= 15
    /bin/systemctl try-restart %{product_name}.service &>/dev/null
%else
    /sbin/service %{product_name} condrestart >/dev/null 2>&1 || :
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

%{_datadir}/%{product_name}

%files common

%doc %{_docdir}/%{product_name}
%{_libdir}/%{product_name}
%{_libdir}/%{product_name}/plugins

%files -n lib%{name}

%{_libdir}/libmongoclient.so

%files -n lib%{name}-devel

%{_includedir}/*

%files server
%{_bindir}/mongod
%{_bindir}/mongos
%{_mandir}/man1/mongod.1*
%{_mandir}/man1/mongos.1*
%dir %attr(0755, %{product_name}, root) %{_sharedstatedir}/%{product_name}
%dir %attr(0755, %{product_name}, root) %{_localstatedir}/log/%{product_name}
%dir %attr(0755, %{product_name}, root) %{_localstatedir}/run/%{product_name}
%config(noreplace) %{_sysconfdir}/logrotate.d/%{product_name}
%config(noreplace) %{_sysconfdir}/%{product_name}.conf
%config(noreplace) %{_sysconfdir}/sysconfig/%{product_name}
%if 0%{?fedora} >= 15
%{_unitdir}/%{product_name}.service
%{_libdir}/../lib/tmpfiles.d/%{product_name}.conf
%else
%if 0%{?rhel} >= 6
%{_initddir}/%{product_name}
%else
%{_initrddir}/%{product_name}
%endif
%endif

%changelog
* Wed Jan 08 2014 Leif Walsh <leif.walsh@gmail.com>
- Update for tokumx.

* Thu Jan 28 2010 Richard M Kreuter <richard@10gen.com>
- Minor fixes.

* Sat Oct 24 2009 Joe Miklojcik <jmiklojcik@shopwiki.com> - 
- Wrote mongo.spec.
