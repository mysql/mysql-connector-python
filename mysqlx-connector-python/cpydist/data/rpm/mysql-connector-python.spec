# Copyright (c) 2015, 2024, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0, as
# published by the Free Software Foundation.
#
# This program is designed to work with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation. The authors of MySQL hereby grant you an
# additional permission to link the program and your derivative works
# with the separately licensed software that they have either included with
# the program or referenced in the documentation.
#
# Without limiting anything contained in the foregoing, this file,
# which is part of MySQL Connector/Python, is also subject to the
# Universal FOSS Exception, version 1.0, a copy of which can be found at
# http://oss.oracle.com/licenses/universal-foss-exception.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

%global wants_py_dnspython_version 2.6.1

%undefine _package_note_file

%if 0%{?rhel} == 8
%{!?__python3: %global __python3 /usr/bin/python3.9}
%{!?python3_pkgversion: %global python3_pkgversion 39}
%endif

%{?protobuf_include_dir: %global with_protobuf_include_dir %{protobuf_include_dir}}
%{?protobuf_lib_dir: %global with_protobuf_lib_dir %{protobuf_lib_dir}}
%{?protoc: %global with_protoc %{protoc}}
%{?extra_compile_args: %global extra_compile_args %{extra_compile_args}}
%{?extra_link_args: %global extra_link_args %{extra_link_args}}

# set version if not defined through 'rpmbuild'
%{!?version: %global version 9.2.0}


# if true set byte_code_only to --byte_code_only
%if 0%{?byte_code_only}
%global byte_code_only --byte-code-only
%endif

# set lic_type to GPLv2 if not defined through 'rpmbuild'
%{!?lic_type: %global lic_type GPLv2}

# if label is defined, set product_suffix to '-{label}'
%if 0%{?label:1}
%global product_suffix -%{label}
%endif

# ======================================================================
# Main section
# ======================================================================

Summary:       Standardized MySQL database driver for Python
Name:          mysqlx-connector-python%{?product_suffix}
Version:       %{version}
Release:       1%{?version_extra:.%{version_extra}}%{?byte_code_only:.1}%{?dist}
License:       Copyright (c) 2015, 2024, Oracle and/or its affiliates. Under %{?license_type} license as shown in the Description field.
URL:           https://dev.mysql.com/downloads/connector/python/
Source0:       https://cdn.mysql.com/Downloads/Connector-Python/mysql-connector-python%{?product_suffix}-%{version}-src.tar.gz

%if 0%{?rhel} == 8
BuildRequires: python%{python3_pkgversion}-devel
BuildRequires: python%{python3_pkgversion}-setuptools
BuildRequires: python%{python3_pkgversion}-rpm-macros
%endif

%description
MySQL Connector/Python enables Python programs to access MySQL
databases, using an API that is compliant with the Python DB API
version 2.0. It is written in pure Python and does not have any
dependencies except for the Python Standard Library.
The MySQL software has Dual Licensing, which means you can use the

MySQL software free of charge under the GNU General Public License
(http://www.gnu.org/licenses/). You can also purchase commercial MySQL
licenses from Oracle and/or its affiliates if you do not wish to be
bound by the terms of the GPL. See the chapter "Licensing and Support"
in the manual for further info.

The MySQL web site (http://www.mysql.com/) provides the latest news
and information about the MySQL software. Also please see the
documentation and the manual for more information.

%package    -n mysqlx-connector-python3%{?product_suffix}
Summary:       Standardized MySQL database driver for Python 3

Obsoletes:   mysqlx-connector-python3%{?product_suffix}-cext < %{version}-%{release}
Provides:    mysqlx-connector-python3%{?product_suffix}-cext = %{version}-%{release}

%if 0%{?byte_code_only:1}
Obsoletes:     mysqlx-connector-python3 < %{version}-%{release}
Provides:      mysqlx-connector-python3 = %{version}-%{release}
Obsoletes:     mysqlx-connector-python3-cext < %{version}-%{release}
Provides:      mysqlx-connector-python3-cext = %{version}-%{release}
%endif

%if 0%{?rhel} == 8
Requires:      python39
%endif


%description -n mysqlx-connector-python3%{?product_suffix}
MySQL Connector/Python enables Python programs to access MySQL
databases, using an API that is compliant with the Python DB API
version 2.0. It is written in pure Python and does not have any
dependencies except for the Python Standard Library.

This is the Python 3 version of the driver.

The MySQL software has Dual Licensing, which means you can use the
MySQL software free of charge under the GNU General Public License
(http://www.gnu.org/licenses/). You can also purchase commercial MySQL
licenses from Oracle and/or its affiliates if you do not wish to be
bound by the terms of the GPL. See the chapter "Licensing and Support"
in the manual for further info.

The MySQL web site (http://www.mysql.com/) provides the latest news
and information about the MySQL software. Also please see the
documentation and the manual for more information.

%prep
%setup -q -n mysql-connector-python%{?product_suffix}-%{version}-src

%install
%{?scl:scl enable %{scl} - << \EOF}
set -ex
COMMON_INSTALL_ARGS="\
    install \
    --prefix=%{_prefix} \
    --root=%{buildroot} \
    --with-protobuf-include-dir=%{with_protobuf_include_dir} \
    --with-protobuf-lib-dir=%{with_protobuf_lib_dir} \
    --with-protoc=%{with_protoc}
"

%if 0%{?extra_compile_args:1}
EXTRA_COMPILE_ARGS="%{extra_compile_args}"
%else
EXTRA_COMPILE_ARGS=""
%endif

%if 0%{?extra_link_args:1}
EXTRA_LINK_ARGS="%{extra_link_args}"
%else
EXTRA_LINK_ARGS=""
%endif

rm -rf %{buildroot}

cd mysqlx-connector-python
%{__python3} setup.py ${COMMON_INSTALL_ARGS} \
    --extra-compile-args="${EXTRA_COMPILE_ARGS}" \
    --extra-link-args="${EXTRA_LINK_ARGS}" %{?byte_code_only}
%{?scl:EOF}

sed -i -e '/protobuf/d' %{buildroot}%{python3_sitearch}/mysqlx_connector_python-*.egg-info/requires.txt

%files -n mysqlx-connector-python3%{?product_suffix}
%doc LICENSE.txt CHANGES.txt README.txt README.rst CONTRIBUTING.md SECURITY.md mysqlx-connector-python/docs/INFO_SRC mysqlx-connector-python/docs/INFO_BIN
%{python3_sitearch}/mysqlx
%{python3_sitearch}/mysqlx_connector_python-*.egg-info
%{python3_sitearch}/_mysqlxpb.cpython*.so

%changelog
* Mon Oct 28 2024 Souma Kanti Ghosh <souma.kanti.ghosh@oracle.com> - 9.2.0-1
- Updated for 9.2.0

* Wed Sep 11 2024 Oscar Pacheco <oscar.p.pacheco@oracle.com> - 9.1.0-1
- Updated for 9.1.0

* Mon Aug 5 2024 Souma Kanti Ghosh <souma.kanti.ghosh@oracle.com> - 9.1.0-1
- Removed rules for Fedora, openSUSE and EL7 platforms
- Removed Python 3.8 support
- Updated copyright year from 2021 to 2024

* Fri May 31 2024 Oscar Pacheco <oscar.p.pacheco@oracle.com> - 9.0.0-1
- Updated for 9.0.0

* Wed Feb 28 2024 Oscar Pacheco <oscar.p.pacheco@oracle.com> - 8.4.0-1
- Updated for 8.4.0

* Wed Dec 6 2023  Oscar Pacheco <oscar.p.pacheco@oracle.com> - 8.3.0-1
- Updated Python version for openSUSE 15 from 3.9 to 3.11

* Fri Dec 1 2023  Oscar Pacheco <oscar.p.pacheco@oracle.com> - 8.3.0-1
- Updated for 8.3.0

* Mon May 22 2023  Nuno Mariz <nuno.mariz@oracle.com> - 8.1.0-1
- Updated for 8.1.0

* Wed Jan 25 2023  Oscar Pacheco <oscar.p.pacheco@oracle.com> - 8.0.33-1
- Updated for 8.0.33

* Sun Oct 23 2022  Kent Boortz <kent.boortz@oracle.com> - 8.0.32-1
- Updated for 8.0.32

* Fri Jul 22 2022  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.31-1
- Updated for 8.0.31

* Mon Apr 18 2022  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.30-1
- Updated for 8.0.30

* Tue Jan 18 2022  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.29-1
- Updated for 8.0.29

* Fri Oct 8 2021  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.28-1
- Updated for 8.0.28

* Tue Jul 20 2021  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.27-1
- Updated for 8.0.27

* Fri Apr 23 2021  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.26-1
- Updated for 8.0.26

* Thu Apr 15 2021  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.25-1
- Updated for 8.0.25

* Mon Feb 1 2021 Sreedhar Sreedhargadda <sreedhar.sreedhargadda@oracle.com> - 8.0.24-2
- Updated for 8.0.24
- Fix for el8 pkgver_lite

* Wed Dec 9 2020 Prashant Tekriwal <prashant.tekriwal@oracle.com> - 8.0.24-1
- Updated for 8.0.24
- Removed python2 support
- Follow updated package guidelines and style

* Mon Nov 16 2020  Prashant Tekriwal <prashant.tekriwal@oracle.com> - 8.0.23-1
- Updated for 8.0.23
- Removed dependency on the Server "client-plugin" RPM

* Mon Sep 07 2020  Kent Boortz <kent.boortz@oracle.com> - 8.0.22-1
- Updated for 8.0.22
- Still provide "mysql-connector-python-cext"
- Removed dependency on "mysql-connector-python3-cext"
- Disabled the bundling of "authentication_ldap_sasl_client.so"
  and added dependency on the Server "client-plugin" RPM

* Thu May 28 2020  Prashant Tekriwal <Prashant.Tekriwal@oracle.com> - 8.0.21-2
- Combined cext package and pure python package to single pkg.
- Added 'lic_type' variable: sets license type. Default is GPLv2
- Removed commercial references.
- Added 'label' variable: can use to add suffix to package name
- Added 'byte_code_only' variable: instructs to generate and keep only .pyc files

* Mon Mar 30 2020  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.21-1
- Updated for 8.0.21

* Mon Jan 13 2020  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.20-1
- Updated for 8.0.20

* Tue Nov 26 2019  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.19-1
- Updated for 8.0.19

* Fri Aug 23 2019  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.18-1
- Updated for 8.0.18

* Mon May 27 2019  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.17-1
- Updated for 8.0.17

* Tue Feb 5 2019  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.16-1
- Updated for 8.0.16

* Fri Jan 25 2019  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.15-1
- Updated for 8.0.15

* Wed Nov 28 2018  Nawaz Nazeer Ahamed <nawaz.nazeer.ahamed@oracle.com> - 8.0.14-2
- Updated copyright year from 2018 to 2019

* Wed Nov 28 2018  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.14-1
- Updated for 8.0.14

* Fri Sep 14 2018  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.13-1
- Updated for 8.0.13

* Sat Sep 1 2018  Israel Gomez <israel.gomez@oracle.com> - 8.0.12-2
- Updated rpm pakage name of open SUSE from sles15 to sl15

* Fri Jun 8 2018  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.12-1
- Updated for 8.0.12

* Fri Mar 16 2018  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.11-1
- Updated for 8.0.11

* Mon Dec 11 2017  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.6-1
- Updated for 8.0.6

* Mon Aug 21 2017  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.5-1
- Updated for 8.0.5

* Mon May 22 2017  Nuno Mariz <nuno.mariz@oracle.com> - 8.0.4-1
- Updated for 8.0.4

* Tue Mar 7 2017  Nuno Mariz <nuno.mariz@oracle.com> - 2.2.3-1
- Updated for 2.2.3

* Tue Oct 4 2016  Nuno Mariz <nuno.mariz@oracle.com> - 2.2.2-1
- Updated for 2.2.2

* Fri Aug 12 2016  Nuno Mariz <nuno.mariz@oracle.com> - 2.2.1-1
- Updated for 2.2.1

* Tue May 24 2016  Nuno Mariz <nuno.mariz@oracle.com> - 2.2.0-1
- Updated for 2.2.0

* Wed Feb 10 2016  Geert Vanderkelen <geert.vanderkelen@oracle.com> - 2.1.4-1
- Updated for 2.1.4

* Fri Jul 31 2015 Balasubramanian Kandasamy <balasubramanian.kandasamy@oracle.com> - 2.1.3-1
- New spec file with support for cext, license options and Python 3 support

