#!/usr/bin/env python

# MySQL Connector/Python - MySQL driver written in Python.
# Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

# MySQL Connector/Python is licensed under the terms of the GPLv2
# <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most
# MySQL Connectors. There are special exceptions to the terms and
# conditions of the GPLv2 as it is applied to this software, see the
# FOSS License Exception
# <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA

"""Script for executing Django unit tests

This script will download and unpack Django in the current working directory.
It will run Django's 'runtests.py'.

MySQL Server and Database Requirements
--------------------------------------
Two MySQL servers, with replication set up, are required to run the tests. It
is probably wise to use MyISAM, as InnoDB will take to much time when
running tests often.
See the MySQL manual for information on how to change the default storage
engine:
 http://dev.mysql.com/doc/refman/5.6/en/storage-engine-setting.html

Make sure the database 'cpydjango1' has been created on the Master and is
also available on the slave.
 CREATE DATABASE cpydjango1 DEFAULT CHARACTER SET utf8;

Test settings
-------------
Database settings can be changed in the test_mysqlconnector_settings.py
file found in the same location as this script.

Notes
-----
* Not all tests will pass, some will fail or give errors. However, the
number of failures/errors should be be minimal (under 30?).
"""

import os
import sys
try:
    from urlparse import urlparse
    from urllib2 import (
        urlopen, ProxyHandler, URLError, HTTPError, install_opener,
        build_opener
    )
except ImportError:
    # Python 3
    from urllib.parse import urlparse
    from urllib.request import (
        urlopen, ProxyHandler, install_opener, build_opener
    )
    from urllib.error import URLError, HTTPError
import shutil
import argparse
import logging
from zipfile import ZipFile
import tarfile


PYMAJ = sys.version_info[0]
DJANGO = {
    '1.4': (
        'django-1.4.zip',
        'https://github.com/django/django/archive/stable/1.4.x.zip'
    ),
    '1.5': (
        'django-1.5.zip',
        'https://github.com/django/django/archive/stable/1.5.x.zip'
    ),
    '1.6': (
        'django-1.6.tar.gz',
        'https://www.djangoproject.com/m/releases/1.6/Django-1.6.tar.gz',
    ),
    '1.7': (
        'django-1.7.tar.gz',
        'https://www.djangoproject.com/m/releases/1.7/Django-1.7c1.tar.gz',
    ),

}

PROXIES = {
    'http': os.environ.setdefault('http_proxy', ''),
    'https': os.environ.setdefault('https_proxy', ''),
}

TEST_GROUPS = {
    'basic': ['basic'],
    'db': ['inspectdb'],
    'all': [],
}

logger = logging.getLogger(os.path.basename(__file__).replace('.py', ''))


def get_args():
    """Parse and return command line arguments
    """
    parser = argparse.ArgumentParser(
        description='Run Django tests',
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--tests', metavar='test', nargs='+',
        help="tests to run"
    )
    parser.add_argument(
        '--debug', action='store_true',
        help="show debugging information"
    )
    parser.add_argument(
        '--offline', action='store_true',
        help="don't check online or download any thing"
    )
    parser.add_argument(
        '--django', required=False, choices=DJANGO.keys(),
        help="Django version to tests, possible values: {versions}".format(
            versions=', '.join(DJANGO.keys()))
    )
    parser.add_argument(
        '--django-path', required=False,
        help="Path to Django source"
    )
    parser.add_argument(
        '--settings', required=False,
        help="Module containing Django Settings (note: not file)"
    )
    parser.add_argument(
        '--failfast', action='store_true',
        help="fail at first error/failure"
    )
    parser.add_argument(
        '--verbosity', action='store', default=1, type=int,
        help="verbosity level"
    )

    parser.add_argument(
        '--group', choices=TEST_GROUPS.keys(),
        help=("specify which group of tests to run; other tests on command "
              "line are discarted")
    )
    args, tests = parser.parse_known_args()

    if args.group and args.tests:
        logger.warning(
            "Executing tests from '{group}'; discarding specific tests."
            "".format(group=args.group)
        )
        args.tests = None

    if tests and not args.tests:
        args.tests = tests
    return args


def download(url, local_file=None):
    """Download a file given a URL

    This function downloads a file using the given url. When local_file
    is not given, a file name will be used as given by the server.
    """
    urlfp = urlopen(url, timeout=10)

    if not local_file:
        local_file = os.path.basename(url)
    # Don't download the same file again, or remove when different
    if os.path.exists(local_file):
        content_length = int(urlfp.info().get('Content-Length'))
        if os.path.getsize(local_file) != content_length:
            try:
                os.remove(local_file)
            except OSError:
                pass
        else:
            logger.info("File '{0}' already downloaded.".format(local_file))
            return

    urlparts = urlparse(url)
    logger.info("Downloading '{file}' from {loc}".format(
        file=local_file, loc=urlparts.netloc))

    with open(local_file, 'wb') as localfp:
        localfp.write(urlfp.read())


def _check_urls():
    """Check the the download URLs

    This function will check the urls given in the DJANGO constants.

    For now, what it does, is to check if there is a proxy available for
    the given URL scheme. If not, a warning will be logged.
    """
    checked = []
    for info in DJANGO.values():
        url = info[1]
        scheme = urlparse(url).scheme
        try:
            if not PROXIES[scheme] and scheme not in checked:
                logger.warning("Note: {scheme}_proxy environment "
                               "variable not set".format(scheme=scheme))
                checked.append(scheme)
        except KeyError:
            pass


def _unpack(archive_file):
    """Unpacks and extracts files from an archive

    This function will unpack and extra files from the file archive_file. It
    will return the directory to which the files were unpacked.

    An AttributeError is raised when the archive is not supported (when the
    name does not end with '.zip' or '.tar.gz')

    Returns string.
    """
    logger.info("Unpacking archive '{0}'".format(archive_file))
    if archive_file.endswith('.zip'):
        archive = ZipFile(archive_file)
        rootdir = archive.namelist()[0]
    elif archive_file.endswith('.tar.gz'):
        archive = tarfile.open(archive_file)
        rootdir = archive.getnames()[0]
    else:
        raise AttributeError("Unsupported archive. Can't unpack.")

    logger.info("Archive root folder is '{0}'".format(rootdir))

    try:
        shutil.rmtree(rootdir)
    except OSError:
        pass
    logger.info("Extracting to '{0}'".format(rootdir))
    archive.extractall()
    archive.close()
    return rootdir


def django_tests(django_version, django_root, myconnpy, tests=None, group=None,
                 settings=None, failfast=False, verbosity=1):
    """Run Django unit tests
    """
    if not (tests or group):
        logger.info("No specific tests or groups specified; running all tests")

    cwd = os.getcwd()
    os.chdir(os.path.join(django_root, 'tests'))
    logger.debug("Running tests from folder {0}".format(os.getcwd()))
    env = {
        'PATH': os.environ.get('PATH'),
        'PYTHONPATH': '{myconnpy}:{django_root}:'
                      '{cwd}:{cwd}/../:{cwd}/../..'.format(
            myconnpy=myconnpy, cwd=cwd, django_root=django_root),
    }
    if os.environ.get('DYLD_LIBRARY_PATH') is not None:
        env['DYLD_LIBRARY_PATH'] = os.environ.get('DYLD_LIBRARY_PATH')
    elif os.environ.get('LD_LIBRARY_PATH') is not None:
        env['LD_LIBRARY_PATH'] = os.environ.get('LD_LIBRARY_PATH')

    django_script = 'runtests.py'

    args = [
        sys.executable,
        '-B',
        '-W', 'ignore::DeprecationWarning',
        django_script,
        '--noinput',
        '--verbosity', str(verbosity),
    ]

    if failfast:
        args.append('--failfast')

    if settings:
        args.extend(['--settings', settings])
    else:
        args.extend(['--settings', 'test_mysqlconnector_settings'])

    if group:
        args.extend(TEST_GROUPS[group])
    elif tests:
        args.extend(tests)

    logger.info("Executing '{script}' script..".format(
        script=django_script)
    )
    os.execve(sys.executable, args, env)


def get_django_version(path):

    VERSION = [999, 0, 0, 'a', 0]  # Set correct after version.py is loaded
    py_module = os.path.join(path, 'django', '__init__.py')
    with open(py_module, 'rb') as fp:
        exec(compile(fp.read(), py_module, 'exec'))

    return VERSION


def main():
    """Start application
    """
    formatter = logging.Formatter(
        "%(asctime)s [%(name)s:%(levelname)s] %(message)s")
    loghandle = logging.StreamHandler()
    loghandle.setFormatter(formatter)
    logger.addHandler(loghandle)

    args = get_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    if os.name != 'nt':
        if 'http_proxy' not in os.environ or 'https_proxy' not in os.environ:
            logger.warning("Note: http_proxy and/or https_proxy "
                           "environment variables not set")

    logger.info("Python v{maj}: {exe}".format(maj=PYMAJ, exe=sys.executable))

    proxy = ProxyHandler()
    install_opener(build_opener(proxy))

    _check_urls()

    if not args.django_path and not args.django:
        logger.error("Need either --django or --django-path")
        sys.exit(1)

    if args.django_path:
        if not os.path.isdir(args.django_path):
            logger.error("Path to Django is not valid, was %s",
                         args.django_path)
            sys.exit(1)
        django_path = os.path.abspath(args.django_path)
    else:
        try:
            archive, url = DJANGO[args.django]
        except KeyError:
            logger.error("Django version %s is not valid", args.django)
            sys.exit(1)
        if not args.offline:
            try:
                download(url, archive)
            except (URLError, HTTPError, TypeError) as err:
                logger.error("Error downloading Django {name}: {error}".format(
                    name=args.django, error=err.reason)
                )
                sys.exit(1)
            try:
                cwd = os.getcwd()
                django_path = os.path.join(cwd, _unpack(archive))
            except IOError as err:
                logger.error("Failed unpacking: {err}".format(err=str(err)))
                sys.exit(1)
            except AttributeError:
                logger.error("Failed unpacking: {err}".format(err=str(err)))
                sys.exit(1)

    django_version = get_django_version(django_path)
    logger.info("Using Django %d.%d.%d", *django_version[0:3])

    myconnpy_install = os.path.join(os.getcwd(), '..', '..', 'lib')
    django_tests(django_version, django_path, myconnpy=myconnpy_install,
                 tests=args.tests, group=args.group, settings=args.settings,
                 failfast=args.failfast, verbosity=args.verbosity)

if __name__ == '__main__':
    main()
