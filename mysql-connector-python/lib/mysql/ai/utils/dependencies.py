# Copyright (c) 2025, 2026, Oracle and/or its affiliates.
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

"""Dependency checking utilities for AI features in MySQL Connector/Python.

Provides check_dependencies() to assert required optional packages are present
with acceptable minimum versions at runtime.
"""

import importlib.metadata

from typing import List


def check_dependencies(tasks: List[str]) -> None:
    """
    Check required runtime dependencies and minimum versions; raise an error
    if any are missing or version-incompatible.

    This verifies the presence and minimum version of essential Python packages.
    Missing or insufficient versions cause an ImportError listing the packages
    and a suggested install command.

    Args:
        tasks (List[str]): Task types to check requirements for.

    Raises:
        ImportError: If any required dependencies are missing or below the
            minimum version.
    """
    task_set = set(tasks)
    task_set.add("BASE")

    # Requirements: (import_name, min_version)
    task_to_requirement = {
        "BASE": [("pandas", "2.3.3")],
        "GENAI": [
            ("langchain", "0.3.25"),
            ("langchain_core", "0.3.58"),
            ("pydantic", "2.7.4"),
        ],
        "ML": [("scikit-learn", "1.7.2")],
    }
    requirements = []
    for task in task_set:
        requirements.extend(task_to_requirement[task])
    requirements_set = set(requirements)

    problems = []
    for name, min_version in requirements_set:
        try:
            installed_version = importlib.metadata.version(name)
            # Version comparison uses simple string comparison to avoid extra
            # dependencies. This is valid for the dependencies defined above;
            # reconsider if adding packages with version schemes that do not
            # compare correctly as strings.
            error = installed_version < min_version
        except importlib.metadata.PackageNotFoundError:
            error = True
        if error:
            problems.append(f"{name} v{min_version} (or later)")
    if problems:
        raise ImportError("Please install " + ", ".join(problems) + ".")
