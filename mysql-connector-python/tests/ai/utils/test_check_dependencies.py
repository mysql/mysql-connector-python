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

import importlib.metadata
import unittest

from unittest.mock import patch

import tests

from tests.ai.constants import AI_SKIP_MESSAGE

if tests.MYSQL_ML_ENABLED:
    from mysql.ai.utils import check_dependencies


@unittest.skipIf(not tests.MYSQL_ML_ENABLED, AI_SKIP_MESSAGE)
class TestCheckDependencies(unittest.TestCase):
    def test_missing_base_dep(self):
        """Should raise if a BASE dependency (pandas) is missing."""
        with patch("importlib.metadata.version") as mock_version:

            def side_effect(name):
                if name == "pandas":
                    raise importlib.metadata.PackageNotFoundError()
                return "999.0.0"  # Satisfy all but pandas

            mock_version.side_effect = side_effect
            with self.assertRaises(ImportError) as cm:
                check_dependencies(["BASE"])
            self.assertIn("pandas", str(cm.exception))

    def test_missing_task_deps(self):
        """Should raise and mention both if multiple required packages for tasks are missing."""
        with patch("importlib.metadata.version") as mock_version:

            def side_effect(name):
                # Simulate langchain and scikit-learn missing
                if name in {"langchain", "scikit-learn"}:
                    raise importlib.metadata.PackageNotFoundError()
                return "999.0.0"

            mock_version.side_effect = side_effect
            with self.assertRaises(ImportError) as cm:
                check_dependencies(["GENAI", "ML"])
            msg = str(cm.exception)
            self.assertIn("langchain", msg)
            self.assertIn("scikit-learn", msg)

    def test_version_too_old(self):
        """Should raise if one of the dependencies is present but too old."""
        with patch("importlib.metadata.version") as mock_version:

            def side_effect(name):
                # Only scikit-learn is too old
                if name == "scikit-learn":
                    return "1.7.1"  # below REQUIRED 1.7.2
                return "999.0.0"

            mock_version.side_effect = side_effect
            with self.assertRaises(ImportError) as cm:
                check_dependencies(["ML"])
            self.assertIn("scikit-learn", str(cm.exception))

    def test_all_present_and_compatible(self):
        """Should not raise if all required dependencies are present and at proper versions."""
        with patch("importlib.metadata.version") as mock_version:
            mock_version.return_value = "999.0.0"  # All versions high enough
            # Should not raise
            check_dependencies(["GENAI", "ML"])

    def test_combined_tasks_requirements(self):
        """Should union all task requirements and check each only once."""
        with patch("importlib.metadata.version") as mock_version:
            versions = {
                "pandas": "2.3.3",
                "langchain": "0.3.25",
                "langchain_core": "0.3.58",
                "pydantic": "2.7.4",
                "scikit-learn": "1.7.2",
            }
            mock_version.side_effect = lambda name: versions[name]
            # Should not raise (all versions adequate)
            check_dependencies(["GENAI", "ML"])
