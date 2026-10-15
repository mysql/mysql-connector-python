from pathlib import Path

import mysql.connector


def test_local_package_imports_from_repo_tree():
    root = Path(__file__).resolve().parents[2]
    assert str(root / 'mysql-connector-python' / 'lib') in mysql.connector.__file__
