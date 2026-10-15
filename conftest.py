from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'mysql-connector-python' / 'lib'))
sys.path.insert(0, str(ROOT / 'mysqlx-connector-python' / 'lib'))
