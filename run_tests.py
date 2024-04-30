import os.path
import sys

import pytest

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT_DIR, "src"))
sys.path.insert(0, os.path.join(ROOT_DIR, "tests"))

# Add any required args for testing.
args = ["--cache-clear", "-rsxX", "-l", "--tb=short", "--strict", "-vv"]

pytest.main(args)
