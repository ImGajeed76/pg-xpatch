#!/usr/bin/env python3
"""
xptest - pg-xpatch Test Runner

Entry point script for running pg-xpatch tests.

Usage:
    ./xptest.py                      # Run all tests
    ./xptest.py tests/unit/          # Run unit tests only
    ./xptest.py -k "stats"           # Filter by name
    ./xptest.py -t unit              # Filter by tag
    ./xptest.py --list               # List tests
    ./xptest.py --help               # Show help
"""

import sys
from pathlib import Path

# Add the project root to path so xptest package can be imported
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from xptest.cli import main

if __name__ == "__main__":
    main()
