#!/usr/bin/env python
import xiaochen_py
import sys


if __name__ == "__main__":
    # check env: python version >= 3.12
    if sys.version_info < (3, 12):
        print("Python 3.12 or higher is required.")
        sys.exit(1)

    xiaochen_py.run_command("python -m scripts.test_postgres")
