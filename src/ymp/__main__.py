"""
This allows calling the YMP cli via ``python -m``

>>> python -m ymp.cli show references -v
"""

import sys
from ymp.cli import main

if __name__ == "__main__":
    sys.argv[0] = "ymp"
    main()
