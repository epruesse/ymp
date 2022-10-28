"""This allows calling the YMP cli via ``python -m``

>>> python -m ymp.cli show references -v

Note that we try to behave just like running ``ymp`` from the command
line, rewriting argv[0] and setting the click program name so that
shell expansion works. This is done mostly to assist unit tests.

"""

import sys
from ymp.cli import main

if __name__ == "__main__":
    sys.argv[0] = "ymp"
    sys.exit(main(prog_name="ymp"))
