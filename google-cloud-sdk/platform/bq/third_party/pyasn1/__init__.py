#!/usr/bin/env python
import sys

# http://www.python.org/dev/peps/pep-0396/
__version__ = '0.4.2'

if sys.version_info[:2] < (2, 4):
    raise RuntimeError('PyASN1 requires Python 2.4 or later')
