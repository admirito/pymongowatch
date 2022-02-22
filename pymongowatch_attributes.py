#!/usr/bin/env python3

"""
This module provides necessary attributes so that a python binary
distribution could be created from the source distribution.

A __version__ equivalent of the pymongo.watcher.__version__ value in
the project directory is available.

The "pymongo" directory in the project doesn't provide a __init__.py
(to be compatible with the real pymongo which has its own __init__.py)
and it works like PEP 420 Namespace Packages although the real pymongo
has __init__.py and will not comply with the PEP.

So this module provides a workaround for setup.cfg file to access
__version__ field.

Another important attribute is `long_description` which will be
extracted from the `README.org` file. Alas, PiPI `doesn't support Org
format
<https://packaging.python.org/en/latest/guides/making-a-pypi-friendly-readme/>`_.
But that dosn't mean we can't convert from .org to .rst (reStructured Text).

Another pitall is taht setup.cfg `doesn't support attr: for
long_description
<https://setuptools.pypa.io/en/latest/userguide/declarative_config.html#metadata>`_.
So we have no option but to use `file:`. So we have to create a real
file: `README.rst`.
"""

import os
import sys
import shutil
import subprocess


def __get_version():
    local_pymongo_path = os.path.join(os.path.dirname(__file__), "pymongo")
    sys.path.insert(0, local_pymongo_path)
    try:
        from watcher import __version__
    finally:
        sys.path.remove(local_pymongo_path)

    return __version__


def __get_long_description():
    fallback = True
    if shutil.which("pandoc"):
        try:
            proc = subprocess.Popen(["pandoc", "-t", "rst", "README.org"],
                                    stdout=subprocess.PIPE)
            stdout, _ = proc.communicate()
            long_description = stdout.decode("utf8")
        except OSError:
            sys.stderr.write(
                "Warning: Error while converting with README.rog with pandoc.\n"
                "         Falling back to literal blocks for the README.rst.\n")
        else:
            fallback = False
    else:
        sys.stderr.write(
            "Warning: pandoc not found.\n"
            "         In a debian system you can install pandoc with:\n"
            "             sudo apt install pandoc\n"
            "         Falling back to literal blocks for the README.rst.\n")

    if fallback:
        with open("README.org") as fp:
            readme_lines = fp.readlines()

        long_description = f"::\n\n  {'  '.join(readme_lines)}\n"

    return long_description


__version__ = __get_version()
long_description = __get_long_description()

with open("README.rst", "w") as fp:
    fp.write(
        f"..\n  This description is automatically generated from README.org "
        f"file.\n\n{long_description}")
