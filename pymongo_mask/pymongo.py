#!/usr/bin/env python3

"""
This is a mask package for pymongo. It is a drop-in replacement for
pymongo but pymongowatch is enabled by default.

This module has to be placed in sys.path with more priority than the
real pymongo, so it can be enabled as a mask for it and leverages the
real pymongo's functionality.
"""

import importlib.machinery
import logging.config
import os
import pathlib
import sys
import warnings

try:
    import yaml
except ModuleNotFoundError:
    # pyyaml is not installed; but we can go on without loading yaml
    # configuration
    pass


class __PymongoMaskImporter(importlib.machinery.PathFinder):
    """
    This class implements a PathFinder for sys.meta_path that can
    import the real pymongo with lower priority in sys.path.
    """
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "pymongo":
            curdir = os.path.realpath(os.path.dirname(__file__))
            path = [i for i in sys.path if i and os.path.realpath(i) != curdir]
            return importlib.machinery.PathFinder().find_spec("pymongo", path)

        return super().find_spec(fullname, path, target)


# We have to remove "pymongo" from python's module cache, sys.modules,
# otherwise the next import statement for pymongo will just return the
# already existing item in the cache which is this module itself not
# the real pymongo.
__this_moudle = sys.modules[__name__]
del sys.modules[__name__]

# Import the real pymongo by leveraging our custom importer.
__mask_importer = __PymongoMaskImporter()
sys.meta_path.insert(0, __mask_importer)
try:
    # let's store the real pymongo in case someone needs it.
    import pymongo as real_pymongo

    try:
        import pymongo.watcher
    except ModuleNotFoundError as exp:
        raise ImportError(
            "pymongowatch mask is imported but watcher module is not "
            "importable.") from exp

    # import all the stuff from pymongo namespace so this module can
    # be used as a replica of pymongo.
    from pymongo import *
    from pymongo import __version__
finally:
    sys.meta_path.remove(__mask_importer)

# With __path__ exactly set to pymogno.__path__, now every sub-module
# in the real pymongo can be imported from this module (which is now a
# full package).
__path__ = real_pymongo.__path__

# Now it's time to get back this module to the python's cache.
sys.modules[__name__] = __this_moudle

# Enable watcher module patches.
real_pymongo.watcher.patch_pymongo()

# Loading pymongowatch logging yaml configurations from the lowest
# priority to the highest, so the incremental configuration
# [https://docs.python.org/3/library/logging.config.html#incremental-configuration]
# can be used.
for config_path in [
        "/etc/pymongowatch.yaml",
        "/etc/pymongowatch/pymongowatch.yaml",
        os.path.join(pathlib.Path.home(), ".pymongowatch.yaml")]:
    if os.path.isfile(config_path):
        try:
            with open(config_path) as fp:
                config_dict = yaml.load(fp, Loader=yaml.CLoader)

            logging.config.dictConfig(config_dict)
        except Exception as exp:
            warnings.warn(f"Cannot load {config_path}: {exp}")
            import traceback
            traceback.print_exc()

if not logging.getLogger("pymongo.watcher").handlers:
    warnings.warn(
        "No handler is configured for pymongo.watcher in the configuration "
        "file. The deafult handler will be used.")
    real_pymongo.watcher.add_logging_handlers()
