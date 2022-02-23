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


def __load_logging_config():
    config_paths = []

    # The users which have used the pymongowatch-mask-installer have a
    # symlink of this file, so it is important to use realpath to
    # resolve the links.
    current_path = pathlib.Path(os.path.realpath(__file__))

    # In a Python virtual environment the etc is inside the venv
    # directory, so to find the "etc", we start from the directory of
    # the pymongo.py itself and go back to its parents step by step
    # and search for an "etc". We continue this until we reach the
    # lowest priority i.e. root or to be compatible with Windows
    # systems "anchor".
    while pathlib.Path(current_path.anchor) != current_path:
        current_path = current_path.parent

        # As we iterate in the while loop we are going from the
        # highest priority to the lowest; so we add paths here in this
        # direction, too. From the highest priority to the lowest;
        # later we can reverse the whole list to get the desired
        # result.
        config_paths.extend(pathlib.Path(
            current_path, "etc", "pymongowatch", "conf.d").glob("*.yaml"))
        config_paths.append(pathlib.Path(
            current_path, "etc", "pymongowatch", "pymongowatch.yaml"))
        config_paths.append(pathlib.Path(
            current_path, "etc", "pymongowatch.yaml"))

    # We will load the pymongowatch logging yaml configurations from
    # the lowest priority to the highest, so the incremental
    # configuration
    # [https://docs.python.org/3/library/logging.config.html#incremental-configuration]
    # can be used. Note that, by default disable_existing_loggers is
    # enabled for the dictConfig. So the newer configuration will
    # replace the old configuration unless
    # disable_existing_loggers=false explicitly specified in the
    # configuration file.
    config_paths.reverse()

    config_paths.append(
        pathlib.Path(pathlib.Path.home(), ".pymongowatch.yaml"))

    for config_path in config_paths:
        if config_path.is_file():
            try:
                with open(config_path) as fp:
                    config_dict = yaml.load(fp, Loader=yaml.CLoader)

                logging.config.dictConfig(config_dict)
            except Exception as exp:
                warnings.warn(f"Cannot load {config_path}: {exp}")


__load_logging_config()
if not logging.getLogger("pymongo.watcher").handlers:
    warnings.warn(
        "No handler is configured for pymongo.watcher in the configuration "
        "file. The deafult handler will be used.")
    real_pymongo.watcher.add_logging_handlers()
