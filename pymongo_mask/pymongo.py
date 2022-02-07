#!/usr/bin/env python3

"""
This is a mask package for pymongo. It is a drop-in replacement for
pymongo and it works exactly like pymongowatch.
"""

import importlib
import importlib.machinery
import os
import sys
import types


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
__this_moudle = sys.modules["pymongo"]
del sys.modules["pymongo"]

# Import the real pymongo by leveraging our custom importer.
__mask_importer = __PymongoMaskImporter()
sys.meta_path.insert(0, __mask_importer)
try:
    # let's store the real pymongo in case someone need it.
    import pymongo as real_pymongo
    # import all the stuff from pymongo namespace so this module can
    # be used as a replica of pymongo.
    from pymongo import *
finally:
    sys.meta_path.remove(__mask_importer)

# With __path__ exactly set to pymogno.__path__, now every sub-module
# in the real pymongo can be imported from this module (which is now a
# full package).
__path__ = real_pymongo.__path__

# Now it's time to get back this module to the python's cache.
sys.modules["pymongo"] = __this_moudle

# `watcher` is a module that will provide shortcuts for useful methods
# from pymongowatch.
watcher = types.ModuleType("watcher")


def __initialize_watchers():
    """
    Add shortcuts from pymongowatch to watcher module
    """
    global watcher

    import pymongowatch
    pymongowatch.PatchWatchers()

    for cur_name, name in [
            ("watch_all_logs", "all_logs"),
            ("watch_clear_logs", "clear_logs"),
            ("watch_follow_logs", "follow_logs")]:
        setattr(watcher, name,
                getattr(pymongowatch.WatchCursor, cur_name))

    def __set_query_normalizer(func):
        """
        Set pymongowatch.WatchCursor.watch_query_normalizer to the provided
        `func`. The `func` must be a callable with exactly on argument
        `query` which will be used to generate the {normalized_query}
        template inside the pymongowatch log.
        """
        assert callable(func)
        pymongowatch.WatchCursor.watch_query_normalizer = staticmethod(func)

    watcher.set_query_normalizer = __set_query_normalizer


watcher._initialize_watchers = __initialize_watchers

try:
    __initialize_watchers()
except AttributeError:
    # If pymognowatch imports pymongo while this module (the pymongo
    # mask) is enabled we my get AttributeError: partially initialized
    # module 'pymongowatch' has no attribute 'PatchWatchers' (most
    # likely due to a circular import); to overcome this issue,
    # pymongowatch has to call `_initialize_watchers` itself.
    pass
