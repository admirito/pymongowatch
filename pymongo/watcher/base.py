#!/usr/bin/env python3

"""
Implement base types for pymongo activity watchers that could be
inherited from.
"""


class BaseWatcher:
    """
    The shared code between all the watcher classes will be maintained
    here.

    :Attributes:
     - `watch_default_fields`: The `default_keys` for WatchMessage
     - `watch_all_fields`: List of all the possible log fields
    """
    watch_default_fields = ()
    watch_all_fields = ()

    @classmethod
    def watch_patch_pymongo(cls):
        """
        This is an abstract method in which the implementer must
        patch/update pymongo internals to enable the watcher
        """
        pass

    @classmethod
    def watch_unpatch_pymongo(cls):
        """
        This is an abstract method in which the implementer must
        patch/update pymongo internals to disable the watcher
        """
        pass
