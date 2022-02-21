#!/usr/bin/env python3

"""
Implement base types for pymongo activity watchers that could be
inherited from.
"""


class BaseWatcher:
    """
    The shared code between all the watcher classes will be maintained
    here.
    """

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
