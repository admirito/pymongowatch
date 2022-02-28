#!/usr/bin/env python3

"""
Implement base types for pymongo activity watchers that could be
inherited from.
"""

import contextlib


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

    # The default timeout in seconds for WatchMessage `timeout_on`
    _watch_default_delay_sec = 600

    @classmethod
    def watch_dictConfig(cls, config):
        """
        Configure the watcher using a dictionary. Similar to
        :func:`logging.config.dictConfig`. The configuration will be
        extracted from the "global" key inside the "watchers"
        dictionary.

        :Parameters:
         - config: configuration dictionary
        """
        _global = config.get("watchers", {}).get("global", {})

        with contextlib.suppress(KeyError):
            cls._watch_default_delay_sec = _global["default_delay_sec"]

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
