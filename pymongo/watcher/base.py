#!/usr/bin/env python3

"""
Implement base types for pymongo activity watchers that could be
inherited from.
"""

import contextlib
import logging.config
import os


class BaseWatchConfigurator(logging.config.BaseConfigurator):
    """
    An extension of :class:`logging.config.BaseConfigurator` which
    adds methods for "watchers" configuration with
    :func:`pymongo.watcher.dictConfig` method.
    """
    def configure_watch_global(self, cls):
        """
        Apply configurations from "global" section to `cls` i.e. a watcher
        class.

        :Parameters:
         - cls: the watcher class
        """
        config = self.config

        _global = config.get("watchers", {}).get("global", {})

        with contextlib.suppress(Exception):
            cls._watch_timeout_sec = int(_global["timeout_sec"])

        log_level = _global.get("log_level", {})
        levels = self.configure_watch_log_level(log_level)
        for log_type in ["first", "update", "final", "timeout"]:
            with contextlib.suppress(KeyError):
                setattr(cls, f"_watch_log_level_{log_type}", levels[log_type])

        with contextlib.suppress(Exception):
            for csv_item in _global.get("csv", []):
                file_name = self.convert(csv_item.get("file"))
                headers = csv_item.get("add_headers_if_empty")

                if file_name and headers:
                    from .logger import WatchMessage
                    csv_columns = ",".join(WatchMessage.csv_columns())
                    headers = headers.replace("{watch.csv}", csv_columns)
                    headers = headers.rstrip("\n") + "\n"

                    with contextlib.suppress(IOError, OSError):
                        if not os.path.exists(file_name) or \
                               os.path.getsize(file_name) == 0:
                            with open(file_name, "w") as fp:
                                fp.write(headers)

    def configure_watch_log_level(self, config):
        """
        Given a dictionary `config` i.e. "log_level" section of
        "watchers", this method will return a mapping from all the
        given keys in the dictionary to the resolved log level.

        For example, if `config` is {"final": "cfg://foo.bar"} and
        "foo.bar" in the confiuration is the string "INFO",
        :attr:`logging.INFO` which has the value 20 as a :class:`int`
        will be resolved for "final". So the result will be {"final":
        20}.

        :Parameters:
         - config: the log_level dictionary in the configuration
        """
        result = {}

        for key, level in config.items():
            with contextlib.suppress(Exception):
                result[key] = logging._checkLevel(self.convert(level))

        return result


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

    # Defaults for global configurations
    _watch_timeout_sec = 600
    _watch_log_level_first = logging.DEBUG
    _watch_log_level_update = logging.DEBUG
    _watch_log_level_final = logging.INFO
    _watch_log_level_timeout = logging.INFO

    @classmethod
    def watch_dictConfig(cls, config, add_globals=True):
        """
        Configure the watcher using a dictionary. Similar to
        :func:`logging.config.dictConfig`. The configuration will be
        extracted from the the "watchers" key in the `config`
        dictionary. The base class only implements the "global"
        section. The extended classes should implement their own
        sections.

        :Parameters:
         - config: configuration dictionary
         - add_globals (optional): A boolean indicating weather to
           load the "global" section or not.
        """
        cls._watch_configurtor = BaseWatchConfigurator(config)

        if add_globals:
            cls._watch_configurtor.configure_watch_global(cls)

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
