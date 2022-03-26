#!/usr/bin/env python3

"""
Implement base types for pymongo activity watchers that could be
inherited from.
"""

import contextlib
import functools
import inspect
import logging.config
import os
from datetime import datetime

from .logger import WatchMessage, log


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

    _watch_name = __name__

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


class OperationWatcher(BaseWatcher):
    watch_operation_result = {
        "enable": True,
        "to": "_result",
        "cast": None,
    }

    watch_operation_undefined_arguments = {
        "enable": False,
    }

    watch_operations = {}

    def _before_operation(
            self,
            message,
            operation_defined_arguments,
            result_enable,
            result_field,
            undefined_arguments_enable,
            undefined_arguments_cast,
            operation_arguments):
        """
        """
        for arg_name, arg_spec in operation_defined_arguments.items():
            if result_enable and arg_name == result_field:
                continue

            field_name = arg_spec.get("to", arg_name)

            field_value = arg_spec.get("value",
                                       operation_arguments.get(arg_name))

            cast = arg_spec.get("cast")
            if cast is not None:
                with contextlib.suppress(Exception):
                    field_value = cast(field_value)

            if field_name is not None:
                message[field_name] = field_value

        if undefined_arguments_enable:
            for arg_name, arg_value in operation_arguments.items():
                if arg_name not in operation_defined_arguments:
                    if undefined_arguments_cast:
                        with contextlib.suppress(Exception):
                            arg_value = undefined_arguments_cast(arg_value)

                    message[arg_name] = arg_value

        log(self._watch_name, message, level=self._watch_log_level_first)

    def _after_operation(
            self,
            message,
            result,
            result_enable,
            result_field,
            result_cast):
        """
        """
        if result_enable:
            result_value = result

            if result_cast:
                with contextlib.suppress(Exception):
                    result_value = result_cast(result)

            if result_field is not None:
                message[result_field] = result_value

        message.finalize()

        log(self._watch_name, message, level=self._watch_log_level_final)

    def _operation(self, operation_name, *args, **kwargs):
        """
        """
        operation_method = getattr(super(), operation_name)

        operation_defined_arguments = self.watch_operations.get(
            operation_name, {})

        empty = object()

        result_enable = self.watch_operation_result.get("enable", True)
        result_field = self.watch_operation_result.get("to", "(result)")
        result_cast = self.watch_operation_result.get("cast")
        result_value = self.watch_operation_result.get("value", empty)
        if result_enable:
            result_spec = operation_defined_arguments.get(result_field, {})
            result_field = result_spec.get("to", result_field)
            result_cast = result_spec.get("cast", result_cast)
            result_value = result_spec.get("value", result_value)

        undefined_arguments_enable = \
            self.watch_operation_undefined_arguments.get("enable", False)
        undefined_arguments_cast = \
            self.watch_operation_undefined_arguments.get("cast")

        message = WatchMessage(
            EndTime=None,
            Duration=None,
            Operation=operation_name)

        message.default_keys = self.watch_default_fields

        message.set_timeout(self._watch_timeout_sec)
        message.timeout_log_level = self._watch_log_level_timeout

        operation_signature = inspect.signature(operation_method)

        operation_arguments = operation_signature.bind(*args, **kwargs)
        operation_arguments.apply_defaults()
        operation_arguments = operation_arguments.arguments

        self._before_operation(
            message=message,
            operation_defined_arguments=operation_defined_arguments,
            result_enable=result_enable,
            result_field=result_field,
            undefined_arguments_enable=undefined_arguments_enable,
            undefined_arguments_cast=undefined_arguments_cast,
            operation_arguments=operation_arguments,
        )

        result = None

        _start = message.get("StartTime", datetime.now())

        try:
            result = operation_method(*args, **kwargs)
        finally:
            _end = datetime.now()

            message["Duration"] = (_end - _start).total_seconds()
            message["EndTime"] = _end

            self._after_operation(
                message=message,
                result=result if result_value is empty else result_value,
                result_enable=result_enable,
                result_field=result_field,
                result_cast=result_cast,
            )

        return result

    def __getattribute__(self, name):
        if name == "watch_operations":
            return type(self).watch_operations

        parent_attribute = super().__getattribute__(name)

        if name in self.watch_operations:
            @functools.wraps(parent_attribute)
            def new_method(*args, **kwargs):
                return self._operation(name, *args, **kwargs)

            return new_method

        return parent_attribute
