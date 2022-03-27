#!/usr/bin/env python3

"""
Implement base types for pymongo activity watchers that could be
inherited from.
"""

import contextlib
import dataclasses
import functools
import inspect
import logging.config
import os
from datetime import datetime
from typing import Any, Callable, Optional, Union

from .logger import WatchMessage, log


class BaseWatchConfigurator(logging.config.BaseConfigurator):
    """
    An extension of :class:`logging.config.BaseConfigurator` which
    adds methods for "watchers" configuration with
    :func:`pymongo.watcher.dictConfig` method.
    """
    def configure_watch_global(self, cls, sub_section="global"):
        """
        Apply configurations from "global" section to `cls` i.e. a watcher
        class.

        :Parameters:
         - cls: the watcher class
        """
        config = self.config

        section = config.get("watchers", {}).get(sub_section, {})

        with contextlib.suppress(Exception):
            cls._watch_timeout_sec = int(section["timeout_sec"])

        log_level = section.get("log_level", {})
        levels = self.configure_watch_log_level(log_level)
        for log_type in ["first", "update", "final", "timeout"]:
            with contextlib.suppress(KeyError):
                setattr(cls, f"_watch_log_level_{log_type}", levels[log_type])

        try:
            default_fields = tuple(key for key in
                                   section.get("default_fields", [])
                                   if key in cls.watch_all_fields)
        except Exception:
            default_fields = ()

        if default_fields:
            cls.watch_default_fields = default_fields

        with contextlib.suppress(Exception):
            for csv_item in section.get("csv", []):
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
    def watch_dictConfig(cls, config, sub_section=None, add_globals=True):
        """
        Configure the watcher using a dictionary. Similar to
        :func:`logging.config.dictConfig`. The configuration will be
        extracted from the the "watchers" key in the `config`
        dictionary. The base class only implements the "global"
        section. The extended classes should implement their own
        sections, but if `sub_section` is provided the common
        configuration applicable to all the watchers will be
        configured for the given sub-section.

        :Parameters:
         - `config`: configuration dictionary
         - `add_globals` (optional): A boolean indicating weather to
           load the "global" section or not.
         - `sub_section` (optional): Name of a sub-section in the
           watchers section to load the common configurations
        """
        cls._watch_configurtor = BaseWatchConfigurator(config)

        if add_globals:
            cls._watch_configurtor.configure_watch_global(cls)

        if sub_section is not None:
            cls._watch_configurtor.configure_watch_global(
                cls, sub_section=sub_section)

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


class OperationWatcherConfigurator(BaseWatchConfigurator):
    def configure_watch_operation_field(self, config):
        """
        Given a dictionary `config` i.e. the definition for a field (an
        operation argument, result or general definition for undefined
        fields) this method will return the equivalent
        :class:`OperationField`.

        "log_level" section of
        "watchers", this method will return a mapping from all the
        given keys in the dictionary to the resolved log level.

        For example, if `config` is {"cast": "builtins.len"} the
        :class:`OperationField` result object will have the
        :func:`len` as the cast.

        :Parameters:
         - config: the field definition dictionary in the configuration
        """
        result = OperationField()

        with contextlib.suppress(Exception):
            result.to = self.convert(config["to"])

        with contextlib.suppress(Exception):
            cast = self.convert(config["cast"])
            if isinstance(cast, str):
                cast = self.resolve(cast)
            result.cast = cast

        with contextlib.suppress(Exception):
            result.value = self.convert(config["value"])

        return result


class UnsetType:
    """
    """
    __instance = None

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)

        return cls.__instance

    def __repr__(self):
        return "<Unset>"


Unset = UnsetType()


@dataclasses.dataclass
class OperationField:
    """
    """
    to: Union[None, UnsetType, str] = Unset
    cast: Optional[Callable[[Any], Any]] = None
    value: Any = Unset


class OperationWatcher(BaseWatcher):
    """
    """

    watch_operation_result = OperationField(to="_result")

    watch_operation_undefined_arguments = OperationField(to="")

    watch_operations = {}

    def _before_operation(
            self,
            message,
            operation_defined_arguments,
            result_field,
            undefined_arguments_field,
            operation_arguments):
        """
        """
        for arg_name, arg_spec in operation_defined_arguments.items():
            if result_field.to is not Unset and arg_name == result_field.to:
                continue

            field_name = arg_name if arg_spec.to is Unset else arg_spec.to

            field_value = operation_arguments.get(arg_name) \
                if arg_spec.value is Unset else arg_spec.value

            cast = arg_spec.cast
            if cast is not None:
                with contextlib.suppress(Exception):
                    field_value = cast(field_value)

            if field_name:
                message[field_name] = field_value

        if undefined_arguments_field.to is not None:
            for arg_name, arg_value in operation_arguments.items():
                if arg_name not in operation_defined_arguments:
                    if undefined_arguments_field.cast is not None:
                        with contextlib.suppress(Exception):
                            arg_value = undefined_arguments_field.cast(
                                arg_value)

                    arg_name = arg_name if undefined_arguments_field.to is \
                        Unset else undefined_arguments_field.to
                    if arg_name:
                        message[arg_name] = arg_value

        log(self._watch_name, message, level=self._watch_log_level_first)

    def _after_operation(
            self,
            message,
            result_field,
            result):
        """
        """
        if result_field.to is Unset:
            result_field = dataclasses.replace(result_field, to="(result)")

        result_value = result

        if result_field.cast:
            with contextlib.suppress(Exception):
                result_value = result_field.cast(result)

        if result_field.to:
            message[result_field.to] = result_value

        message.finalize()

        log(self._watch_name, message, level=self._watch_log_level_final)

    def _operation(self, operation_name, *args, **kwargs):
        """
        """
        operation_method = getattr(super(), operation_name)

        operation_defined_arguments = self.watch_operations.get(
            operation_name, {})

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
            result_field=self.watch_operation_result,
            undefined_arguments_field=self.watch_operation_undefined_arguments,
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
                result_field=self.watch_operation_result,
                result=result,
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

    @classmethod
    def watch_dictConfig(cls, config, sub_section=None, add_globals=True):
        """
        Extends the `watch_dictConfig` method in :class:`BaseWatcher` to
        also load the `result`, `undefined_arguments` and `operations`
        field definitions from the provided `sub_section`.

        :Parameters:
         - `config`: configuration dictionary
         - `add_globals` (optional): A boolean indicating weather to
           load the "global" section or not.
         - `sub_section` (optional): Name of a sub-section in the
           watchers section to load the configurations
        """
        super().watch_dictConfig(config, sub_section=sub_section,
                                 add_globals=add_globals)

        cls._watch_configurtor = OperationWatcherConfigurator(config)

        if sub_section is None:
            return

        section = config.get("watchers", {}).get(sub_section, {})

        result = section.get("result")
        if result is not None:
            cls.watch_operation_result = \
                cls._watch_configurtor.configure_watch_operation_field(result)

        undefined_arguments = section.get("undefined_arguments")
        if undefined_arguments is not None:
            cls.watch_operation_undefined_arguments = \
                cls._watch_configurtor.configure_watch_operation_field(
                    undefined_arguments)

        for operation, args in section.get("operations", {}).items():
            for arg, spec in args.items():
                cls.watch_operations.setdefault(operation, {})[arg] = \
                    cls._watch_configurtor.configure_watch_operation_field(
                        spec)
