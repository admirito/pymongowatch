#!/usr/bin/env python3

"""
Filter instances are used to perform arbitrary filtering of
LogRecords. Loggers and Handlers can optionally use Filter instances
to filter records as desired.

This module implement useful filters based on :class:`logging.Filter`
for logging with `pymongo.watcher` logger.
"""

import logging


class ExpressionFilter(logging.Filter):
    """
    A filter based on :class:`logging.Filter` class to filter by a
    python expression as a string using :func:`eval`.

    The attributes of the message inside the
    :class:`logging.LogRecord` are available as variables of the
    expression. For example for a
    :class:`pymongo.watcher.logger.WatchMessage` you can both use the
    `watch` attribute and the attributes inside `watch` itself:

      ExpressionFilter("watch['RetrieveTime'] < 10")
      ExpressionFilter("RetrieveTime > 20 and Query == {}")
    """
    def __init__(self, expression, exception_result=False):
        """
        Initialize a filter based on a python expression.

        By default `exception_result` is `False`. So the LogRecords
        will be filtered if the expression raise an exception during
        the evaluation.

        :Parameters:
         - `expression`: the filter expression as a :class:`str` string
         - `excpetion_result` (optional): the reuslt of :meth:`filter`
           method in case of exception while evaluting the
           `expression`
        """
        self._expression = expression
        self._exception_result = exception_result

    def filter(self, record):
        """
        Determine if the specified record is to be logged by evaluating
        the expression.

        Returns eval(expression) except when an exception occurs, in
        which case the requested result for excpetions will be
        returned.

        :Parameters:
         - `record`: the input :class:`logging.LogRecord` instance
        """
        watch = getattr(record, "watch", {})
        _locals = {"_record": record, "_watch": watch, **watch}
        try:
            return eval(self._expression, _locals, _locals)
        except Exception:
            return self._exception_result


class ExecuteFilter(logging.Filter):
    """
    A filter based on :class:`logging.Filter` class to apply changes to
    the LogRecord (and possibly determine the filtering/passing the
    record) by some python source code as a string using :func:`exec`.

    The attributes of the message inside the
    :class:`logging.LogRecord` are available as local variables in the
    source code and can be modified. For example for a
    :class:`pymongo.watcher.logger.WatchMessage` you can both use the
    `watch` attribute and the attributes inside `watch` itself:

      ExecuteFilter("watch['RetrieveTime'] += 1")
      ExecuteFilter('''\
      import numbers as _numbers
      _mask = (lambda d:
          {k: _mask(v) for k, v in d.items()} if isinstance(d, dict) else
          [_mask(i) for i in d] if isinstance(d, list) else
          "x" * len(d) if isinstance(d, str) else
          0 if isinstance(d, _numbers.Number) else None)
      Query = _mask(Query)
      ''')

    The final value of all the local variables which their name does
    not start with `_` will be added/updated in the LogRecord.

    The special variable `_reuslt` could be defined and set to False
    to filter the LogRecord completely.
    """
    def __init__(self, execute, exception_result=True):
        """
        Initialize a filter based on a python execution code.

        By default `exception_result` in contrast with
        :class:`pymongo.watcher.filters.ExpressionFilter` is
        `True`. So the LogRecords will not be filtered if the
        source code raise an exception during the execution.

        :Parameters:
         - `execute`: the python execution code as a :class:`str` string
         - `excpetion_result` (optional): the reuslt of :meth:`filter`
           method in case of exception while executing the `execute`
        """
        self._execute = execute
        self._exception_result = exception_result

    def filter(self, record):
        """
        Execute the instance `execute` source code with :func:`exec` to
        possibly modify the LogRecord. Optionally, `_result` local
        variable could be set in the source code to determine the
        filtering/passing of the LogRecord.

        Returns True except when an exception occurs and
        `exception_result` is False or when the `_result` variable in
        the execution code explicitly set to False.

        :Parameters:
         - `record`: the input :class:`logging.LogRecord` instance
        """
        watch = getattr(record, "watch", {})
        _locals = {"_record": record, "_watch": watch, **watch}
        try:
            # It is important to pass _locals as globals argument of
            # exec, too. So the user can implement a recursive
            # function and refer to it inside the function.
            exec(self._execute, _locals, _locals)
            for key, value in _locals.items():
                if not key.startswith("_"):
                    watch[key] = value
                    setattr(record, key, value)
        except Exception:
            return self._exception_result

        return _locals.get("_result", True)


class AddFieldAttributes(logging.Filter):
    _default_attributes = {}

    def __init__(self, name="", field_name="_result", attributes=None):
        """
        """
        super().__init__(name)

        self.main_field_name = field_name
        self.attributes = attributes or self._default_attributes

    def filter(self, record):
        """
        """
        result = super().filter(record)
        if not result or not hasattr(record, "watch"):
            return result

        try:
            main_field = record.watch[self.main_field_name]
        except KeyError:
            return result

        for attr_name, new_name in self.attributes.items():
            try:
                value = getattr(main_field, attr_name)
            except AttributeError:
                pass
            else:
                new_name = new_name or attr_name
                record.watch[new_name] = value

        return result


class AddPymongoResults(AddFieldAttributes):
    """
    """
    _default_attributes = {
        "matched_count": "MatchedCount",
        "inserted_count": "InsertedCount",
        "upserted_count": "UpsertedCount",
        "modified_count": "ModifiedCount",
        "deleted_count": "DeletedCount",
        "inserted_ids": "_InsertedIds",
    }

    def filter(self, record):
        """
        """
        result = super().filter(record)
        if not result or not hasattr(record, "watch"):
            return result

        if "_InsertedIds" in record.watch:
            record.watch["InsertedCount"] = len(record.watch["_InsertedIds"])
            record.watch.pop("_InsertedIds", None)

        none_attributes = [attr for attr in self._default_attributes.values()
                           if record.watch.get(attr) is None]
        record.watch.default_keys = tuple(
            i for i in record.watch.default_keys if i not in none_attributes)

        return result

