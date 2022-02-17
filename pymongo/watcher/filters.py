#!/usr/bin/env python3

"""
Filter instances are used to perform arbitrary filtering of LogRecords.

Loggers and Handlers can optionally use Filter instances to filter
records as desired.
"""

import logging


class ExpressionFilter(logging.Filter):
    """
    """
    def __init__(self, expression, exception_result=False):
        """
        Initialize a filter for based on a python expression.

        Initialize with the expression, together with a string
        containing the operator and the compared value. For example
        CompareFilter(RetrieveTime="> 20", Query="= {}")
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
        """
        watch = getattr(record, "watch", {})
        _locals = {"_record": record, "_watch": watch, **watch}
        try:
            return eval(self._expression, _locals, _locals)
        except Exception:
            return self._exception_result


class ExecuteFilter(logging.Filter):
    """
    """
    def __init__(self, execute, exception_result=True):
        """
        """
        self._execute = execute
        self._exception_result = exception_result

    def filter(self, record):
        """
        """
        watch = getattr(record, "watch", {})
        _locals = {"_record": record, "_watch": watch, **watch}
        try:
            exec(self._execute, _locals, _locals)
            for key, value in _locals.items():
                if not key.startswith("_"):
                    watch[key] = value
                    setattr(record, key, value)
        except Exception:
            return self._exception_result

        return _locals.get("_result", True)
