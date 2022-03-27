#!/usr/bin/env python3

"""
A collection of functions intended to be used as the "cast"
function for :class:`pymogno.watcher.bases.OperationField` which can
be used to transform an operation field to something useful for the
logging.
"""


def one_if_not_none(value):
    """
    Returns the integer 0 if value is None and 1 if not.
    """
    return 0 if value is None else 1
