#!/usr/bin/env python3

"""
A collection of functions intended to be used as the "cast"
function for :class:`pymogno.watcher.bases.OperationField` which can
be used to transform an operation field to something useful for the
logging.
"""

import pymongo

from .cursor import WatchCursor


def one_if_not_none(value):
    """
    Returns the integer 0 if value is None and 1 if not.
    """
    return 0 if value is None else 1


def to_watch_cursor(cursor, watch_message=None):
    """
    TODO: add details when this mehod has side effects on cursor.
    """
    if not isinstance(cursor, pymongo.cursor.Cursor):
        raise TypeError(f"to_watch_cursor argument must be a "
                        f"pymongo Cursor, not {type(cursor)}")

    result = (cursor if isinstance(cursor, WatchCursor)
              else WatchCursor.clone(cursor))

    if watch_message is not None:
        result._watch_log = watch_message
        # TODO: update default_keys and timeout log level in collecion
        # operation
        result._watch_log.default_keys = cursor.watch_default_fields
        result._watch_log.timeout_log_level = cursor._watch_log_level_timeout

    return result
