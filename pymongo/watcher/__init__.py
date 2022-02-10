#!/usr/bin/env python3

from .cursor import PatchWatchers
from .cursor import WatchCursor

__version__ = "0.1.0"

# create some shortcuts for WatchCursor class methods
all_logs = WatchCursor.watch_all_logs
clear_logs = WatchCursor.watch_clear_logs
follow_logs = WatchCursor.watch_follow_logs


def set_query_normalizer(func):
    """
    Set pymongowatch.WatchCursor.watch_query_normalizer to the
    provided `func`. The `func` must be a callable with exactly on
    argument `query` which will be used to generate the
    {normalized_query} template inside the pymongowatch log.
    """
    assert callable(func)
    WatchCursor.watch_query_normalizer = staticmethod(func)
