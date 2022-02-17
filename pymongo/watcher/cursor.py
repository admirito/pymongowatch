#!/usr/bin/env python3

"""
This module implements WatchCursor class that extends
pymongo.cursor.Cursor to collect query logs. You can enable this class
as the result of pymongo.collection operators such as `find` by
calling the monkey-patching method `watch_patch_pymongo`:

  WatchCursor.watch_patch_pymongo()

  logger = logging.getLogger("pymongo.watcher.cursor")
  # Add logger handlers as you like; although it is recommended to use
  # logging.handlers.QueueHandler with a
  # pymongo.watcher.logger.WatchQueue and a custom logging.Formatter
  # e.g. logging.Formatter("{asctime} {name} - {watch}", style="{")

  client = MongoClient()
  list(client.dbname.coll.find({"foo": 1}))
  list(client.dbname.coll.find({"bar": 2}))
"""

import time
from datetime import datetime

import pymongo

from .base import BaseWatcher
from .logger import WatchMessage, log


class WatchCursor(pymongo.cursor.Cursor, BaseWatcher):
    """
    A cursor / iterator over Mongo query results just like
    pymongo.cursor.Cusrsor class but it can also collect logs for
    applied queries.
    """

    # TODO
    _watch_default_fields = (
        "CreateTime", "DB", "Collection", "Query", "RetrieveTime",
        "RetrievedCount")

    # TODO
    _watch_default_delay_sec = 600

    def rewind(self):
        """
        Call rewind on the origianl pymongo's Cursor to rewind this cursor
        to its unevaluated state.
        """
        super().rewind()
        del self._watch_log

    def next(self):
        """
        Call next on the origianl pymongo's Cursor to advance the cursor.
        """
        first_time = False

        if not hasattr(self, "_watch_log"):
            self._watch_log = WatchMessage.make(
                {"CreateTime": datetime.now(),
                 "LastRetrievedTime": None,
                 "Query": self._Cursor__spec,
                 "DB": self.collection.database.name,
                 "Collection": self.collection.name,
                 "RetrieveTime": 0,
                 "RetrievedCount": 0},
                default_keys=self._watch_default_fields,
                ready=False,
                delay_sec=self._watch_default_delay_sec)
            first_time = True

        self._watch_log["LastRetrievedTime"] = datetime.now()

        _start = time.time()
        try:
            result = super().next()
        finally:
            _end = time.time()
            self._watch_log["RetrieveTime"] += _end - _start
            self._watch_log["RetrievedCount"] = self.retrieved
            if first_time:
                log(__name__, self._watch_log)

        return result

    __next__ = next

    def close(self):
        """
        Call close on the origianl pymongo's Cursor to explicitly close /
        kill this cursor.
        """
        super().close()

        try:
            self._watch_log.set_ready()
        except Exception:
            pass

    @classmethod
    def watch_patch_pymongo(cls):
        """
        Monkey patch pymongo methods to use WatchCursor
        """
        super().watch_patch_pymongo()

        cls.__real_pymongo_cursor = pymongo.cursor.Cursor

        pymongo.cursor.Cursor = WatchCursor
        pymongo.collection.Cursor = WatchCursor

    @classmethod
    def watch_unpatch_pymongo(cls):
        """
        Undo pymongo monkey patching and use the pymongo's original Cursor
        """
        super().watch_unpatch_pymongo()

        try:
            pymongo.cursor.Cursor = cls.__real_pymongo_cursor
            pymongo.collection.Cursor = cls.__real_pymongo_cursor
        except AttributeError:
            pass
