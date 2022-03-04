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

import contextlib
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

    watch_default_fields = (
        "DB", "Collection", "Query", "RetrieveTime", "RetrievedCount")

    watch_all_fields = (
        "CreateTime", "LastRetrievedTime", "DB", "Collection", "Query",
        "RetrieveTime", "RetrievedCount")

    # The default timeout in seconds for WatchMessage `timeout_on`
    _watch_default_delay_sec = 600

    def rewind(self):
        """
        Call rewind on the origianl pymongo's Cursor to rewind this cursor
        to its unevaluated state.
        """
        super().rewind()
        with contextlib.suppress(AttributeError):
            del self._watch_log

    def next(self):
        """
        Call next on the origianl pymongo's Cursor to advance the cursor.
        """
        if not hasattr(self, "_watch_log"):
            self._watch_log = WatchMessage(
                LastRetrievedTime=None,
                DB=self.collection.database.name,
                Collection=self.collection.name,
                Query=self._Cursor__spec,
                RetrieveTime=0,
                RetrievedCount=0)
            self._watch_log.default_keys = self.watch_default_fields

        _start = time.time()
        try:
            # In pymongo after version 4.0 the `close` method might be
            # called after calling the `next` method
            # automatically. So, we have to make sure redudant logging
            # will not happen. Also we can only update parameters such
            # as `RetrieveTime` and `RetrievedCount` after calling the
            # super().next().
            #
            # So we use an instance attribute to mark the instance as
            # it is in the middle of calling `next` method so the
            # `close` method can skip calling `finalize` and `log`
            # methods. Then we must call them in this method instead.
            self._watch_cursor_next_is_in_progress = True

            final_state_before_next = self._watch_log.final

            result = super().next()

            # If StopIteration didn't occur:
            self._watch_log["LastRetrievedTime"] = datetime.now()
        finally:
            _end = time.time()

            del self._watch_cursor_next_is_in_progress

            if not final_state_before_next:
                self._watch_log["RetrieveTime"] += _end - _start
                self._watch_log["RetrievedCount"] = self.retrieved
                self._watch_log["Iteration"] += 1
                self._watch_log.set_timeout(self._watch_default_delay_sec)

                if getattr(self, "_watch_cursor_skipped_finalization", False):
                    # If `close` method skipped finalization we have
                    # to call it here instead.
                    del self._watch_cursor_skipped_finalization
                    self._watch_log.finalize()

                log(__name__, self._watch_log)

        return result

    __next__ = next

    def close(self):
        """
        Call close on the origianl pymongo's Cursor to explicitly close /
        kill this cursor.
        """
        super().close()

        if getattr(self, "_watch_cursor_next_is_in_progress", False):
            # We are in the middle of `next` method. The `next` method
            # will take care of finalization itself. We just have to
            # mark the instance:
            self._watch_cursor_skipped_finalization = True
        else:
            with contextlib.suppress(Exception):
                if not self._watch_log.final:
                    self._watch_log["Iteration"] += 1
                    self._watch_log.finalize()
                    log(__name__, self._watch_log)

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
