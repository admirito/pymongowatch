#!/usr/bin/env python3

"""
This module implements the required classes to emit logs in the
watchers.

All the :mod:`pymongo.watcher` modules use a singleton instance of
:class:`WatchLogEmitter` class returned by :func:`get_log_emitter` to
emit logs which internally emit logs by leveraging python
:class:`logging`. The :func:`log` function is also available as a
syntactic sugar for the emitter instance :meth:`log` method.

.. note:: Maybe "logging" was the proper name for this module, but to
   prevent confusion with the python :class:`logging` class, the name
   "logger" seems more appropriate.
"""

import contextlib
import csv
import heapq
import io
import json
import logging
import multiprocessing.managers
import queue
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime

import bson


def _repr(s):
    """
    Returns repr(s) but always use single-quotes

    Using single-quotes has the advantage that when combined with json
    strings (which only use double-quotes) less escaping is required.
    """
    # repr will use double-quotes if the sring contains single-quotes
    # but no double-quotes; so if we add a double-quote to the string
    # we can be sure repr will use single-quotes
    return "'" + repr(f'"{s}')[2:]


class WatchMessage(dict):
    """
    An extended :class:`dict` to be used as the log message in
    :mod:`pymongo.watcher` modules as explained in `Using arbitrary
    objects as messages in Python Logging HOWTO
    <https://docs.python.org/3/howto/logging.html#using-arbitrary-objects-as-messages>`_.

    Dot notation access to keys is also available, so the
    :class:`logging.Formatter` can access the inner key/values.

    :Attributes:
     - `default_keys`: a :class:`tuple` of keys to be represented in
       str format
     - `default_delimiter`: the separator between items in str format
     - `default_key_value_separator`: the separator between key/values
       in str format
     - `timeout_on`: A :class:`datetime.datetime` in which the message
       will be assumed final
    """

    default_keys = None
    default_delimiter = " "
    default_key_value_separator = "="
    timeout_log_level = logging.INFO

    def __init__(self, /, *args, **kwargs):
        """
        Initialize self. This mehod has exactly the same signature as
        standard dictionaries. It will automatically initialize the
        dictionary with the following items if not already provided as
        arguments:

         - WatchID: A unqiue ID from bson.ObjectId()
         - Iteration: 0
         - CreateTime: current datetime
        """
        now = datetime.now()

        # Insert default items as the first items because python
        # dictionaries pereserve order on insertion and serialization
        # methods like __str__ could have a more persistent
        # output. Also we have to add them with the lowest priority to
        # dict constructor because they could be changed if *args or
        # **kwargs has different values for them.
        super().__init__(
            {"WatchID": None,
             "Iteration": 0,
             "CreateTime": None,
             **dict(*args, **kwargs)})

        # Initialize the instance attributes
        self.timeout_on = now.timestamp()
        if args:
            arg = args[0]
            # Here exactly like Python's dict __init__ we expect at
            # most one argument. Again, just like the dict it could be
            # an iterator or a mapping. But if it is a WatchMessage
            # mapping we have to clone the instance attributes too.
            if isinstance(arg, WatchMessage):
                for key in ["timeout_on", "default_keys", "default_delimiter",
                            "default_key_value_separator"]:
                    setattr(self, key, getattr(arg, key))

        create_time = None

        if self["WatchID"] is None:
            _id = bson.ObjectId()
            self["WatchID"] = str(_id)
            # bson ObjectId generation_time is in UTC which we will
            # convert to local timezone.
            create_time = _id.generation_time.astimezone(tz=None)

        if self["CreateTime"] is None:
            try:
                create_time = (
                    create_time or
                    bson.ObjectId(self["WatchID"]
                                  ).generation_time.astimezone(tz=None))
            except Exception:
                # If all the possible methods to generate consistent
                # CreateTime compatible with WatchID failed, we use
                # the current time as the last resort.
                create_time = now

            self["CreateTime"] = create_time

    def set_timeout(self, seconds=None):
        """
        Set :attr:`timeout_on` to the datetime exactly `seconds` seconds
        after the current time or None if `seconds` is 0 or other
        values which will be evaluated to False.

        :Parameters:
         - `delay_sec` (optional): the seconds after the current time
           to be set as :attr:`timeout_on`
        """
        # Use None for 0 and other values which evaluate to False
        seconds = seconds or None

        self.timeout_on = seconds and time.time() + seconds

    def __getattr__(self, name):
        """
        Enable dot notation access to dictionary attributes
        """
        # Let's not mess with __ attributes; modules like
        # multiprocessing may check attributes such as __getstate__
        # and expect a callable object or AttributeError
        if name.startswith("__"):
            raise AttributeError(
                f"{self.__class__!r} object has no attribute {name!r}")

        # dot notation access to dictionary attributes
        return self.get(name)

    @property
    def iteration(self):
        """
        Returns the integer value for self["Iteration"], zero if not able
        to cast it to :class:`int` or `sys.maxsize` if self.final is
        True. Useful for easy comparison of different iterations of a
        message.
        """
        if self.final:
            return sys.maxsize

        try:
            return int(self["Iteration"])
        except (KeyError, TypeError, ValueError):
            return 0

    @property
    def final(self):
        """
        A read-only property that shows whether the message is in its
        final unmutable state or not.

        This mehod use "Iteration" value for checking the final
        state. If it is an instance of :class:`str` it is final, if
        not then it is not.
        """
        return isinstance(self.get("Iteration"), str)

    def finalize(self):
        """
        Mark the message state as final i.e. the log is mutated to its
        final state.

        The marking will be applied by changing the "Iteration" item's
        value to a string i.e. "final" with an optional "(n)" at the
        end in which `n` is the previous value of the "Iteration"
        key. The optional ending will only be added if `n` is an
        instance of :class:`int`.
        """
        if not self.final:
            now = time.time()
            if self.timeout_on > now:
                self.timeout_on = now

            current_iteration = self.get("Iteration")
            self["Iteration"] = (
                "final" + (f"({current_iteration})"
                           if isinstance(current_iteration, int) and
                           current_iteration != 0 else ""))

    @staticmethod
    def prepare_value(value, application="simple"):
        """
        An static method which will be called by all the string serializes
        of the class to serializes the dictionary values.

        :Parameters:
         - `value`: the input value for serialization
         - `application` (optional): for which application the value
           should be prepared e.g. simple, full or csv
        """
        if isinstance(value, datetime):
            if application == "csv":
                return value.strftime("%Y-%m-%d %X.%f")
            else:
                return f"'{value.strftime('%Y-%m-%d %X,%f')[:-3]}'"
        elif isinstance(value, float) and application == "simple":
            return f"{value:.3f}"
        elif isinstance(value, (list, dict)):
            try:
                value = json.dumps(value)
            except Exception:
                value = str(value)

            if application == "full":
                return _repr(value)
            else:
                return value
        elif isinstance(value, str) and application != "csv":
            return _repr(value)
        elif isinstance(value, bool):
            return json.dumps(value)

        return str(value)

    @property
    def full(self):
        """
        An alternative for :meth:`__str__` serializer of the class which
        will return all the keys (except the ones started with `_`)
        instead of only the ones defined in :attr:`default_keys`.
        """
        return self.default_delimiter.join(
            f"{key}{self.default_key_value_separator}"
            f"{self.prepare_value(value, application='full')}"
            for key, value in self.items()
            if not key.startswith("_"))

    @property
    def csv(self):
        """
        A CSV (RFC-4180) serializer for the class values.

        First all the columns according to :meth:`csv_columns` method
        will be returned, then the remaining values.
        """
        csv_columns = self.csv_columns()
        result = io.StringIO()
        writer = csv.writer(result, dialect="unix", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(
            [self.prepare_value(self.get(col, ""), application="csv")
             for col in csv_columns] +
            [self.prepare_value(value, application="csv")
             for key, value in self.items() if key not in csv_columns])
        return result.getvalue().rstrip("\n")

    @classmethod
    def csv_columns(cls):
        """
        Returns a list of column names that :meth:`csv` property will use
        """
        from .cursor import WatchCursor
        return ("WatchID", "Iteration", "CreateTime"
                ) + WatchCursor.watch_all_fields

    def __str__(self):
        keys = self.keys() if self.default_keys is None else self.default_keys
        return self.default_delimiter.join(
            f"{key}{self.default_key_value_separator}"
            f"{self.prepare_value(self.get(key))}"
            for key in keys)


class WatchLogEmitter:
    """
    Used for emitting logs in :mod:`pymongo.watcher` modules.

    :Attributes:
     - `default_level`: the default logging level
     - `support_old_style_formatter`: enable support for `%()s` and
       `$` style logging formatters in :class:`logging.Formatter`
    """
    default_level = logging.INFO
    support_old_style_formatter = False

    def log(self, logger_name, msg, *, level=None):
        """
        Emit a log

        :Parameters:
         - `logger_name`: the name of the logger
         - `msg`: the log message (an instance of
           :class:`pymongo.watcher.logger.WatchMessage`)
         - `level` (optional): the log level
        """
        level = self.default_level if level is None else level
        extra = {"watch": msg}
        if self.support_old_style_formatter and isinstance(msg, dict):
            extra.update(msg)
        logging.getLogger(logger_name).log(level, msg, extra=extra)


@dataclass
class NonWatchQueueItem:
    """
    A dataclass container for items of any type. :class:`WatchQueue`
    uses this container to differentiate LogRecords with WatchMessage
    items with other items such as the sentinel returned by
    :meth:`logging.handlers.QueueListener.enqueue_sentinel`.

    The constructor accepted the item for storage and calling the
    instance objects of this class will return the stored item.
    """
    item: "typing.Any"

    def __call__(self):
        return self.item


class WatchQueue:
    """
    A priority queue implementation similar to :class:`queue.Queue`
    with :meth:`put_nowait` and :meth:`get` methods. If the items put
    in the queue are the instances of :class:`logging.LogRecord` and
    they contain :class:`pymongo.watcher.logger.WatchMessage` messages
    (with .watch attribute) they will be prioritized in the queue such
    that the item with the least value for .watch.timeout_on will be
    retrieved sooner.

    Other items such as the sentinel returned by
    :meth:`logging.handlers.QueueListener.enqueue_sentinel` can also
    be inserted in the queue and they will be prioritized with the
    time they will be putted in the queue (with time.time()).

    The :meth:`get` method will return the LogRecords that are final
    (by :meth:`pymongo.watcher.logger.WatchMessage.final`) or
    timedout.

    The class instances could be used as the queue for
    :class:`logging.handlers.QueueHandler` or any compatible
    implementation.

    The queue items are internally tracked in multiple containers for
    better performance but instead garbage collection is needed that
    will be automatically take place in the :meth:`put` method.
    """
    def __new__(cls, maxsize=0, *, enable_multiprocessing=False, **kwargs):
        """
        With `enable_multiprocessing` set to False will work as usual and
        calls the parent method which will in turn call the normal
        :meth:`__init__` method.

        But if `enable_multiprocessing` is True, it will use
        :class:`WatchMultiprocessingManager` to create a started
        manager and returns a proxy object
        (:class:`multiprocessing.managers.BaseProxy`) to the
        constructed WatchQueue. The manager will be stored as a class
        attribute and will be shared among all the class instances.
        """
        if enable_multiprocessing and enable_multiprocessing != "_marked":
            if not hasattr(cls, "_manager"):
                cls._manager = WatchMultiprocessingManager()
                cls._manager.start()

            # Set enable_multiprocessing="_marked"; so basically the
            # consequent __init__ call will assume
            # enable_multiprocessing is True but the consequent
            # __new__ call knows that should not return a _manager
            # proxy and ignore the enable_multiprocessing's value.
            return cls._manager.WatchQueue(
                maxsize, enable_multiprocessing="_marked", **kwargs)

        return super().__new__(cls)

    def __init__(self, maxsize=0, *, forced_delay_sec=None,
                 garbage_collection_rate=10000,
                 enable_multiprocessing=False):
        """
        Creates a new :class:`WatchQueue` instance.

        With a non-None value for `forced_delay_sec` the
        :attr:`pymongo.watcher.logger.WatchMessage.timeout_on` will be
        used for each :class:`logging.LogRecord` item. Otherwise all
        the items, even the LogRecords with a final WatchMessage will
        be retrieved only after the specified `forced_delay_sec`
        timeout.

        :Parameters:
         - `maxsize`: the maximum size of the queue
         - `forced_delay_sec`: the seconds which will be forced as the
           timeout for every LogRecord
         - `garbage_collection_rate`: peform garbage collection after
           everytime this amount of items added to the queue
         - `enable_multiprocessing`: boolean which controls enabling
           the multiprocessing
        """
        self.maxsize = maxsize
        self.forced_delay_sec = forced_delay_sec
        self.garbage_collection_rate = garbage_collection_rate

        # A sorted storage of tuples of (timeout_on, watch_id) which
        # will be maintained by heapq operations. WatchIDs are only
        # saved as a reference to the items inside the self._records.
        self._heap = []

        # A map from `watch_id` to `record` item putted in the
        # queue. This is the main storage of the items in the queue.
        self._records = {}

        # A map from `watch_id` to `watch.iteration` for easy access
        # when we want to make sure an item is the latest version or
        # not.
        self._last_iteration = {}

        # A Condition for message passing between put/get methods. Its
        # inner lock will also be used for making thread-safe access
        # to interal containers.
        self._new_item_condition = threading.Condition()

        # A counter for the number of times an item is added to the
        # queue. Useful for running garbage collection.
        self._putted_items = 0

    def put_nowait(self, item):
        """
        Put the `item` into the queue. If the queue is the backend for
        :class:`logging.handlers.QueueHandler` the items are usually
        :class:`logging.LogRecord` instances containing
        :class:`pymongo.watcher.logger.WatchMessage` messages or the
        sentinel returned by
        :meth:`logging.handlers.QueueListener.enqueue_sentinel`.

        :Parameters:
         - `item: the :class:`logging.LogRecord` item to be queued
        """
        now = time.time()

        # If the item is a LogRecord with a WatchMessage it will have
        # a `watch` attribute. The logging.handlers.QueueHandler also
        # may put a sentinel object (usually None) to singal its
        # thread for finalization which does not have this attribute.
        watch = getattr(item, "watch", None)

        with self._new_item_condition:
            if watch is None:
                if len(self._heap) >= self.maxsize > 0:
                    raise queue.Full

                heapq.heappush(self._heap, (now, NonWatchQueueItem(item)))
            else:
                _id = watch.get("WatchID", "")
                previous_record = self._records.get(_id)

                ts = (watch.timeout_on if self.forced_delay_sec is None
                      else now + self.forced_delay_sec)

                if previous_record is None:
                    # No previous record is present for this WatchID,
                    # so this mus be a new record.

                    if len(self._heap) >= self.maxsize > 0:
                        raise queue.Full

                    heapq.heappush(self._heap, (ts, _id))
                    self._records[_id] = item
                else:
                    # A previous record is present for this WatchID.

                    if watch.iteration > self._last_iteration.get(_id, -1):
                        # The given record has a higher iteration than
                        # the latest inserted item, so it is the newer
                        # version and we have to update the _record
                        # dictionary and also put a new reference to
                        # the _heap. The old reference will be ignored
                        # by `get` method when it timed out or it may
                        # be removed by `garbage_collect`.

                        self._last_iteration[_id] = watch.iteration
                        self._records[_id] = item
                        heapq.heappush(self._heap, (ts, _id))
                    else:
                        # This is an older version of a record which
                        # we already have in the queue, we can ignore
                        # it safely.
                        return

            # Here, we know no queue.Full exception has raised and if
            # the method has not already returned, it means that in
            # fact a new item has arrived (or updated). So we have to
            # notify the `get` method.
            self._new_item_condition.notify_all()

            # Add count for garbage collection
            self._putted_items += 1

        if self._putted_items >= self.garbage_collection_rate:
            self.garbage_collect()

    def get(self, block=True):
        """
        Returns the highest priority :class:`logging.LogRecord` item form
        the queue which is final or timed out or wait and blocks until
        such item is present and then return one.

        Non-LogRecord items will be returned with their insertion time
        as the timeout; so they will always be returned without
        blocking.

        The current implementation only supports block=True which is
        enough for :class:`logging.handlers.QueueHandle`.

        :Parameters:
         - `block`: must be True; just for compatibility with
           :class:`logging.handlers.QueueHandler`
        """
        if not block:
            raise NotImplementedError("Only block=True is implemented.")

        with self._new_item_condition:
            while True:
                wait_time = None

                while self._heap:
                    now = time.time()

                    # _heap[0] is the smallest item; equivalent of
                    # heapq.nsmallest(1, self._heap)[0]
                    heap_ts, _id = self._heap[0]

                    if isinstance(_id, NonWatchQueueItem):
                        return _id()

                    record = self._records.get(_id)

                    if record is None:
                        # The record doesn't exist. Probably, a newer
                        # version (iteration) of this record is
                        # already fetched from the queue, so let's
                        # remove this reference from the _heap and
                        # forget about it.
                        heapq.heappop(self._heap)
                    else:
                        if heap_ts > now:
                            # This item has the lowest timeout in the
                            # _heap (which means it is the highest
                            # priority) but it is not yet timed
                            # out. So we have to break and wait until
                            # we are ready.
                            wait_time = heap_ts - now
                            break
                        else:
                            # The highest priority item has timed out
                            # (and is probably ready to be returned).

                            heapq.heappop(self._heap)

                            if heap_ts >= record.watch.timeout_on:
                                # The _records always has the latest
                                # version (iteration) of each record,
                                # so if the timestamp in the _heap has
                                # a lower value, it is probably for an
                                # older version and we have to wait
                                # until the new version is also timed
                                # out. So in that case only the
                                # heappop instruction above is enough
                                # and we have to continue the while
                                # loop.
                                #
                                # But if we are inside this `if`
                                # statement, the heap timeout is most
                                # probably equal to the stored record
                                # timed out because we hit exactly the
                                # same reference.
                                #
                                # The > condition should never happen
                                # unless it is a bug! In that case
                                # returning the record is probably the
                                # best possible solution.

                                self._records.pop(_id)

                                with contextlib.suppress(Exception):
                                    if not record.watch.final:
                                        # If the watch message is not
                                        # final then it has to be
                                        # timed out and we have to set
                                        # its log level to the
                                        # appropriate value.
                                        record.levelno = \
                                            record.watch.timeout_log_level
                                        record.levelname = \
                                            logging.getLevelName(
                                                record.levelno)

                                return record

                # Now we know self._heap is empty or none of the items
                # has timed out.
                self._new_item_condition.wait(wait_time)

    def garbage_collect(self):
        """
        Collect the garbage items and free memeory space.

        Usually you don't need to call this method manually as it will
        be called automatically in :meth:`put` from time to time.
        """
        with self._new_item_condition:
            self._putted_items = 0

            old_heap = self._heap
            self._heap = []

            active_ids = set()

            for ts, _id in old_heap:
                if isinstance(_id, NonWatchQueueItem):
                    heapq.heappush(self._heap, (ts, _id))
                else:
                    record = self._records.get(_id)
                    watch = getattr(record, "watch", None)
                    if watch is not None and watch.timeout_on == ts:
                        heapq.heappush(self._heap, (ts, _id))
                        active_ids.add(_id)

            self._last_iteration = {_id: it for _id, it
                                    in self._last_iteration.items()
                                    if _id in active_ids}


class WatchMultiprocessingManager(multiprocessing.managers.SyncManager):
    """
    A :mod:`multiprocessing` manager with a :meth:`WatchQueue` method
    which will return a proxy to a :class:`WatchQueue` object that can
    be shared accross multiple processes
    """
    pass


WatchMultiprocessingManager.register("WatchQueue", WatchQueue)


def get_log_emitter():
    """
    Returns a singleton instance of the
    :class:`pymongo.watcher.logger.WatchLogEmitter` class.
    """
    global _emitter

    try:
        return _emitter
    except NameError:
        _emitter = WatchLogEmitter()

    return _emitter


def log(logger_name, msg, *, level=None):
    """
    A syntactic sugar for the
    :class:`pymongo.watcher.logger.WatchLogEmitter` singleton instance
    :meth:`log` method with exactly the same arguments.
    """
    get_log_emitter().log(logger_name, msg, level=level)
