#!/usr/bin/env python3

"""
This module implements the required classes to emit logs in the
watchers.

All the :mod:`pymongo.watcher` modules use a singleton instance of
:class:`WatchLogEmitter` class returned by :func:`getLogEmitter` to
emit logs which internally emit logs by leveraging python
:class:`logging`. The :func:`log` function is also available as a
syntactic sugar for the emitter instance :meth:`log` method.

.. note:: Maybe "logging" was the proper name for this module, but to
   prevent confusion with the python :class:`logging` class, the name
   "logger" seems more appropriate.
"""

import heapq
import json
import logging
import queue
import threading
import time
from datetime import datetime
from functools import partial


class WatchMessage(dict):
    """
    An extended :class:`dict` to be used as the log message in
    :mod:`pymongo.watcher` modules as explained in `Using arbitrary
    objects as messages in Python Logging HOWTO
    <https://docs.python.org/3/howto/logging.html#using-arbitrary-objects-as-messages>`_.

    The :meth:`make` class method could be used to create instances of
    this class.  The default constuctor is compatible with
    :class:`dict` which accepts all the keyword arguments as the
    dictionary key/values.

    The object :func:`id` is provided as the hash so the instaces
    could be used as :class:`dict` keys.

    Dot notation access to keys is also available, so the
    :class:`logging.Formatter` can access the inner key/values.

    :Attributes:
     - `default_keys`: a :class:`tuple` of keys to be represented in
       str format
     - `default_delimiter`: the separator between items in str format
     - `default_key_value_separator`: the separator between key/values
       in str format
     - `timeout_on`: A :class:`datetime.datetime` in which the message
       will be assumed ready
    """

    default_keys = None
    default_delimiter = " "
    default_key_value_separator = "="
    timeout_on = None
    _ready = False

    # dot notation access to dictionary attributes
    __getattr__ = dict.get

    def set_ready(self):
        """
        Mark the object state as "ready" i.e. the log is mutated to its
        final state and its ready to be handled by the
        :mod:`logging.handlers`. The :meth:`ready_call_back` will be
        called in this state change.
        """
        if not self._ready:
            self.ready_call_back()

        self._ready = True

    def ready_call_back(self):
        """
        A method meant to be overridden. It will be called when the state
        of the object turned to "ready" by :meth:`set_ready` method.
        """
        pass

    @staticmethod
    def prepare_value(value):
        """
        An static method which will be called by all the string serializes
        of the class to serializes the dictionary values.

        :Parameters:
         - `value`: the input value for serialization
        """
        if isinstance(value, datetime):
            value = value.strftime('%Y-%m-%d %X,%f')[:-3]
        elif isinstance(value, float):
            return f"{value:.3f}"

        try:
            return json.dumps(value)
        except Exception:
            return str(value)

    @classmethod
    def make(cls, message_dict, ready=False, delay_sec=None, **kwargs):
        """
        Alternative constructor of the class which returns an instance of
        the class with message_dict as the base dictionary passed to
        the parent :class:`dict` constructor.

        Extra keyword arguments will be set as the attributes of the
        returned object.

        :Parameters:
         - `message_dict`: the base dictionary
         - `ready` (optional): call :meth:`set_ready` method while instantiating
         - `delay_sec` (optional): the seconds after the current time
           to be set as :attr:`timeout_on`
        """
        result = cls(message_dict)

        for key, value in kwargs.items():
            setattr(result, key, value)

        if delay_sec:
            result.timeout_on = time.time() + delay_sec

        if ready:
            result.set_ready()

        return result

    @property
    def full(self):
        """
        An alternative for :meth:`__str__` serializer of the class which
        will return all the keys (except the ones started with `_`)
        instead of only the ones defined in :attr:`default_keys`.
        """
        return self.default_delimiter.join(
            f"{key}{self.default_key_value_separator}"
            f"{self.prepare_value(value)}"
            for key, value in self.items()
            if not key.startswith("_"))

    def __str__(self):
        keys = self.keys() if self.default_keys is None else self.default_keys
        return self.default_delimiter.join(
            f"{key}{self.default_key_value_separator}"
            f"{self.prepare_value(self.get(key))}"
            for key in keys)

    def __hash__(self):
        return id(self)


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


class WatchQueue:
    """
    A priority qeuue implementation similar to :class:`queue.Queue`
    with :meth:`put_nowait` and :meth:`get` methods. The items put in
    the queue should be :class:`logging.LogRecord` instances
    containing :class:`pymongo.watcher.logger.WatchMessage` messages
    or the sentinel returned by
    :meth:`logging.handlers.QueueListener.enqueue_sentinel`.

    The :meth:`get` method will return the LogRecords that are ready
    (by :meth:`pymongo.watcher.logger.WatchMessage.set_ready`) or
    timedout.

    The class instances could be used as the queue for
    :class:`logging.handlers.QueueHandler` or any compatible
    implementation.

    The queue items are internally stored in multiple containers such
    as heap queues, dictionaries and sets for better performance but
    instead garbage collection is needed that will be automatically
    take place in the :meth:`get` method.
    """
    def __init__(self, maxsize=0, default_delay_sec=0,
                 force_default_delay=False, garbage_collection_limit=10000):
        """
        Create a new :class:`WatchQueue`.

        Without `force_default_delay` the
        :attr:`pymongo.watcher.logger.WatchMessage.timeout_on` will be
        used for each :class:`logging.LogRecord` item.

        :Parameters:
         - `maxsize`: the maximum size of the queue
         - `default_delay_sec`: the default value for timeout of items
         - `force_default_delay`: apply the `default_delay_sec`
           timeout even if the items has their own timeout.
         - `garbage_collection_limit`: peform garbage collection after
           generation of this amount of garbage
        """
        self.maxsize = maxsize
        self.default_delay_sec = default_delay_sec
        self.force_default_delay = force_default_delay
        self.garbage_collection_limit = garbage_collection_limit

        # The inner queue of LogRecords; we will use heapq to maintain
        # a list of LogRecords sorted by their `watch.timeout_on`
        # attribute. So the items in this heap list are tuples of
        # first the timestamp and second the LogRecord.
        self.queue = []

        # Alongside the heapq `queue` of items which can be used by
        # `get` method to find the earliest log with O(1) time
        # complexity, we have to maintain a dictionary of items that
        # may be set as "ready" (by WatchMessage `set_ready` method)
        # out of order so we can find them with O(1) time complexity
        # too. As we have redundant LogRecords we require a garbage
        # collection mechanism.
        #
        # Key   = LogRecords that their .watch is ready
        # Value = time.time() on insertion
        self.ready_items = {}

        # A threading Condition for message passing between put/get
        # methods
        self.new_item_condition = threading.Condition()

        # A set of WatchMessage items (the msg inside the LogRecord)
        # which are already returned by `get` method and are ready to
        # be collected by the garbage collector
        self.garbage_items = set()

    def put_nowait(self, item):
        """
        Put the `item` into the queue. The items should be
        :class:`logging.LogRecord` instances containing
        :class:`pymongo.watcher.logger.WatchMessage` messages or the
        sentinel returned by
        :meth:`logging.handlers.QueueListener.enqueue_sentinel`.

        :Parameters:
         - `item: the :class:`logging.LogRecord` item to be queued
        """
        if len(self.queue) >= self.maxsize > 0:
            raise queue.Full

        # Fetch the inner WatchMessage inside the LogRecord `item`;
        # but sometimes items may not be a LogRecord (or even a
        # LogRecord without a "watch" in case someone is messing with
        # us!). For example the logging.handlers.QueueHandler put a
        # sentinel object (usually i.e. None) to singal its thread for
        # finalization.
        watch = getattr(item, "watch", None)

        timeout_on = time.time() + self.default_delay_sec
        if not self.force_default_delay and watch:
            timeout_on = watch.timeout_on or timeout_on

        ready = watch._ready if watch else None

        if self.force_default_delay or not ready:
            if ready is not None:  # ready is False
                watch.ready_call_back = partial(self.__ready_call_back,
                                                item)
            with self.new_item_condition:
                heapq.heappush(self.queue, (timeout_on, item))
                self.new_item_condition.notify_all()
        else:
            # item is already in "ready" state
            self.__ready_call_back(item)

    def get(self, block=True):
        """
        Returns the highest priority :class:`logging.LogRecord` item form
        the queue which is ready or timedout or wait and blocks until
        such item is present and then return one.

        The current implementation only supports block=True which is
        enough for :class:`logging.handlers.QueueHandle`.

        :Parameters:
         - `block`: has no effect; just for compatibility with
           :class:`logging.handlers.QueueHandler`
        """
        # the fetching item might be None itself, so we need our own
        # NONE to mark the null value.
        NONE = object()

        # The timestamp for the item in the queue heap and the one in
        # ready_items dictionary
        queue_ts = ready_ts = None

        queue_item = ready_item = NONE

        with self.new_item_condition:
            while True:
                now = time.time()

                while True:
                    if self.queue:
                        # queue[0] is the smallest item; equivalent of
                        # heapq.nsmallest(1, self.queue)[0]
                        queue_ts, item = self.queue[0]
                        queue_item = item if now >= queue_ts else NONE

                        item_watch = getattr(queue_item, "watch", None)

                        # We have to skip items that are already
                        # fetched (Because we have two parallel
                        # mechanisms that may make the items legitimate
                        # for fetching; (1) The timeout mechanism (2)
                        # The manual marking with `set_ready`. And we
                        # don't know which mechanism has already
                        # returned the legitimate item)
                        if item_watch in self.garbage_items:
                            # This item has already fetched; so we have
                            # to mark the queue_item as NONE again and
                            # start over to find the legitimate item
                            # to be returned
                            queue_item = NONE

                            # While we have found a garbage item,
                            # someone might have called the put method
                            # and added newer items (with earlier
                            # timeout) in the queue, so if we don't be
                            # cautious we may remove non-garbage items
                            # by mistake.
                            #
                            # We can use the diverse types of python
                            # thraeding locks to create mutual
                            # exclusion, preven race conditions and
                            # make sure no one can interfere with our
                            # garbage collection mechanism.
                            #
                            # But damn the locks! They are just
                            # academic stuff to make your programs
                            # slower. If we can avoid using locks
                            # we'll avoid using locks.
                            #
                            # Here, we loop through the newest items
                            # till we found the item we expect
                            # (hopefully most of the times, in our
                            # first try) but if we found different
                            # items we put them in poped_items list so
                            # later we can put them back in the queue.
                            poped_items = []
                            while True:
                                poped_ts, poped_item = heapq.heappop(
                                    self.queue)
                                if poped_item is item:
                                    # found the expected item; let's
                                    # remove it from the garbage_items
                                    # and ignore it by breaking.
                                    self.garbage_items.remove(item_watch)
                                    break
                                else:
                                    # Oops! Bad luck. New items has
                                    # arrived. Let's store them
                                    # somewhere so we can put them
                                    # back in the queue later.
                                    poped_items.append((poped_ts, poped_item))
                            # Restore the items into the queue that
                            # were poped by mistake
                            for item in poped_items:
                                heapq.heappush(self.queue, item)
                        else:
                            # We have found our candidate (the
                            # earliest item in the queue) that is not
                            # already fetched
                            break
                    else:  # if self.queue is empty:
                        break

                if self.ready_items:
                    # Grab the first item from the self.reay_items
                    # dictionary
                    ready_item, ready_ts = next(iter(self.ready_items.items()))

                if queue_item is NONE and ready_item is NONE:
                    # No item has timedout and no item has set to be
                    # ready. If non-timedout items are present we have
                    # to wait at most for the time between now and its
                    # timeout or when someone triggers the
                    # new_item_condition.
                    timeout = queue_ts - now if queue_ts else None
                    self.new_item_condition.wait(timeout)
                else:
                    # We have found our candidates for returning
                    break

        if len(self.garbage_items) > self.garbage_collection_limit:
            self.garbage_collect()

        # If both queue_item and ready_item are present (not NONE) we
        # will return the earliest one to make sure starvation will
        # not occur in neither of the sides
        if (queue_item is not NONE and
                (ready_item is NONE or queue_ts < ready_ts)):
            ts, item = heapq.heappop(self.queue)
            if getattr(item, "watch", None):
                # disable the ready_call_back
                item.watch.reay_call_back = lambda x: None

            # Make sure the item will not be returned again as the
            # items that are ready. Here we don't need a garbage
            # collection mechanism because we can remove them from
            # ready_items dictionary in O(1) time complexity.
            self.ready_items.pop(item, None)

            return item
        else:  # the "ready" item is the right candidate for returning
            watch_item = getattr(ready_item, "watch", None)
            if watch_item:
                # Here we have more time complexity to remove the item
                # from the queue heap (to avoid redundant returnes by
                # get method) as heaps are not very optimized for
                # searching for the arbitrary items. So it is a better
                # approach to store them in the garbage container and
                # remove them in batch in garbage_collect method.
                self.garbage_items.add(watch_item)

            self.ready_items.pop(ready_item)

            return ready_item

    def garbage_collect(self):
        """
        Collect the garbage items and free memeory space.

        Usually you don't need to call this method manually as it will
        be called automatically in :meth:`get` from time to time.
        """
        useful_items = []
        while self.queue:
            ts, item = heapq.heappop(self.queue)
            watch_item = getattr(item, "watch", None)
            if watch_item in self.garbage_items:
                self.garbage_items.remove(watch_item)
            else:
                useful_items.append((ts, item))

        for item in useful_items:
            heapq.heappush(self.queue, item)

    def __ready_call_back(self, item):
        """
        The call back function that could be used as
        :meth:`pymongo.watcher.logger.WatchMessage.ready_call_back`. It
        will add the `item` into the :attr:`ready_items` dictionary
        and notify all the threads waiting for the
        :attr:`new_item_condition`.
        """
        with self.new_item_condition:
            self.ready_items[item] = time.time()
            self.new_item_condition.notify_all()


def getLogEmitter():
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
    getLogEmitter().log(logger_name, msg, level=None)
