#!/usr/bin/env python3

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
    """

    default_keys = None
    default_delimiter = " "
    default_key_value_separator = "="
    timeout_on = None
    _ready = False

    # dot notation access to dictionary attributes
    __getattr__ = dict.get

    def set_ready(self):
        if not self._ready:
            self.ready_call_back()

        self._ready = True

    def ready_call_back(self):
        """
        """
        pass

    @staticmethod
    def prepare_value(value):
        """
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
        """
        result = cls(message_dict)

        for key, value in kwargs.items():
            setattr(result, key, value)

        if delay_sec:
            result.timeout_on = time.time() + delay_sec

        if ready:
            result.set_ready()

        return result

    def __str__(self):
        keys = self.keys() if self.default_keys is None else self.default_keys
        return self.default_delimiter.join(
            f"{key}{self.default_key_value_separator}"
            f"{self.prepare_value(self.get(key))}"
            for key in keys)

    @property
    def full(self):
        return self.default_delimiter.join(
            f"{key}{self.default_key_value_separator}"
            f"{self.prepare_value(value)}"
            for key, value in self.items()
            if not key.startswith("_"))

    def __hash__(self):
        return id(self)


class WatchLogEmitter:
    """
    """
    default_level = logging.INFO
    support_old_style_formatter = False

    def log(self, logger_name, msg, *, level=None):
        """
        """
        level = self.default_level if level is None else level
        extra = {"watch": msg}
        if self.support_old_style_formatter and isinstance(msg, dict):
            extra.update(msg)
        logging.getLogger(logger_name).log(level, msg, extra=extra)


class WatchQueue:
    """
    """
    def __init__(self, maxsize=0, default_delay_sec=0,
                 force_default_delay=False):
        """
        """
        self.default_delay_sec = default_delay_sec
        self.force_default_delay = force_default_delay

        self.queue = []
        # Key= LogRecords that their .watch is ready, Value =
        # time.time() on insertion
        self.ready_items = {}
        self.new_item_condition = threading.Condition()
        self.garbage_items = set()  # watch items

        self.garbage_collection_limit = 10000

    def put_nowait(self, item):
        """
        """
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
        """
        NONE = object()

        queue_ts = ready_ts = None
        ready_item = queue_item = NONE

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

                        if item_watch in self.garbage_items:
                            queue_item = NONE

                            poped_items = []
                            while True:
                                poped_ts, poped_item = heapq.heappop(
                                    self.queue)
                                if poped_item is item:
                                    self.garbage_items.remove(item_watch)
                                    break
                                else:
                                    poped_items.append((poped_ts, poped_item))
                            for item in poped_items:
                                heapq.heappush(self.queue, item)
                        else:
                            break
                    else:
                        break

                if self.ready_items:
                    # Grab the first item from the self.reay_items
                    # dictionary
                    ready_item, ready_ts = next(iter(self.ready_items.items()))

                if queue_item is NONE and ready_item is NONE:
                    timeout = queue_ts - now if queue_ts else None
                    self.new_item_condition.wait(timeout)
                else:
                    break

        if len(self.garbage_items) > self.garbage_collection_limit:
            self.garbage_collect()

        if (queue_item is not NONE and
                (ready_item is NONE or queue_ts < ready_ts)):
            ts, item = heapq.heappop(self.queue)
            if getattr(item, "watch", None):
                item.watch.reay_call_back = lambda x: None
            self.ready_items.pop(item, None)
            return item
        else:
            watch_item = getattr(ready_item, "watch", None)
            if watch_item:
                self.garbage_items.add(watch_item)
            self.ready_items.pop(ready_item)
            return ready_item

    def garbage_collect(self):
        """
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
        """
        with self.new_item_condition:
            self.ready_items[item] = time.time()
            self.new_item_condition.notify_all()


def getLogEmitter():
    """
    """
    global _emitter

    try:
        return _emitter
    except NameError:
        _emitter = WatchLogEmitter()

    return _emitter


def log(logger_name, msg, *, level=None):
    """
    """
    getLogEmitter().log(logger_name, msg, level=None)
