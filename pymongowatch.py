#!/usr/bin/env python3

"""
A drop-in replacemnt for pymongo: Python driver for MongoDB. It
uses pymongo as the backend and provide all the pymongo modules and
classes.

pymongowatch has an extra WatchCursor class that extends
pymongo.cursor.Cursor to collect query logs. You can enable this class
as the result of pymongo.collection operators such as `find` by
calling the monkey-patching method `PatchWatchers`:

  import pymongowatch
  pymongowatch.PatchWatchers()
  client = pymongowatch.MongoClient()
  list(client.dbname.coll.find({"a": 1}))
  list(client.dbname.coll.find({"a": 2}))
  print(pymongowatch.WatchCursor.watch_all_logs())
"""

import json
import threading
import time
from collections import deque
from datetime import datetime

import pymongo

# add all the objects from pymongo so they are importable from
# pymongowatch, too
from pymongo import *

# Convert pymongowatch module to a package (every module with __path__
# attribute in python is practically a package). Here, pymongowatch
# will have all the pymongo sub-modules as its own sub-modules.
__path__ = pymongo.__path__

__version__ = "0.1.0"


class WatchCursor(pymongo.cursor.Cursor):
    """
    A cursor / iterator over Mongo query results just like
    pymongo.cursor.Cusrsor class but it can also collect logs for
    applied queries.
    """

    # a deque to store logs
    _watch_all_logs = deque(maxlen=10000)

    # a list of deques for each log follower; each item in each deque
    # is a tuple; the cursor create time and the log
    _watch_followers = []

    # A threading condition object to synchronize log generators and
    # log followers
    _watch_new_log_condition = threading.Condition()

    # The default log format template
    watch_log_format = (
        "{create_time.strftime('%Y %b %d %X.%f')[:-3]} - "
        "Collection={json.dumps(collection)} "
        "Query={json.dumps(normalized_query)} "
        "FetchTime={fetch_time:.6f} "
        "TriedFetchedCount={tried_fetched_count} "
        "FetchedCount={fetched_count}")

    def __init__(self, *args, **kwargs):
        """
        Accepts the same arguments as pymongo.cursor.Cusrsor with extra
        kwargs.

        `log_format` is a :calss:`str` which determines the format of
        the logs (in python f-string format) and could contain the
        following templates:
        - {create_time}          :class:`datetime.datetime`
        - {last_fetched_time}    :class:`datetime.datetime`
        - {query}                :class:`dict`
        - {normalized_query}     :class:`dict`
        - {collection}           :class:`str`
        - {fetched_count}        :class:`int`
        - {tried_fetched_count}  :class:`int`
        - {fetch_time}           :class:`float`

        The `log_format` if evaluated with literal f-strings. So it
        can also have function calls. For example:

        log_format = ("{create_time.strftime('%Y %b %d %X.%f')[:-3]} - "
                      "Query={json.dumps(query)")

        The generated log could be retrieved by the class method
        `watch_follow_logs`.

        If `enable_all_logs` is True, the log will be also stored in
        the internal `_watch_all_logs` deque (with the default
        maxlen=10000) and can be retrieved by `watch_all_logs`
        method. If you are only using `watch_follow_logs` method,
        setting `enable_all_logs` to False has performance advantages.

        :Parameters: 
         - `log_format`: the default template for each log.
         - `enable_all_logs`: enable/disable registering in `all_logs`.
        """
        self._watch_create_time = datetime.now()
        self._watch_last_fetched_time = None

        watch_log_format = kwargs.pop("watch_log_format", None)
        if watch_log_format:
            # override the class default
            self.watch_log_format = watch_log_format

        self._watch_enable_all_logs = kwargs.pop("enable_all_logs", True)

        super().__init__(*args, **kwargs)

        self._watch_query = self._Cursor__spec
        self._watch_tried_count = 0
        self._watch_fetched_count = 0
        self._watch_fetch_time = 0

        # `__watch_log` will be stored in `_watch_all_logs` for later
        # retrieval by `watch_all_logs` method. We have to use a
        # mutable object (such as a dict, instead of a string) to
        # store logs in `_watch_all_logs`, so we can update it later
        # in database fetch time
        self.__watch_log = self.watch_log_dict()

        if self._watch_enable_all_logs:
            self._watch_all_logs.append(self.__watch_log)

        for follower in self._watch_followers:
            with self._watch_new_log_condition:
                follower.append(self.__watch_log)
                self._watch_new_log_condition.notify_all()

    def next(self):
        """Advance the cursor."""
        try:
            self._watch_last_fetched_time = datetime.now()
            self._watch_tried_count += 1

            _start = time.time()
            try:
                result = super().next()
            finally:
                _end = time.time()
                self._watch_fetch_time += _end - _start

            # if no exception occurs above
            self._watch_fetched_count += 1
            return result
        finally:
            if self._watch_enable_all_logs:
                self.__watch_log.update(self.watch_log_dict())

    __next__ = next

    @staticmethod
    def __format_dict(fmt, dic):
        """
        Render `fmt` a :class:str like PEP 498 f-string literals with the
        defined variables in dic a :class:`dict` with string keys.
        """
        exec_locals = {"dic": dic}
        variables = "\n".join(f"{key} = dic[{key!r}]" for key in dic.keys())
        exec(f"{variables}\nresult = f'''{fmt}'''", None, exec_locals)
        return exec_locals["result"]


    def watch_log_dict(self):
        """
        Returns the log generated by WatchCursor as :class:`dict` for the
        cursor.
        """
        return {
            "create_time": self._watch_create_time,
            "last_fetched_time": self._watch_last_fetched_time,
            "query": self._watch_query,
            "normalized_query": self.watch_query_normalizer(self._watch_query),
            "collection": self.collection.name,
            "fetched_count": self._watch_fetched_count,
            "tried_fetched_count": self._watch_tried_count,
            "fetch_time": self._watch_fetch_time,
        }

    def watch_log(self, log_format=None):
        """
        Returns the log generated by WatchCursor as :class:`str` for the
        cursor.

        :Parameters:
         - `log_format: the :class:`str` template for the log.
        """
        log_format = (self.watch_log_format if log_format is None
                      else log_format)

        return self.__format_dict(log_format, self.watch_log_dict())

    def watch_query_normalizer(self, query):
        """
        This method returns `query` argument intact. You can override it
        and return any transformed query instead to set
        {normalized_query} log template. For example you can mask user
        data that is not appropriate for collection.
        """
        return query

    @classmethod
    def watch_all_logs(cls, log_format=None):
        """
        Returns a generator of all the logs generated by WatchCursor
        class.

        You can pass either a string template to get string logs or
        the type `dict` for `log_format` to get dictionary logs.

        :Parameters:
         - `log_format`: the format of the log.
        """
        log_format = (cls.watch_log_format if log_format is None
                      else log_format)

        for log_dict in cls._watch_all_logs:
            if log_format is dict:
                yield log_dict
            else:
                yield cls.__format_dict(log_format, log_dict)

    @classmethod
    def watch_clear_logs(cls):
        """
        Clears all the stored logs in the WatchCursor class.
        """
        cls._watch_all_logs.clear()

    @classmethod
    def watch_follow_logs(cls, log_format=None, wait_time_ms=600000):
        """
        Follow logs as they arrive and yiled them as a generator. This
        method wait `wait_time_ms` milliseconds before generating each
        log to make sure the log is up-to-date with latest fetching
        times.

        You can pass either a string template to get string logs or
        the type `dict` for `log_format` to get dictionary logs.

        :Parameters:
         - `log_format`: the format of the log.
         - `wait_time_ms: waiting time for each log in milliseconds.

        """
        log_format = (cls.watch_log_format if log_format is None
                      else log_format)

        logs_queue = deque()
        cls._watch_followers.append(logs_queue)

        next_log_will_be_available_at = datetime.now()

        def fetch_available_logs():
            result = []
            now = datetime.now()
            while logs_queue:
                log_dict = logs_queue.popleft()
                if ((now - log_dict["create_time"]).total_seconds() * 1000
                    > wait_time_ms):
                    result.append(log_dict)
                else:
                    # put backed the fetched item! it's not ready yet.
                    logs_queue.appendleft(log_dict)
                    break
            return result

        while True:
            with cls._watch_new_log_condition:
                logs = fetch_available_logs()
                while not logs:
                    # there are two conditions that a log may be
                    # available; when the log generator notifies the
                    # cls._watch_new_log_condition and when
                    # wait_time_ms milliseconds has passed since the
                    # earliest log in the deque (i.e. the first log in
                    # logs_queue)
                    timeout = \
                        max(0, wait_time_ms / 1000.0 -
                            (datetime.now() -
                             logs_queue[0]["create_time"]).total_seconds()) \
                             if logs_queue else None
                    cls._watch_new_log_condition.wait(timeout)
                    logs = fetch_available_logs()

            for log_dict in logs:
                if log_format is dict:
                    yield log_dict
                else:
                    yield cls.__format_dict(log_format, log_dict)


def PatchWatchers():
    """
    Monkey patch pymongo methods to use pymongowatch cursors
    """
    pymongo.cursor.Cursor = WatchCursor
    pymongo.collection.Cursor = WatchCursor


try:
    # If pymongo mask is enabled, but pymongowatch is imported, the
    # mask cannot import pymongowatch and enable the `watcher`
    # module. So pymongowatch has to initialize it, itself.
    pymongo.watcher._initialize_watchers()
except Exception:
    # Exception may occuer if pymongo mask is not enbaled
    pass
