#!/usr/bin/env python3

"""
`pymongowatch` which usually installs as an extension for
:mod:`pymongo` package as a sub-package at :mod:`pymongo.watcher` path
add the watching and auditing ability to :mod:`pymongo`.

The python standard :mod:`logging` moudle could be used to handle the
emitted logs. First you have to call :func:`patch_pymongo` to patch
pymongo internals to use the pymongowatch watchers:

  from pymongo.watcher import patch_pymongo
  patch_pymongo()

Then, the pymongo auditing logs will be emitted by the logger name
`pymongo.watcher` (and sub-loggers under this name).

As the logs may mutate over time (for example for fetching more items
from a :class:`pymongo.cursor.Cursor`) it is recommended to use a
:class:`pymongo.watcher.logger.WatchQueue` queue alongside a
:class:`logging.handlers.QueueHandler` which will wait for the logs to
be muted to their final state and ther return the logs.

Also some logging formatting and level setting may be required to get
the desired result.

You can automatically setup the queue handlers/listners
(:class:`logging.handlers.QueueHandler` and
:class:`logging.handlers.QueueListener`) and appropriate log
formatting by :func:`add_logging_handlers` function all in once:

  from pymongo.watcher import add_logging_handlers
  import logging
  my_handler = logging.FileHandler("/tmp/pymongo-watcher.log")
  add_logging_handlers(my_handler)
"""

import atexit
import logging.handlers

from .collection import WatchCollection
from .cursor import WatchCursor
from .logger import WatchQueue

__version__ = "0.4.0"


def dictConfig(config):
    """
    Configure the watcher using a dictionary. Similar to
    :func:`logging.config.dictConfig`. The configuration will be
    extracted from the "watchers" key.

    :Parameters:
     - config: configuration dictionary
    """
    WatchCollection.watch_dictConfig(config)
    WatchCursor.watch_dictConfig(config)


def patch_pymongo():
    """
    Monkey patch pymongo methods to use pymongowatch logging system
    """
    WatchCollection.watch_patch_pymongo()
    WatchCursor.watch_patch_pymongo()


def unpatch_pymongo():
    """
    Undo pymongo monkey patching
    """
    WatchCollection.watch_unpatch_pymongo()
    WatchCursor.watch_unpatch_pymongo()


queue_listners = []


def setup_queue_handler(backend, filters=None, register_atexit=True, **kwargs):
    """
    Creates an instance of :class:`logging.handlers.QueueHandler` with
    an instance of :class:`pymongo.watcher.logger.WatchQueue`. Then it
    will create a :class:`logging.handlers.QueueListener` for the
    queue and the provided `backend` logging handler and starts it.

    When `filters` is None (the default), it will automatically
    initialize and add
    :class:`pymongo.watcher.filters.RestoreOriginalWatcher` and
    :class:`AddPymongoResults` instances to the QueueHandler.

    Optionally the stop method of the listner could be registered with
    :func:`atexit.register`.

    :Parameters:
     - `backend`: the backend logging handler
     - `filters`: a list of filters to add to the QueueHandler
     - `register_atexit` (optional): enable registering with
       :mod:`atexit`
     - `kwargs` (optional): keyword arguments supplying any additional
       options for the WatchQueue object
    """
    global queue_listners

    que = WatchQueue(**kwargs)
    queue_handler = logging.handlers.QueueHandler(que)

    if filters is None:
        from . import filters as watch_filters
        filters = [watch_filters.RestoreOriginalWatcher(),
                   watch_filters.AddPymongoResults()]

    for _filter in filters:
        queue_handler.addFilter(_filter)

    listener = logging.handlers.QueueListener(que, backend)
    listener.start()
    queue_listners.append(listener)

    if register_atexit:
        atexit.register(listener.stop)

    return queue_handler


def add_logging_handlers(
        *handlers, logger_name="pymongo.watcher", level=logging.INFO,
        formatter="{asctime} {name} - {watch}", with_queue=True,
        register_atexit=True, **kwargs):
    """
    Add the specified `handlers` to the pymongowatch logger.

    If no handler is specified, a new instance of
    :class:`logging.StreamHandler` will be created and used as the
    handler.

    The handlers format can optionally be set to the provided string
    `fromatter`. You can disable this feature by passing None as the
    `formatter`. The log format has to be set with `{` style.

    The handlers level, could also optionally be set by `level`
    argument. This feature could be disabled by passing None as
    `level`, too.

    Note that `level` only specifies the logging level for the
    handlers. Not the level of the emitted logs by pymongowatch. To
    change the log level of the emitted logs you have to change the
    :attr:`pymongo.watcher.logger.WatchLogEmitter.default_level`.

    If `with_queue` is enabled (the default),
    :func:`setup_queue_handler` will be called to setup a
    :class:`logging.handlers.QueueHandler` with an instance of
    :class:`pymongo.watcher.logger.WatchQueue`.

    :Parameters:
     - `handlers` (optional): all positional arguments will be used as
       the logging handlers
     - `logger_name` (optional): the name of the pymongowatch logger
     - `level` (optional): the log level
     - `formatter` (optional): the :class:`str` string to set as the
       log format
     - `with_queue` (optional): if True (the default) enable seting up
       with a :class:`logging.handlers.QueueHandler`
     - `kwargs` (optional): keyword arguments supplying any additional
       options for :func:`setup_queue_handler`
    """
    if not handlers:
        handlers = [logging.StreamHandler()]

    logger = logging.getLogger(logger_name)
    if level is not None:
        logger.setLevel(level)

    if formatter is not None and not isinstance(formatter, logging.Formatter):
        formatter = logging.Formatter(formatter, style="{")

    for handler in handlers:
        if formatter is not None:
            handler.setFormatter(formatter)

        if with_queue:
            handler = setup_queue_handler(handler, **kwargs)

        logger.addHandler(handler)
