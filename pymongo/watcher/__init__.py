#!/usr/bin/env python3

import atexit
import logging.handlers

from .cursor import WatchCursor
from .logger import WatchQueue

__version__ = "0.1.0"


def set_query_normalizer(func):
    """
    Set WatchCursor.watch_query_normalizer to the provided `func`. The
    `func` must be a callable with exactly on argument `query` which
    will be used to generate the {Query} template inside the
    watcher log.
    """
    assert callable(func)
    WatchCursor.watch_query_normalizer = staticmethod(func)


def patch_pymongo():
    """
    Monkey patch pymongo methods to use pymongowatch logging system
    """
    WatchCursor.watch_patch_pymongo()


def unpatch_pymongo():
    """
    Undo pymongo monkey patching
    """
    WatchCursor.watch_patch_pymongo()


queue_listners = []


def setup_queue_handler(backend, register_atexit=True, **kwargs):
    """
    """
    global queue_listners

    que = WatchQueue(**kwargs)
    queue_handler = logging.handlers.QueueHandler(que)

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
            handler = setup_queue_handler(
                handler, register_atexit=register_atexit, **kwargs)

        logger.addHandler(handler)
