#!/usr/bin/env python3

"""
"""

import contextlib

import pymongo

from .base import OperationWatcher
from .transforms import one_if_not_none


class WatchCollection(OperationWatcher, pymongo.collection.Collection):
    """
    """

    watch_default_fields = (
        "DB", "Collection", "Operation", "Filter", "Duration", "MatchedCount",
        "InsertedCount", "UpsertedCount", "ModifiedCount", "DeletedCount")

    watch_all_fields = (
        "EndTime", "DB", "Collection", "Operation", "Filter", "Duration",
        "MatchedCount", "InsertedCount", "UpsertedCount", "ModifiedCount",
        "DeletedCount")

    watch_operations = {
        "aggregate": {"pipeline": {"to": "Filter"}},
        "find_one": {"_result": {"to": "MatchedCount",
                                 "cast": one_if_not_none}},
        "find_one_and_delete": {"_result": {"to": "DeletedCount",
                                            "cast": one_if_not_none}},
        "find_one_and_replace": {"_result": {"to": "ModifiedCount",
                                             "cast": one_if_not_none}},
        "find_one_and_update": {"_result": {"to": "ModifiedCount",
                                            "cast": one_if_not_none}},
        "count_documents": {"_result": {"to": "MatchedCount"}},
        "estimated_document_count": {"_result": {"to": "MatchedCount"}},
        "distinct": {"_result": {"to": "MatchedCount",
                                 "cast": len}},
        "bulk_write": {"requests": {"to": "MatchedCount",
                                    "cast": len}},
        "insert_one": {"_result": {"to": "InsertedCount", "value": 1}},
        "insert_many": {"documents": {"to": "InsertedCount",
                                      "cast": len}},
        "replace_one": {"filter": {"to": "Filter"}},
        "update_one": {"filter": {"to": "Filter"}},
        "update_many": {"filter": {"to": "Filter"}},
        "delete_one": {"filter": {"to": "Filter"}},
        "delete_many": {"filter": {"to": "Filter"}},
    }

    _watch_name = __name__

    def _before_operation(self, message, *args, **kwargs):
        """
        """
        message["DB"] = self.database.name
        message["Collection"] = self.name
        super()._before_operation(message, *args, **kwargs)

    @classmethod
    def watch_dictConfig(cls, config, add_globals=True):
        """
        Configure the watcher using a dictionary. Similar to
        :func:`logging.config.dictConfig`. The configuration will be
        extracted from the the "watchers" key in the `config`
        dictionary. This method implements the "collection" section
        configuration.

        :Parameters:
         - config: configuration dictionary
         - add_globals (optional): A boolean indicating weather to
           load the "global" section or not.
        """
        super().watch_dictConfig(config, add_globals=add_globals)

        _collection = config.get("watchers", {}).get("collection", {})

        with contextlib.suppress(Exception):
            cls._watch_timeout_sec = int(_collection["timeout_sec"])

        log_level = _collection.get("log_level", {})
        levels = cls._watch_configurtor.configure_watch_log_level(log_level)
        for log_type in ["first", "update", "final", "timeout"]:
            with contextlib.suppress(KeyError):
                setattr(cls, f"_watch_log_level_{log_type}", levels[log_type])

        try:
            default_fields = tuple(key for key in
                                   _collection.get("default_fields", [])
                                   if key in cls.watch_all_fields)
        except Exception:
            default_fields = ()

        if default_fields:
            cls.watch_default_fields = default_fields

    @classmethod
    def watch_patch_pymongo(cls):
        """
        Monkey patch pymongo methods to use WatchCursor
        """
        super().watch_patch_pymongo()

        cls.__real_pymongo_collection = pymongo.collection.Collection

        # pymongo.collection.Collection = cls
        pymongo.database.Collection = cls

    @classmethod
    def watch_unpatch_pymongo(cls):
        """
        Undo pymongo monkey patching and use the pymongo's original Cursor
        """
        super().watch_unpatch_pymongo()

        with contextlib.suppress(AttributeError):
            # pymongo.collection.Collection = cls.__real_pymongo_collection
            pymongo.database.Collection = cls.__real_pymongo_collection
