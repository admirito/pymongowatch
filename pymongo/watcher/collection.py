#!/usr/bin/env python3

"""
This module implements :class:`WatchCollection` a
:class:`pymongo.collection.Collection` replacement which will emit
logs on each database operations.
"""

import contextlib

import pymongo

from .bases import OperationWatcher, OperationField
from .transforms import one_if_not_none


class WatchCollection(OperationWatcher, pymongo.collection.Collection):
    """
    A replacement for :class:`pymongo.collection.Collection` which will
    use :class:`pymongo.watcher.bases.OperationWatcher` to emit logs
    for each operation.
    """

    watch_default_fields = (
        "DB", "Collection", "Operation", "Filter", "Duration", "MatchedCount",
        "InsertedCount", "UpsertedCount", "ModifiedCount", "DeletedCount")

    watch_all_fields = (
        "EndTime", "DB", "Collection", "Operation", "Filter", "Duration",
        "MatchedCount", "InsertedCount", "UpsertedCount", "ModifiedCount",
        "DeletedCount")

    watch_operations = {
        "aggregate": {"pipeline": OperationField(to="Filter")},
        "find_one": {"_result": OperationField(to="MatchedCount",
                                               cast=one_if_not_none)},
        "find_one_and_delete": {"_result": OperationField(
            to="DeletedCount", cast=one_if_not_none)},
        "find_one_and_replace": {"_result": OperationField(
            to="ModifiedCount", cast=one_if_not_none)},
        "find_one_and_update": {"_result": OperationField(
            to="ModifiedCount", cast=one_if_not_none)},
        "count_documents": {"_result": OperationField(to="MatchedCount")},
        "estimated_document_count": {"_result":
                                     OperationField(to="MatchedCount")},
        "distinct": {"_result": OperationField(to="MatchedCount", cast=len)},
        "bulk_write": {"requests": OperationField(to="MatchedCount",
                                                  cast=len)},
        "insert_one": {"_result": OperationField(to="InsertedCount", value=1)},
        "insert_many": {"documents": OperationField(to="InsertedCount",
                                                    cast=len)},
        "replace_one": {"filter": OperationField(to="Filter")},
        "update_one": {"filter": OperationField(to="Filter")},
        "update_many": {"filter": OperationField(to="Filter")},
        "delete_one": {"filter": OperationField(to="Filter")},
        "delete_many": {"filter": OperationField(to="Filter")},
    }

    _watch_name = __name__

    def _before_operation(self, message, *args, **kwargs):
        """
        Calls the parent :class:`pymongo.watcher.bases.OperationWatcher`
        _before_operation to emit the log but it patch the `message`
        and add pymongo collection specific fields.
        """
        message["DB"] = self.database.name
        message["Collection"] = self.name
        return super()._before_operation(message, *args, **kwargs)

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
        super().watch_dictConfig(config, sub_section="collection",
                                 add_globals=add_globals)

        _collection = config.get("watchers", {}).get("collection", {})

    @classmethod
    def watch_patch_pymongo(cls):
        """
        Monkey patch pymongo methods to use WatchCollection
        """
        super().watch_patch_pymongo()

        cls.__real_pymongo_collection = pymongo.collection.Collection

        # We shall not patch Collection in pymongo.collection because
        # the Database class will creates a WatchCollection. The its
        # __init__ will call super(Collection, self).__init__ which
        # will be WatchCollection.
        pymongo.database.Collection = cls

    @classmethod
    def watch_unpatch_pymongo(cls):
        """
        Undo pymongo monkey patching and use the pymongo's original Collection
        """
        super().watch_unpatch_pymongo()

        with contextlib.suppress(AttributeError):
            pymongo.database.Collection = cls.__real_pymongo_collection
