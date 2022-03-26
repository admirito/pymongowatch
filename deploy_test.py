#!/usr/bin/env python3

import argparse
import contextlib
import inspect
import logging.handlers
import logging.config
import multiprocessing
import os
import sys
import time

import yaml


def get_real_pymongo_path():
    """
    Returns the path to the real pymongo
    """
    current_dir = os.path.dirname(__file__)

    for path in sys.path:
        with contextlib.suppress(OSError):
            if not path or os.path.samefile(path, current_dir):
                continue
            elif os.path.isfile(os.path.join(path, "pymongo", "__init__.py")):
                return path

    return None


def test(mongodb_url):
    client = pymongo.MongoClient(mongodb_url)
    db = client.pywatch

    test_collection(db)
    test_cursor(db)
    test_rates(db)

    time.sleep(2)


def test_cursor(db):
    it1 = db.pywatch.find()
    [None for _ in zip(range(10), it1)]

    it2 = db.pywatch.find({"a": {"$lt": 20}})
    it2.batch_size(8)
    with it2:
        [None for _ in zip(range(5), it2)]

    it3 = db.pywatch.find({"a": {"$lt": 30}})
    list(it3[5:10])

    it4 = db.pywatch.find({"a": {"$lt": 40}})
    try:
        it4[20]
        it4[30]
    except Exception:
        pass

    list(db.pywatch.find({"a": {"$lt": 50}}))

    it2.rewind()
    list(it2)

    it5 = db.pywatch.find({"a": {"$lt": 60}})
    it5.batch_size(5)
    [None for _ in zip(range(31), it5)]
    it5.close()

    it5.rewind()
    it5.batch_size(10)
    [None for _ in zip(range(31), it5)]

    time.sleep(pymongo.watcher.WatchCursor._watch_timeout_sec / 2)

    [None for _ in zip(range(10), it5)]

    time.sleep(pymongo.watcher.WatchCursor._watch_timeout_sec / 2)

    [None for _ in zip(range(10), it5)]


def test_collection(db):
    db.pywatch.delete_many({"a": {"$exists": True}})
    db.pywatch.insert_many([{"a": i} for i in range(100)])
    db.pywatch.insert_many([
        {"b": 1}, {"b": 2}, {"b": 3}, {"b": 4}])
    db.pywatch.update_many({"b": {"$gte": 3}}, {"$inc": {"b": 10}})
    db.pywatch.delete_many({"b": {"$exists": True}})
    db.pywatch.delete_one({"foo": {"$exists": True}})


def test_rates(db):
    for i in range(26):
        db.pywatch.insert_many([{"c": i} for _ in range(10)])
        time.sleep(0.25)

    db.pywatch.delete_many({"c": {"$exists": True}})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Deployment tests for pymongowatch")

    current_dir = os.path.dirname(__file__)

    parser.add_argument(
        "-i", "--normal-import", action="store_true",
        help="import pymongo and without chaning sys.paths (pymongo_mask and "
        "pymongo with watcher must be in sys.path)")
    parser.add_argument(
        "-M", "--disable-multiprocessing", action="store_false",
        dest="multiprocessing",
        help="Disable running tests in a separate process")
    parser.add_argument(
        "-P", "--disable-patching", action="store_false",
        dest="patching",
        help="Disable running patch_watchers")
    parser.add_argument(
        "-c", "--config", metavar="<yaml_file>",
        default=os.path.join(current_dir, "etc", "pymongowatch.yaml"),
        help="Load logging configuration from <yaml_file>")
    parser.add_argument(
        "-m", "--mongodb-url", metavar="<url>", default=None,
        help="Set MongoDB url to <url>")

    args = parser.parse_args()

    if args.normal_import:
        import pymongo
    else:
        sys.path.insert(0, get_real_pymongo_path())
        import pymongo
        sys.path.pop(0)

        sys.path.insert(0, os.path.join(current_dir, "pymongo"))
        import watcher
        sys.path.pop(0)
        pymongo.watcher = watcher

        sys.modules["pymongo.watcher"] = watcher
        for name, attr in watcher.__dict__.items():
            if inspect.ismodule(attr):
                full_name = f"pymongo.watcher.{name}"
                sys.modules[full_name] = attr
                attr.__name__ = full_name

        watcher.cursor.WatchCursor._watch_name = "pymongo.watcher.cursor"
        watcher.collection.WatchCollection._watch_name = \
            "pymongo.watcher.collection"

    print(f"deploy tests with {pymongo.__version__=}...",
          file=sys.stderr, flush=True)

    if args.config:
        with open(args.config) as fp:
            config_dict = yaml.load(fp, Loader=yaml.CLoader)
        logging.config.dictConfig(config_dict)
        pymongo.watcher.dictConfig(config_dict)

    if args.patching:
        pymongo.watcher.patch_pymongo()

    if args.multiprocessing:
        p = multiprocessing.Process(target=test, args=(args.mongodb_url,))
        p.start()
        p.join()
    else:
        test(args.mongodb_url)
