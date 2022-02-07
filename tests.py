#!/usr/bin/env python3

import unittest
from collections import deque
from unittest import mock

import pymongowatch


class TestWatchCursor(unittest.TestCase):
    def setUp(self):
        # We use port=-1 (an invalid value for port number) to make
        # sure we will never connect to a real MongoDB by mistake
        self.test_collection = \
            pymongowatch.MongoClient(port=-1).test_db.test_collection

        self.query = {"value": 10}

        self.test_cursor = pymongowatch.WatchCursor(
            self.test_collection, filter=self.query)

        # clear the logs from the class attribute
        self.test_cursor.watch_clear_logs()

    @unittest.mock.patch("pymongo.cursor.Cursor.next")
    def test_log(self, mock_next):
        with mock.patch("time.time", side_effect=range(100)):
            items = [next(self.test_cursor) for _ in range(10)]
        self.assertEqual(mock_next.call_count, 10)

        self.assertEqual(
            self.test_cursor.watch_log_dict(), {
                "create_time": mock.ANY,
                "last_fetched_time": mock.ANY,
                "query": self.query,
                "normalized_query": self.query,
                "collection": "test_collection",
                "fetched_count": 10,
                "tried_fetched_count": 10,
                "fetch_time": 10})

        self.assertRegex(
            self.test_cursor.watch_log(
                "{create_time.strftime('%Y %b %d %X.%f')[:-3]} - "
                "{last_fetched_time.strftime('%Y %b %d %X.%f')[:-3]} : "
                "Collection={json.dumps(collection)} "
                "Query={query!r} "
                "NQuery={normalized_query!r} "
                "FetchTime={fetch_time:.6f} "
                "TriedFetchedCount={tried_fetched_count} "
                "FetchedCount={fetched_count}"),
            r'\d+ \w+ \d\d \d\d:\d\d:\d\d.\d{3} - '
            r'\d+ \w+ \d\d \d\d:\d\d:\d\d.\d{3} : '
            f'Collection="test_collection" Query={self.query!r} '
            f'NQuery={self.query!r} FetchTime=10.000000 '
            'TriedFetchedCount=10 FetchedCount=10')

    @unittest.mock.patch("pymongo.cursor.Cursor.next")
    def test_all_logs(self, mock_next):
        for i in range(3):
            cursor = pymongowatch.WatchCursor(
                self.test_collection, filter=self.query)
            items = [next(cursor) for _ in range(10)]

        self.assertEqual(
            list(pymongowatch.WatchCursor.watch_all_logs(log_format=dict)),
            [{"create_time": mock.ANY,
              "last_fetched_time": mock.ANY,
              "query": self.query,
              "normalized_query": self.query,
              "collection": "test_collection",
              "fetched_count": 10,
              "tried_fetched_count": 10,
              "fetch_time": mock.ANY}] * 3)

    @unittest.mock.patch("pymongo.cursor.Cursor.next")
    @unittest.mock.patch("threading.Condition.wait")
    @unittest.mock.patch("pymongowatch.deque")
    def test_follow_logs(self, mock_deque, mock_wait, mock_next):
        queue = mock_deque.return_value = deque()

        try:
            pymongowatch.WatchCursor._watch_followers.append(queue)

            for i in range(3):
                cursor = pymongowatch.WatchCursor(
                    self.test_collection, filter=self.query)
                items = [next(cursor) for _ in range(10)]
        finally:
            # calling `watch_follow_logs` will add the deque again
            # into the followers list
            pymongowatch.WatchCursor._watch_followers.pop()

        mock_wait.side_effect = [True] * 3

        # create the generator
        follower = pymongowatch.WatchCursor.watch_follow_logs(
            log_format=dict, wait_time_ms=0)

        self.assertEqual(
            [next(follower) for _ in range(3)],
            [{"create_time": mock.ANY,
              "last_fetched_time": mock.ANY,
              "query": self.query,
              "normalized_query": self.query,
              "collection": "test_collection",
              "fetched_count": 10,
              "tried_fetched_count": 10,
              "fetch_time": mock.ANY}] * 3)


if __name__ == '__main__':
    unittest.main()
