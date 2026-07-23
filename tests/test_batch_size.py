"""Tests for batch_size interval normalization and discovery parsing."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ray_dispatcher import HandlerSpec, KafkaSource, discover_workers
from ray_dispatcher.models import merge_batch_windows, normalize_batch_size


class BatchSizeNormalizeTests(unittest.TestCase):
    def test_int_maps_to_open_upper(self) -> None:
        self.assertEqual((5, None), normalize_batch_size(5))
        self.assertEqual((0, None), normalize_batch_size(0))

    def test_open_lower_defaults_min_to_one(self) -> None:
        self.assertEqual((1, 5), normalize_batch_size((None, 5)))

    def test_closed_range(self) -> None:
        self.assertEqual((3, 10), normalize_batch_size((3, 10)))

    def test_rejects_inverted_range(self) -> None:
        with self.assertRaises(ValueError):
            normalize_batch_size((10, 3))

    def test_handler_spec_batch_window(self) -> None:
        source = KafkaSource("events", ("b",), "events")
        spec = HandlerSpec("w", object(), (source,), batch_size=(None, 7))
        self.assertEqual((1, 7), spec.batch_window)

    def test_merge_batch_windows_takes_strictest(self) -> None:
        self.assertEqual(
            (5, 10),
            merge_batch_windows(((1, 10), (5, None), (2, 20))),
        )


class BatchSizeDiscoveryTests(unittest.TestCase):
    def test_discovers_list_range(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "w.py").write_text(
                """
SOURCES = {
    "events": {
        "kind": "kafka",
        "brokers": ["broker"],
        "topic": "events",
        "initial_offset": "earliest",
    }
}
HANDLERS = [
    {"entrypoint": "handle", "sources": ["events"], "batch_size": [2, 8]}
]

def handle(request, records):
    return len(records)
""",
                encoding="utf-8",
            )
            workers, _, _ = discover_workers(directory)
            self.assertEqual(1, len(workers))
            self.assertEqual((2, 8), workers[0].batch_size)
            self.assertEqual((2, 8), workers[0].batch_window)


if __name__ == "__main__":
    unittest.main()
