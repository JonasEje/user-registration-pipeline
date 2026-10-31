import os
import sys
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from consumer.consumer import build_mongo_uri, decode_registration
from producer.producer import build_interval_seconds, build_registration

MONGO_ENV = {
    "MONGO_ROOT_USERNAME": "projectadmin",
    "MONGO_ROOT_PASSWORD": "p@ss word",
}


class MongoUriTests(unittest.TestCase):
    def test_builds_authenticated_uri_from_parts(self) -> None:
        with patch.dict(
            os.environ,
            {
                **MONGO_ENV,
                "MONGO_HOST": "mongodb",
                "MONGO_PORT": "27018",
            },
            clear=True,
        ):
            uri = build_mongo_uri()

        self.assertEqual(
            uri,
            "mongodb://projectadmin:p%40ss%20word@mongodb:27018/?authSource=admin",
        )

    def test_prefers_explicit_uri(self) -> None:
        with patch.dict(
            os.environ,
            {"MONGO_URI": "mongodb://example.test:27017"},
            clear=True,
        ):
            uri = build_mongo_uri()

        self.assertEqual(uri, "mongodb://example.test:27017")

    def test_missing_credentials_name_the_variable(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(
                RuntimeError,
                "MONGO_ROOT_USERNAME",
            ):
                build_mongo_uri()


class DecodeRegistrationTests(unittest.TestCase):
    def test_decodes_valid_event(self) -> None:
        raw_value = (
            b'{"_id": "2d255c63-6d9c-446a-bdc1-5d56a2b052dc", '
            b'"registered_at": "2026-10-29T09:12:48.644Z"}'
        )

        registration = decode_registration(raw_value)

        self.assertEqual(
            registration,
            {
                "_id": "2d255c63-6d9c-446a-bdc1-5d56a2b052dc",
                "registered_at": "2026-10-29T09:12:48.644Z",
            },
        )

    def test_rejects_invalid_json(self) -> None:
        self.assertIsNone(decode_registration(b"not-json"))

    def test_rejects_non_object_event(self) -> None:
        self.assertIsNone(decode_registration(b'["value"]'))

    def test_rejects_event_without_user_id(self) -> None:
        self.assertIsNone(
            decode_registration(b'{"registered_at": "2026-10-29T09:12:48Z"}')
        )

    def test_rejects_event_without_timestamp(self) -> None:
        self.assertIsNone(
            decode_registration(b'{"_id": "2d255c63-6d9c-446a-bdc1-5d56a2b052dc"}')
        )


class ProducerTests(unittest.TestCase):
    def test_builds_uuid_and_utc_timestamp(self) -> None:
        now = datetime(2026, 10, 29, 12, 34, 56, 789000, tzinfo=timezone.utc)

        registration = build_registration(now)

        uuid.UUID(registration["_id"])
        self.assertEqual(
            registration["registered_at"],
            "2026-10-29T12:34:56.789000+00:00",
        )
        self.assertEqual(
            sorted(registration),
            ["_id", "registered_at"],
        )

    def test_interval_defaults_to_5000_seconds(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(build_interval_seconds(), 5000.0)

    def test_interval_must_be_positive(self) -> None:
        with patch.dict(
            os.environ,
            {"PRODUCER_INTERVAL_SECONDS": "0"},
            clear=True,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "PRODUCER_INTERVAL_SECONDS",
            ):
                build_interval_seconds()

    def test_interval_must_be_a_number(self) -> None:
        with patch.dict(
            os.environ,
            {"PRODUCER_INTERVAL_SECONDS": "soon"},
            clear=True,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "PRODUCER_INTERVAL_SECONDS",
            ):
                build_interval_seconds()


if __name__ == "__main__":
    unittest.main()
