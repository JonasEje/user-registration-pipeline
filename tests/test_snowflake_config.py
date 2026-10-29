import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark.daily_registration_job import (
<<<<<<< Updated upstream:tests/test_snowflake_config.py
=======
    PROJECT_ROOT,
    build_mongo_uri,
>>>>>>> Stashed changes:snowflake/test_snowflake_config.py
    build_snowflake_options,
    create_snowflake_table_statement,
    ensure_registration_columns,
    mongo_collection,
    mongo_database,
    qualified_snowflake_table,
)

BASE_ENV = {
    "SNOWFLAKE_ACCOUNT": "myorg-myaccount",
    "SNOWFLAKE_USER": "pipeline_user",
    "SNOWFLAKE_PASSWORD": "secret",
    "SNOWFLAKE_DATABASE": "USER_REGISTRATION_DB",
    "SNOWFLAKE_SCHEMA": "ANALYTICS",
    "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
    "SNOWFLAKE_TABLE": "DAILY_REGISTRATIONS",
}


class MongoSourceTests(unittest.TestCase):
    def test_explicit_mongo_uri_is_used(self) -> None:
        with patch.dict(
            os.environ,
            {"MONGO_URI": "mongodb://example.test:27017"},
            clear=True,
        ):
            self.assertEqual(
                build_mongo_uri(),
                "mongodb://example.test:27017",
            )

    def test_mongo_credentials_are_percent_encoded(self) -> None:
        env = {
            "MONGO_ROOT_USERNAME": "project admin",
            "MONGO_ROOT_PASSWORD": "p@ss word",
        }

        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(
                build_mongo_uri(),
                "mongodb://project%20admin:p%40ss%20word@localhost:27017/"
                "?authSource=admin",
            )

    def test_database_and_collection_come_from_environment(self) -> None:
        env = {
            "MONGO_DATABASE": "OtherDatabase",
            "MONGO_COLLECTION": "OtherCollection",
        }

        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(mongo_database(), "OtherDatabase")
            self.assertEqual(mongo_collection(), "OtherCollection")

    def test_database_and_collection_have_defaults(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(mongo_database(), "UserRegistrationDB")
            self.assertEqual(mongo_collection(), "User")

    def test_missing_registered_at_column_is_rejected(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "No registrations found"):
            ensure_registration_columns(["_id"])

    def test_registered_at_column_is_accepted(self) -> None:
        ensure_registration_columns(["_id", "registered_at"])


class SnowflakeConfigTests(unittest.TestCase):
    def test_password_auth_builds_options(self) -> None:
        with patch.dict(os.environ, BASE_ENV, clear=True):
            options = build_snowflake_options()

        self.assertEqual(
            options["sfURL"],
            "myorg-myaccount.snowflakecomputing.com",
        )
        self.assertEqual(options["sfUser"], "pipeline_user")
        self.assertEqual(options["sfDatabase"], "USER_REGISTRATION_DB")
        self.assertEqual(options["sfSchema"], "ANALYTICS")
        self.assertEqual(options["sfWarehouse"], "COMPUTE_WH")
        self.assertEqual(options["sfPassword"], "secret")
        self.assertNotIn("pem_private_key", options)
        self.assertNotIn("sfRole", options)

    def test_identifiers_are_normalized_to_uppercase(self) -> None:
        env = {
            **BASE_ENV,
            "SNOWFLAKE_DATABASE": "user_registration_db",
            "SNOWFLAKE_SCHEMA": "analytics",
            "SNOWFLAKE_TABLE": "daily_registrations",
            "SNOWFLAKE_WAREHOUSE": "compute_wh",
            "SNOWFLAKE_ROLE": "pipeline_role",
        }

        with patch.dict(os.environ, env, clear=True):
            options = build_snowflake_options()
            table = qualified_snowflake_table()

        self.assertEqual(options["sfDatabase"], "USER_REGISTRATION_DB")
        self.assertEqual(options["sfSchema"], "ANALYTICS")
        self.assertEqual(options["sfWarehouse"], "COMPUTE_WH")
        self.assertEqual(options["sfRole"], "PIPELINE_ROLE")
        self.assertEqual(
            table,
            "USER_REGISTRATION_DB.ANALYTICS.DAILY_REGISTRATIONS",
        )

    def test_full_snowflake_host_is_preserved(self) -> None:
        env = {
            **BASE_ENV,
            "SNOWFLAKE_ACCOUNT": (
                "https://myorg-myaccount.snowflakecomputing.com/"
            ),
        }

        with patch.dict(os.environ, env, clear=True):
            options = build_snowflake_options()

        self.assertEqual(
            options["sfURL"],
            "myorg-myaccount.snowflakecomputing.com",
        )

    def test_private_key_replaces_password(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            key_path = Path(temp_dir) / "rsa_key.p8"
            key_text = (
                "-----BEGIN PRIVATE KEY-----\n"
                "test-key-material\n"
                "-----END PRIVATE KEY-----\n"
            )
            key_path.write_text(key_text, encoding="utf-8")

            env = {
                key: value
                for key, value in BASE_ENV.items()
                if key != "SNOWFLAKE_PASSWORD"
            }
            env["SNOWFLAKE_PRIVATE_KEY_PATH"] = str(key_path)

            with patch.dict(os.environ, env, clear=True):
                options = build_snowflake_options()

        self.assertEqual(options["pem_private_key"], key_text)
        self.assertNotIn("sfPassword", options)

    def test_missing_private_key_file_fails(self) -> None:
        env = {
            key: value
            for key, value in BASE_ENV.items()
            if key != "SNOWFLAKE_PASSWORD"
        }
        env["SNOWFLAKE_PRIVATE_KEY_PATH"] = "missing-key.p8"

        with patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(
                RuntimeError,
                "SNOWFLAKE_PRIVATE_KEY_PATH",
            ):
                build_snowflake_options()

    def test_missing_password_fails(self) -> None:
        env = {
            key: value
            for key, value in BASE_ENV.items()
            if key != "SNOWFLAKE_PASSWORD"
        }

        with patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(RuntimeError, "SNOWFLAKE_PASSWORD"):
                build_snowflake_options()

    def test_invalid_identifier_is_rejected(self) -> None:
        env = {
            **BASE_ENV,
            "SNOWFLAKE_TABLE": "daily-registrations",
        }

        with patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(RuntimeError, "SNOWFLAKE_TABLE"):
                qualified_snowflake_table()

    def test_qualified_table_and_ddl(self) -> None:
        with patch.dict(os.environ, BASE_ENV, clear=True):
            table = qualified_snowflake_table()

        self.assertEqual(
            table,
            "USER_REGISTRATION_DB.ANALYTICS.DAILY_REGISTRATIONS",
        )
        self.assertEqual(
            create_snowflake_table_statement(table),
            (
                "CREATE TABLE IF NOT EXISTS "
                "USER_REGISTRATION_DB.ANALYTICS.DAILY_REGISTRATIONS "
                "(registration_date DATE, new_users BIGINT)"
            ),
        )


if __name__ == "__main__":
    unittest.main()
