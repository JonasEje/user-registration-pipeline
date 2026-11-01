import base64
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark.daily_registration_job import (
    PROJECT_ROOT,
    build_mongo_uri,
    build_snowflake_options,
    create_registrations_table_statement,
    create_snowflake_table_statement,
    ensure_registration_columns,
    mongo_collection,
    mongo_database,
    qualified_snowflake_registrations_table,
    qualified_snowflake_table,
    resolve_private_key_path,
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

PRIVATE_KEY_BODY = base64.b64encode(
    bytes([0x30, 0x82, 0x01, 0x00]) + bytes(16)
).decode()
PRIVATE_KEY_PEM = (
    "-----BEGIN PRIVATE KEY-----\n"
    f"{PRIVATE_KEY_BODY}\n"
    "-----END PRIVATE KEY-----\n"
)


def write_key(directory: Path, name: str, content: str) -> str:
    path = directory / name
    path.write_text(content, encoding="utf-8")
    return str(path)


def key_environment(private_key_path: str) -> dict[str, str]:
    environment = {
        key: value
        for key, value in BASE_ENV.items()
        if key != "SNOWFLAKE_PASSWORD"
    }
    environment["SNOWFLAKE_PRIVATE_KEY_PATH"] = private_key_path
    return environment


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


class RegistrationsTableTests(unittest.TestCase):
    def test_registrations_table_has_default_name(self) -> None:
        with patch.dict(os.environ, BASE_ENV, clear=True):
            self.assertEqual(
                qualified_snowflake_registrations_table(),
                "USER_REGISTRATION_DB.ANALYTICS.REGISTRATIONS",
            )

    def test_registrations_table_name_comes_from_environment(self) -> None:
        with patch.dict(
            os.environ,
            {**BASE_ENV, "SNOWFLAKE_REGISTRATIONS_TABLE": "raw_events"},
            clear=True,
        ):
            self.assertEqual(
                qualified_snowflake_registrations_table(),
                "USER_REGISTRATION_DB.ANALYTICS.RAW_EVENTS",
            )

    def test_registrations_table_ddl(self) -> None:
        self.assertEqual(
            create_registrations_table_statement("DB.SCHEMA.REGISTRATIONS"),
            "CREATE TABLE IF NOT EXISTS DB.SCHEMA.REGISTRATIONS "
            "(user_id VARCHAR, registered_at TIMESTAMP_TZ)",
        )


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

    def test_private_key_path_is_optional(self) -> None:
        with patch.dict(os.environ, BASE_ENV, clear=True):
            self.assertIsNone(resolve_private_key_path())

    def test_absolute_private_key_path_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            key_path = Path(temp_dir) / "rsa_key.p8"
            with patch.dict(
                os.environ,
                {"SNOWFLAKE_PRIVATE_KEY_PATH": str(key_path)},
                clear=True,
            ):
                resolved = resolve_private_key_path()

        self.assertEqual(resolved, key_path)

    def test_relative_private_key_path_is_resolved_from_project_root(
        self,
    ) -> None:
        with patch.dict(
            os.environ,
            {"SNOWFLAKE_PRIVATE_KEY_PATH": "keys/rsa_key.p8"},
            clear=True,
        ):
            resolved = resolve_private_key_path()

        self.assertEqual(
            resolved,
            PROJECT_ROOT / "keys" / "rsa_key.p8",
        )

    def test_private_key_replaces_password(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            key_path = write_key(
                Path(temp_dir),
                "rsa_key.p8",
                PRIVATE_KEY_PEM,
            )

            with patch.dict(
                os.environ,
                key_environment(key_path),
                clear=True,
            ):
                options = build_snowflake_options()

        self.assertEqual(options["pem_private_key"], PRIVATE_KEY_BODY)
        self.assertNotIn("sfPassword", options)

    def test_bare_base64_key_body_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            key_path = write_key(
                Path(temp_dir),
                "rsa_key.b64",
                PRIVATE_KEY_BODY,
            )

            with patch.dict(
                os.environ,
                key_environment(key_path),
                clear=True,
            ):
                options = build_snowflake_options()

        self.assertEqual(options["pem_private_key"], PRIVATE_KEY_BODY)

    def test_pkcs1_private_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            key_path = write_key(
                Path(temp_dir),
                "rsa_key.pem",
                (
                    "-----BEGIN RSA PRIVATE KEY-----\n"
                    "test-key-material\n"
                    "-----END RSA PRIVATE KEY-----\n"
                ),
            )

            with patch.dict(
                os.environ,
                key_environment(key_path),
                clear=True,
            ):
                with self.assertRaisesRegex(RuntimeError, "PKCS#1"):
                    build_snowflake_options()

    def test_encrypted_private_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            key_path = write_key(
                Path(temp_dir),
                "rsa_key.p8",
                (
                    "-----BEGIN ENCRYPTED PRIVATE KEY-----\n"
                    "test-key-material\n"
                    "-----END ENCRYPTED PRIVATE KEY-----\n"
                ),
            )

            with patch.dict(
                os.environ,
                key_environment(key_path),
                clear=True,
            ):
                with self.assertRaisesRegex(RuntimeError, "unencrypted"):
                    build_snowflake_options()

    def test_private_key_without_pem_markers_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            key_path = write_key(
                Path(temp_dir),
                "rsa_key.p8",
                "not-a-pem-key\n",
            )

            with patch.dict(
                os.environ,
                key_environment(key_path),
                clear=True,
            ):
                with self.assertRaisesRegex(RuntimeError, "PKCS#8"):
                    build_snowflake_options()

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
