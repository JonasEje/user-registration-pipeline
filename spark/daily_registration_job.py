import base64
import binascii
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import quote

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, to_date

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MONGO_DATABASE = "UserRegistrationDB"
DEFAULT_MONGO_COLLECTION = "User"
MONGO_CONNECTOR_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0"
SNOWFLAKE_CONNECTOR_PACKAGE = (
    "net.snowflake:spark-snowflake_2.13:3.2.3-spark_4.1"
)
SNOWFLAKE_FORMAT = "net.snowflake.spark.snowflake"
SNOWFLAKE_JDBC_DRIVER = "net.snowflake.client.jdbc.SnowflakeDriver"
SNOWFLAKE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
PEM_PKCS8_HEADER = "-----BEGIN PRIVATE KEY-----"
PEM_PKCS8_FOOTER = "-----END PRIVATE KEY-----"
PEM_PKCS1_HEADER = "-----BEGIN RSA PRIVATE KEY-----"
PEM_ENCRYPTED_HEADER = "-----BEGIN ENCRYPTED PRIVATE KEY-----"
PEM_LEGACY_ENCRYPTION_MARKER = "Proc-Type: 4,ENCRYPTED"
PRIVATE_KEY_CONVERSION_HINT = (
    "openssl pkcs8 -topk8 -nocrypt -in rsa_key.pem -out rsa_key.p8"
)


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def optional_env(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def configure_hadoop_home() -> None:
    if os.name != "nt" or "HADOOP_HOME" in os.environ:
        return

    default_hadoop_home = Path.home() / "hadoop"
    if (default_hadoop_home / "bin" / "winutils.exe").is_file():
        os.environ["HADOOP_HOME"] = str(default_hadoop_home)
        os.environ["PATH"] = (
            f"{default_hadoop_home / 'bin'}{os.pathsep}"
            f"{os.environ.get('PATH', '')}"
        )


def build_mongo_uri() -> str:
    explicit_uri = optional_env("MONGO_URI")
    if explicit_uri:
        return explicit_uri

    username = quote(require_env("MONGO_ROOT_USERNAME"), safe="")
    password = quote(require_env("MONGO_ROOT_PASSWORD"), safe="")
    host = optional_env("MONGO_HOST") or "localhost"
    port = optional_env("MONGO_PORT") or "27017"
    return f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"


def mongo_database() -> str:
    return optional_env("MONGO_DATABASE") or DEFAULT_MONGO_DATABASE


def mongo_collection() -> str:
    return optional_env("MONGO_COLLECTION") or DEFAULT_MONGO_COLLECTION


def ensure_registration_columns(columns: list[str]) -> None:
    if "registered_at" not in columns:
        raise RuntimeError(
            "No registrations found in MongoDB; refusing to overwrite Snowflake"
        )


def validate_identifier(value: str, name: str) -> str:
    if not SNOWFLAKE_IDENTIFIER_PATTERN.fullmatch(value):
        raise RuntimeError(
            f"{name} must be a simple Snowflake identifier containing only "
            "letters, digits, underscores, or dollar signs"
        )
    return value.upper()


def normalize_snowflake_host(account: str) -> str:
    host = account.removeprefix("https://").removeprefix("http://").rstrip("/")
    if host.endswith(".snowflakecomputing.com"):
        return host
    return f"{host}.snowflakecomputing.com"


def load_private_key(path: Path) -> str:
    try:
        key_text = path.read_text(encoding="utf-8-sig")
    except OSError as error:
        raise RuntimeError(
            f"Unable to read SNOWFLAKE_PRIVATE_KEY_PATH: {path}"
        ) from error

    if (
        PEM_ENCRYPTED_HEADER in key_text
        or PEM_LEGACY_ENCRYPTION_MARKER in key_text
    ):
        raise RuntimeError(
            "SNOWFLAKE_PRIVATE_KEY_PATH must be an unencrypted PEM key; "
            f"convert it with: {PRIVATE_KEY_CONVERSION_HINT}"
        )

    if PEM_PKCS1_HEADER in key_text:
        raise RuntimeError(
            "SNOWFLAKE_PRIVATE_KEY_PATH contains a PKCS#1 key; "
            f"convert it with: {PRIVATE_KEY_CONVERSION_HINT}"
        )

    if PEM_PKCS8_HEADER in key_text:
        if PEM_PKCS8_FOOTER not in key_text:
            raise RuntimeError(
                "SNOWFLAKE_PRIVATE_KEY_PATH contains an incomplete PEM key; "
                f"expected {PEM_PKCS8_FOOTER}"
            )
        key_body = key_text.split(PEM_PKCS8_HEADER, 1)[1].split(
            PEM_PKCS8_FOOTER,
            1,
        )[0]
    else:
        key_body = key_text

    compact_key_body = "".join(key_body.split())
    if not compact_key_body:
        raise RuntimeError(
            "SNOWFLAKE_PRIVATE_KEY_PATH does not contain a private key"
        )

    try:
        der_bytes = base64.b64decode(compact_key_body, validate=True)
    except (binascii.Error, ValueError) as error:
        raise RuntimeError(
            "SNOWFLAKE_PRIVATE_KEY_PATH must contain a PKCS#8 private key "
            "or its base64 body"
        ) from error

    if not der_bytes or der_bytes[0] != 0x30:
        raise RuntimeError(
            "SNOWFLAKE_PRIVATE_KEY_PATH must contain a PKCS#8 private key "
            "or its base64 body"
        )

    return compact_key_body


def resolve_private_key_path() -> Path | None:
    private_key_path = optional_env("SNOWFLAKE_PRIVATE_KEY_PATH")
    if not private_key_path:
        return None
    key_path = Path(private_key_path).expanduser()
    return key_path if key_path.is_absolute() else PROJECT_ROOT / key_path


def build_snowflake_options() -> dict[str, str]:
    database = validate_identifier(
        require_env("SNOWFLAKE_DATABASE"),
        "SNOWFLAKE_DATABASE",
    )
    schema = validate_identifier(
        require_env("SNOWFLAKE_SCHEMA"),
        "SNOWFLAKE_SCHEMA",
    )
    warehouse = validate_identifier(
        require_env("SNOWFLAKE_WAREHOUSE"),
        "SNOWFLAKE_WAREHOUSE",
    )

    options = {
        "sfURL": normalize_snowflake_host(require_env("SNOWFLAKE_ACCOUNT")),
        "sfUser": require_env("SNOWFLAKE_USER"),
        "sfDatabase": database,
        "sfSchema": schema,
        "sfWarehouse": warehouse,
    }

    role = optional_env("SNOWFLAKE_ROLE")
    if role:
        options["sfRole"] = validate_identifier(role, "SNOWFLAKE_ROLE")

    key_path = resolve_private_key_path()
    if key_path is not None:
        options["pem_private_key"] = load_private_key(key_path)
    else:
        options["sfPassword"] = require_env("SNOWFLAKE_PASSWORD")

    return options


def build_jdbc_properties(jvm: Any, options: dict[str, str]) -> Any:
    properties = jvm.java.util.Properties()
    properties.setProperty("user", options["sfUser"])

    key_path = resolve_private_key_path()
    if key_path is None:
        properties.setProperty("password", options["sfPassword"])
    else:
        properties.setProperty("private_key_file", str(key_path))
        properties.setProperty("authenticator", "SNOWFLAKE_JWT")

    if "sfRole" in options:
        properties.setProperty("role", options["sfRole"])
    properties.setProperty("warehouse", options["sfWarehouse"])
    return properties


def execute_snowflake_statement(
    spark: SparkSession,
    options: dict[str, str],
    statement_sql: str,
) -> None:
    jvm = spark.sparkContext._jvm
    class_loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    driver = class_loader.loadClass(SNOWFLAKE_JDBC_DRIVER).newInstance()
    connection = driver.connect(
        f"jdbc:snowflake://{options['sfURL']}/",
        build_jdbc_properties(jvm, options),
    )
    if connection is None:
        raise RuntimeError("Snowflake JDBC driver returned no connection")

    try:
        statement = connection.createStatement()
        try:
            statement.execute(statement_sql)
        finally:
            statement.close()
    finally:
        connection.close()


def qualified_snowflake_table() -> str:
    database = validate_identifier(
        require_env("SNOWFLAKE_DATABASE"),
        "SNOWFLAKE_DATABASE",
    )
    schema = validate_identifier(
        require_env("SNOWFLAKE_SCHEMA"),
        "SNOWFLAKE_SCHEMA",
    )
    table = validate_identifier(
        require_env("SNOWFLAKE_TABLE"),
        "SNOWFLAKE_TABLE",
    )
    return f"{database}.{schema}.{table}"


def create_snowflake_table_statement(table: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table} "
        "(registration_date DATE, new_users BIGINT)"
    )


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("DailyRegistrationJob")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            f"{MONGO_CONNECTOR_PACKAGE},{SNOWFLAKE_CONNECTOR_PACKAGE}",
        )
        .config("spark.mongodb.read.connection.uri", build_mongo_uri())
        .config("spark.mongodb.read.database", mongo_database())
        .config("spark.mongodb.read.collection", mongo_collection())
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def load_daily_counts(spark: SparkSession) -> DataFrame:
    users_df = spark.read.format("mongodb").load()
    ensure_registration_columns(users_df.columns)
    return (
        users_df.withColumn(
            "registration_date",
            to_date(col("registered_at")),
        )
        .filter(col("registration_date").isNotNull())
        .groupBy("registration_date")
        .agg(count("*").alias("new_users"))
        .orderBy("registration_date")
        .select("registration_date", "new_users")
    )


def write_daily_counts(
    daily_counts: DataFrame,
    options: dict[str, str],
    table: str,
) -> None:
    (
        daily_counts.write.format(SNOWFLAKE_FORMAT)
        .options(**options)
        .option("dbtable", table)
        .option("truncate_table", "on")
        .option("usestagingtable", "on")
        .option("keep_column_case", "off")
        .mode("overwrite")
        .save()
    )


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    configure_hadoop_home()

    snowflake_options = build_snowflake_options()
    snowflake_table = qualified_snowflake_table()

    spark = create_spark_session()
    try:
        daily_counts = load_daily_counts(spark)
        daily_counts.persist()

        try:
            if daily_counts.isEmpty():
                raise RuntimeError(
                    "No registrations found in MongoDB; refusing to "
                    "overwrite Snowflake"
                )

            execute_snowflake_statement(
                spark,
                snowflake_options,
                create_snowflake_table_statement(snowflake_table),
            )

            print("Daily registrations from MongoDB:")
            daily_counts.show(truncate=False)

            write_daily_counts(daily_counts, snowflake_options, snowflake_table)
            print("Daily registrations written to Snowflake.")
        finally:
            daily_counts.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
