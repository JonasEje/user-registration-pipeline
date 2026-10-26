import os
import re
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, to_date

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MONGO_DATABASE = "UserRegistrationDB"
MONGO_COLLECTION = "User"
MONGO_CONNECTOR_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0"
SNOWFLAKE_CONNECTOR_PACKAGE = (
    "net.snowflake:spark-snowflake_2.13:3.2.3-spark_4.1"
)
SNOWFLAKE_FORMAT = "net.snowflake.spark.snowflake"
SNOWFLAKE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


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
    username = quote_plus(require_env("MONGO_ROOT_USERNAME"))
    password = quote_plus(require_env("MONGO_ROOT_PASSWORD"))
    host = os.environ.get("MONGO_HOST", "localhost")
    port = os.environ.get("MONGO_PORT", "27017")
    return f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"


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

    private_key_path = optional_env("SNOWFLAKE_PRIVATE_KEY_PATH")
    if private_key_path:
        key_path = Path(private_key_path).expanduser()
        if not key_path.is_absolute():
            key_path = PROJECT_ROOT / key_path

        try:
            key_text = key_path.read_text(encoding="utf-8")
        except OSError as error:
            raise RuntimeError(
                f"Unable to read SNOWFLAKE_PRIVATE_KEY_PATH: {key_path}"
            ) from error

        options["pem_private_key"] = key_text
    else:
        options["sfPassword"] = require_env("SNOWFLAKE_PASSWORD")

    return options


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
        .config("spark.mongodb.read.database", MONGO_DATABASE)
        .config("spark.mongodb.read.collection", MONGO_COLLECTION)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def load_daily_counts(spark: SparkSession) -> DataFrame:
    users_df = spark.read.format("mongodb").load()
    return (
        users_df.withColumn(
            "registration_date",
            to_date(col("registered_at")),
        )
        .filter(col("registration_date").isNotNull())
        .groupBy("registration_date")
        .agg(count("*").alias("new_users"))
        .orderBy("registration_date")
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
        .option("preactions", create_snowflake_table_statement(table))
        .option("truncate_table", "on")
        .option("usestagingtable", "on")
        .option("column_mapping", "name")
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
