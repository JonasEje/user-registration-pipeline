import os
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_date

# .env aus dem Projekt-Root laden, unabhängig vom Arbeitsverzeichnis
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

# Windows: Spark/Hadoop benötigt winutils.exe, HADOOP_HOME muss gesetzt sein
if os.name == "nt" and "HADOOP_HOME" not in os.environ:
    default_hadoop_home = Path.home() / "hadoop"
    if (default_hadoop_home / "bin" / "winutils.exe").is_file():
        os.environ["HADOOP_HOME"] = str(default_hadoop_home)
        os.environ["PATH"] = (
            f"{default_hadoop_home / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"
        )

# MongoDB-Zugangsdaten aus der Umgebung lesen
mongo_username = os.environ["MONGO_ROOT_USERNAME"]
mongo_password = os.environ["MONGO_ROOT_PASSWORD"]

# Vom Windows-Host aus ist MongoDB über localhost erreichbar,
# aus einem Container heraus über den Service-Namen "mongodb".
mongo_host = os.environ.get("MONGO_HOST", "localhost")
mongo_port = os.environ.get("MONGO_PORT", "27017")

mongo_uri = (
    f"mongodb://{quote_plus(mongo_username)}:{quote_plus(mongo_password)}"
    f"@{mongo_host}:{mongo_port}/?authSource=admin"
)

# Spark starten und MongoDB-Verbindung konfigurieren
spark = (
    SparkSession.builder
    .appName("DailyRegistrationJob")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0"
    )
    .config("spark.mongodb.read.connection.uri", mongo_uri)
    .config("spark.mongodb.read.database", "UserRegistrationDB")
    .config("spark.mongodb.read.collection", "User")
    .getOrCreate()
)

try:
    # Registrierungen aus der lokalen MongoDB laden
    users_df = spark.read.format("mongodb").load()

    # Registrierungen nach Datum zählen
    daily_counts = (
        users_df
        .withColumn(
            "registration_date",
            to_date(col("registered_at"))
        )
        .filter(col("registration_date").isNotNull())
        .groupBy("registration_date")
        .agg(count("*").alias("new_users"))
        .orderBy("registration_date")
    )

    print("Tägliche Registrierungen aus MongoDB:")
    daily_counts.show(truncate=False)

finally:
    spark.stop()
