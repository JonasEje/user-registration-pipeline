import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count
load_dotenv()


# MongoDB-Zugangsdaten aus der Umgebung lesen
mongo_username = os.environ["MONGO_ROOT_USERNAME"]
mongo_password = os.environ["MONGO_ROOT_PASSWORD"]

mongo_uri = (
    f"mongodb://{mongo_username}:{mongo_password}"
    "@mongodb:27017/?authSource=admin"
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
    .getOrCreate()
)

try:
    # Registrierungen aus der lokalen MongoDB laden
    users_df = (
        spark.read
        .format("mongodb")
        .option("database", "UserRegistrationDB")
        .option("collection", "User")
        .load()
    )

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