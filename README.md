# user-registration-pipeline
Python, Docker, Spark, Kafka, Snowflake

## Snowflake export

`spark/daily_registration_job.py` aggregates registrations per day and writes
the result to Snowflake. The target table is replaced atomically through a
Snowflake staging table. The job refuses to write when MongoDB returns no
registrations, so an empty source cannot erase existing Snowflake data.

Add these variables to `.env`:

```dotenv
SNOWFLAKE_ACCOUNT=your-account-identifier
SNOWFLAKE_USER=your-user
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_DATABASE=USER_REGISTRATION_DB
SNOWFLAKE_SCHEMA=ANALYTICS
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_TABLE=DAILY_REGISTRATIONS
SNOWFLAKE_ROLE=your-role
```

`SNOWFLAKE_ACCOUNT` accepts either the bare account identifier or a hostname
ending in `.snowflakecomputing.com`. `SNOWFLAKE_ROLE` is optional. Database,
schema, warehouse, table, and role must be simple unquoted identifiers
containing only letters, digits, underscores, or dollar signs; they are
normalized to uppercase.

For key-pair authentication, omit `SNOWFLAKE_PASSWORD` and set
`SNOWFLAKE_PRIVATE_KEY_PATH` to an unencrypted PEM private key. Relative paths
are resolved from the project root.

The Snowflake role needs `USAGE` on the database, schema, and warehouse, plus
`CREATE TABLE` and `CREATE STAGE` on the schema. The target table is created if
it does not exist:

```sql
CREATE TABLE IF NOT EXISTS USER_REGISTRATION_DB.ANALYTICS.DAILY_REGISTRATIONS (
    registration_date DATE,
    new_users BIGINT
);
```

The Spark job uses connector `net.snowflake:spark-snowflake_2.13:3.2.3-spark_4.1`
and downloads the connector and JDBC driver on first run. The machine running
the job needs access to Maven Central and Snowflake.

## Spark on Windows

Spark requires Hadoop's `winutils.exe` on Windows. Without it, Spark fails to
start with `HADOOP_HOME and hadoop.home.dir are unset`.

1. Download `winutils.exe` and `hadoop.dll` from
   [kontext-tech/winutils](https://github.com/kontext-tech/winutils/tree/master/hadoop-3.4.0-win10-x64/bin)
   into `%USERPROFILE%\hadoop\bin`.
2. Set `HADOOP_HOME=%USERPROFILE%\hadoop` and add `%HADOOP_HOME%\bin` to `PATH`.
3. Restart the terminal / VS Code so the new environment variables are picked up.

`spark/daily_registration_job.py` also auto-detects `%USERPROFILE%\hadoop\bin\winutils.exe`,
so step 2 can be skipped when running that script.

## Running the daily registration job

```powershell
python spark/daily_registration_job.py
```

The script loads credentials from `.env`, connects to the MongoDB container
through `localhost:27017`, and writes the daily aggregate to Snowflake. Set
`MONGO_HOST=mongodb` when running inside the Docker network.

Run the configuration tests with:

```powershell
python -m unittest discover -s tests -v
```
