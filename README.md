# user-registration-pipeline
Python, Docker, Spark, Kafka, Snowflake

## Snowflake export

`spark/daily_registration_job.py` aggregates registrations per day and writes
the result to Snowflake. The target table is replaced atomically through a
Snowflake staging table. The job refuses to write when MongoDB returns no
registrations, so an empty source cannot erase existing Snowflake data.

For local development, run `snowflake/setup.sql` once in a Snowflake worksheet
as `ACCOUNTADMIN`. It creates the warehouse, database, and schema, then prints
the active role, user, warehouse, database, and schema.

Add these variables to `.env`:

```dotenv
SNOWFLAKE_ACCOUNT=your-account-identifier
SNOWFLAKE_USER=your-user
SNOWFLAKE_PRIVATE_KEY_PATH=~/.snowflake/rsa_key.p8
SNOWFLAKE_DATABASE=USER_REGISTRATION_DB
SNOWFLAKE_SCHEMA=ANALYTICS
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_TABLE=DAILY_REGISTRATIONS
SNOWFLAKE_ROLE=your-role
```

Set `SNOWFLAKE_PASSWORD` instead of `SNOWFLAKE_PRIVATE_KEY_PATH` to use password
authentication. The key path takes precedence when both are present.

`SNOWFLAKE_ACCOUNT` accepts either the bare account identifier or a hostname
ending in `.snowflakecomputing.com`. `SNOWFLAKE_ROLE` is optional. Database,
schema, warehouse, table, and role must be simple unquoted identifiers
containing only letters, digits, underscores, or dollar signs; they are
normalized to uppercase.

## Key-pair authentication

Generate an unencrypted PKCS#8 key pair:

```powershell
powershell -ExecutionPolicy Bypass -File snowflake/generate_rsa_key.ps1
```

The script writes `%USERPROFILE%\.snowflake\rsa_key.p8`, prints the public key
body, and refuses to overwrite an existing key. Register that public key once in
Snowflake as `ACCOUNTADMIN`:

```sql
ALTER USER <your-snowflake-user> SET RSA_PUBLIC_KEY='<printed-public-key>';
```

Set `SNOWFLAKE_PRIVATE_KEY_PATH` as shown above and remove
`SNOWFLAKE_PASSWORD` from `.env`.

Relative paths are resolved from the project root. The job reads the key,
validates that it is an unencrypted PKCS#8 key, strips the PEM armor, and passes
the base64 body to the connector. Encrypted keys and PKCS#1 keys beginning with
`-----BEGIN RSA PRIVATE KEY-----` are rejected before Spark starts, together
with the `openssl pkcs8 -topk8 -nocrypt` command that converts them.

The Snowflake role needs `USAGE` on the database, schema, and warehouse, plus
`CREATE TABLE` and `CREATE STAGE` on the schema. The job creates the target
table through JDBC before writing, so the first run does not depend on a manual
DDL step:

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
python -m unittest discover -s snowflake -p "test_*.py" -v
```

## Backend and frontend

`backend/` is an Express service that publishes registrations to Kafka and reads
daily counts from MongoDB. It serves the static page in `frontend/`.

Install and start it:

```powershell
cd backend
npm.cmd install
npm.cmd start
```

Open http://localhost:3000. The service loads `.env` from the project root.

Optional variables:

| Variable | Default |
| --- | --- |
| `PORT` | `3000` |
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9094` |
| `KAFKA_TOPIC` | `user-registrations` |
| `MONGO_HOST` | `localhost` |
| `MONGO_PORT` | `27017` |
| `MONGO_DATABASE` | `UserRegistrationDB` |
| `MONGO_COLLECTION` | `User` |

`MONGO_ROOT_USERNAME` and `MONGO_ROOT_PASSWORD` are required.
`MONGO_URI` overrides the assembled MongoDB connection string.

Run the backend tests with:

```powershell
cd backend
npm.cmd test
```

