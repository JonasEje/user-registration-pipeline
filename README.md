# user-registration-pipeline
Python, Docker, Spark, Kafka, Snowflake

## Docker stack

Start MongoDB, Kafka, the Python app, the backend, and Kafka UI:

```powershell
docker compose up -d --build
```

The `app` service contains `producer/producer.py`, `consumer/consumer.py`, and
`spark/daily_registration_job.py`. It runs the producer and consumer together:
one demo registration is published every `PRODUCER_INTERVAL_SECONDS` (default
`5000`, so every 5000 seconds) and stored in MongoDB.

The `backend` service serves the Express API and the static dashboard from
`frontend/` at http://localhost:3000. Start only the backend and its
dependencies:

```powershell
docker compose up -d backend
```

Rebuild it after changing `backend/` or `frontend/`:

```powershell
docker compose up -d --build backend
```

Open Kafka UI at http://localhost:8080 to inspect the `local` cluster,
`user-registrations` topic, and consumer group.

Run the Snowflake export inside the running container:

```powershell
docker compose exec app python spark/daily_registration_job.py
```

The container mounts `~/.snowflake` read-only at `/keys`, so the RSA key is
available at `/keys/rsa_key.p8`. Set `SNOWFLAKE_KEY_DIR` when the key lives
elsewhere.

| Variable | Default |
| --- | --- |
| `PRODUCER_INTERVAL_SECONDS` | `5000` |
| `KAFKA_TOPIC` | `user-registrations` |
| `KAFKA_GROUP_ID` | `user-registration-consumer` |
| `MONGO_DATABASE` | `UserRegistrationDB` |
| `MONGO_COLLECTION` | `User` |
| `SNOWFLAKE_KEY_DIR` | `~/.snowflake` |

Compose connects the container to `kafka:9092` and `mongodb:27017`, so the
host-only defaults do not apply inside Docker.

Stop the stack with `docker compose down`.

## Snowflake export

`spark/daily_registration_job.py` aggregates registrations per day and writes
the result to Snowflake. The target table is replaced atomically through a
Snowflake staging table. The job refuses to write when MongoDB returns no
registrations, so an empty source cannot erase existing Snowflake data.

For local development, run `snowflake/setup.sql` once in a Snowflake worksheet
as `ACCOUNTADMIN`. It creates the warehouse, database, schema, the target
table, and the cumulative view `DAILY_REGISTRATIONS_TOTAL`, then prints the
current rows.

Inspect the exported rows with the Snowflake CLI:

```powershell
snow sql -q "select * from USER_REGISTRATION_DB.ANALYTICS.DAILY_REGISTRATIONS_TOTAL order by registration_date"
```

In the Snowflake web UI open `Data Products > Databases >
USER_REGISTRATION_DB > Analytics` to see the `DAILY_REGISTRATIONS` table and
the `DAILY_REGISTRATIONS_TOTAL` view.

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
`MONGO_HOST=mongodb` when running inside the Docker network. `MONGO_URI`
overrides the assembled connection string, and `MONGO_DATABASE` /
`MONGO_COLLECTION` select the source collection.

Run the configuration tests with:

```powershell
python -m unittest discover -s snowflake -p "test_*.py" -v
```

## Backend and frontend

`backend/` is an Express service that publishes registrations to Kafka and reads
daily counts from MongoDB. It serves the static page in `frontend/`.

Start it in Docker from the project root:

```powershell
docker compose up -d --build backend
```

Open http://localhost:3000. The container listens on port `3000` and reads the
project `.env`.

Alternatively run it directly with Node.js:

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
