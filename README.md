# user-registration-pipeline

A registration pipeline built from Kafka, MongoDB, Apache Spark, Snowflake, and
a small Node.js dashboard.

Every registration flows through Kafka into MongoDB during the day. At
00:00 UTC the Spark exporter writes the finished day to Snowflake, where a task
turns the raw rows into a daily list: the newest day first, then the day before,
and so on. A Streamlit app and a Snowflake notebook show that list.

## Services and ports

| Service | What it does | Port |
| --- | --- | --- |
| `mongodb` | stores every registration | `127.0.0.1:27017` |
| `kafka` | message broker for new registrations | `127.0.0.1:9094` |
| `kafka-ui` | browser UI for topics and consumer groups | http://localhost:8080 |
| `app` | runs the producer and the consumer | none |
| `exporter` | writes the finished day to Snowflake at 00:00 UTC | none |
| `backend` | API and static dashboard | http://localhost:3000 |

### Startup order

`docker compose up` starts the services in dependency order:

- `app` waits for `mongodb` and `kafka`
- `exporter` waits for `mongodb`
- `backend` waits for `mongodb` and `kafka`
- `kafka-ui` waits for `kafka`

`depends_on` only controls the order in which containers start. It does not
wait until MongoDB or Kafka accept connections, so a container can report `Up`
while its client is still retrying. Every client either retries or exits, and
`restart: unless-stopped` starts it again.

## Architecture

Registration path:

```
  frontend  --->  backend (:3000)  --->  Kafka  --->  consumer  --->  MongoDB
  producer  --------------------------->  Kafka
```

Analytics path:

```
  MongoDB  --->  exporter (Spark, 00:00 UTC)  --->  Snowflake
                                                        |
                                                        | 00:10 UTC
                                                        v
                                          Streamlit app / notebook
```

Inspection:

```
  kafka-ui (:8080)  --->  Kafka
```

| Component | Role |
| --- | --- |
| `frontend` | static dashboard; sends `POST /api/registrations` and reads the daily counts |
| `backend` | Express API on port 3000; publishes registrations to Kafka, aggregates MongoDB for the local dashboard, serves `frontend/` |
| `producer` | demo generator inside the `app` container; publishes one registration per `PRODUCER_INTERVAL_SECONDS` |
| `Kafka` | buffer and replay log for the topic `user-registrations` |
| `consumer` | validates each message and upserts it into MongoDB; runs next to the producer in `app` |
| `MongoDB` | system of record until the daily export runs |
| `exporter` | Spark job that reads the whole collection and writes `REGISTRATIONS` and `DAILY_REGISTRATIONS` at 00:00 UTC |
| `REFRESH_DAILY_REGISTRATIONS_TASK` | Snowflake task that rebuilds the daily list from `REGISTRATIONS` at 00:10 UTC |
| Streamlit app / notebook | read-only daily list inside Snowflake, newest day first |
| `kafka-ui` | inspection UI for topics, partitions, and consumer groups |

MongoDB holds every registration during the day. Snowflake only receives a
finished snapshot at midnight, so the numbers in the Snowflake dashboard are one
day behind the local dashboard by design.

## Before you start

| Tool | Windows | Linux | macOS |
| --- | --- | --- | --- |
| Docker Desktop / Docker Engine with Compose | install Docker Desktop | `sudo apt install docker.io docker-compose-v2` or follow docs.docker.com | install Docker Desktop, or `brew install colima docker docker-compose` |
| Git | `winget install Git.Git` | `sudo apt install git` | `xcode-select --install` |
| Python 3.13+ | `winget install Python.Python.3.13` | `sudo apt install python3 python3-pip`, use pyenv if the shipped version is older | `brew install python@3.13` |
| Node.js 22+ | `winget install OpenJS.NodeJS.LTS` | `sudo apt install nodejs npm`, use nodesource.com if the shipped version is older | `brew install node` |
| Snowflake CLI (optional) | `py -m pip install snowflake-cli` | `python3 -m pip install snowflake-cli` | `python3 -m pip install snowflake-cli` |
| OpenSSL (key pair, optional) | ships with Git for Windows | `sudo apt install openssl` | ships with macOS |

macOS uses the Linux commands in every step below. Only the tool installation
differs.

You need a Snowflake account with a warehouse you may use. The steps below use
`ACCOUNTADMIN`, warehouse `COMPUTE_WH`, database `USER_REGISTRATION_DB`, and
schema `ANALYTICS`.

## Step 1 - Get the code

Windows (PowerShell):

```powershell
git clone https://github.com/JonasEje/user-registration-pipeline.git
cd user-registration-pipeline
```

Linux (bash):

```bash
git clone https://github.com/JonasEje/user-registration-pipeline.git
cd user-registration-pipeline
```

## Step 2 - Create the Snowflake objects

Open Snowsight, go to **Projects > Worksheets**, paste the content of
`snowflake/setup.sql`, and run it. The script creates the warehouse, the
database, the schema, both tables, the view `DAILY_REGISTRATIONS_TOTAL`, the
refresh procedure, and the daily task.

The same script runs from the terminal once the Snowflake CLI is configured:

Windows (PowerShell):

```powershell
snow connection add
snow sql -f snowflake/setup.sql
```

Linux (bash):

```bash
snow connection add
snow sql -f snowflake/setup.sql
```

`snow connection add` asks for your account, user, and authentication method.
The CLI prints an encoding warning on some Windows terminals. It is harmless.

Check the result:

Windows (PowerShell):

```powershell
snow sql -q "select * from USER_REGISTRATION_DB.ANALYTICS.DAILY_REGISTRATIONS_TOTAL order by registration_date desc"
```

Linux (bash):

```bash
snow sql -q "select * from USER_REGISTRATION_DB.ANALYTICS.DAILY_REGISTRATIONS_TOTAL order by registration_date desc"
```

## Step 3 - Create the environment file

Create a file named `.env` in the project root and fill in your values. The file
is gitignored and must never be committed.

```dotenv
MONGO_ROOT_USERNAME=projectadmin
MONGO_ROOT_PASSWORD=LocalDev
SNOWFLAKE_PRIVATE_KEY_PATH=
KAFKA_TOPIC=user-registrations
MONGO_DATABASE=UserRegistrationDB
MONGO_COLLECTION=User
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_DATABASE=USER_REGISTRATION_DB
SNOWFLAKE_SCHEMA=ANALYTICS
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_TABLE=DAILY_REGISTRATIONS
SNOWFLAKE_ROLE=
```

Fill in `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_WAREHOUSE`, and
`SNOWFLAKE_ROLE`. Authentication uses either `SNOWFLAKE_PRIVATE_KEY_PATH`
(Step 4) or an added `SNOWFLAKE_PASSWORD` line.

`SNOWFLAKE_ACCOUNT` accepts the bare account identifier or a hostname ending in
`.snowflakecomputing.com`. Database, schema, warehouse, table, and role must be
simple identifiers and are normalized to uppercase.

If your Snowflake key is not in `~/.snowflake`, add the directory as well.

Windows (PowerShell):

```dotenv
SNOWFLAKE_KEY_DIR=C:/Users/your-name/.snowflake
```

Linux (bash):

```dotenv
SNOWFLAKE_KEY_DIR=/home/your-name/.snowflake
```

## Step 4 - Create the Snowflake key pair (optional)

Skip this step when you add `SNOWFLAKE_PASSWORD` to `.env`.

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -File .\snowflake\generate_rsa_key.ps1
```

Linux (bash):

```bash
bash snowflake/generate_rsa_key.sh
```

Register the printed public key body in Snowflake:

```sql
ALTER USER your_user SET RSA_PUBLIC_KEY='paste-the-public-key-body-here';
```

Then remove `SNOWFLAKE_PASSWORD` from `.env` and add the private key path.

Windows (PowerShell):

```dotenv
SNOWFLAKE_PRIVATE_KEY_PATH=C:/Users/your-name/.snowflake/rsa_key.p8
```

Linux (bash):

```dotenv
SNOWFLAKE_PRIVATE_KEY_PATH=/home/your-name/.snowflake/rsa_key.p8
```

Inside Docker the key is mounted read-only at `/keys/rsa_key.p8`, and compose
sets that path automatically.

## Step 5 - Start the stack

Windows (PowerShell):

```powershell
docker compose up -d --build
```

Linux (bash):

```bash
docker compose up -d --build
```

The first build downloads the Python and Node dependencies and takes a few
minutes.

## Step 6 - Check that everything runs

Windows (PowerShell):

```powershell
docker compose ps
curl.exe http://localhost:3000/api/registrations/stats
docker compose logs -f exporter
```

Linux (bash):

```bash
docker compose ps
curl http://localhost:3000/api/registrations/stats
docker compose logs -f exporter
```

Expected result: six containers report `Up`, the API answers with
`{"days":[]}` or a list of days, and the exporter logs a line such as
`Next Snowflake export at 2026-11-02T00:00:00+00:00`. The log command keeps
following the container, so press `Ctrl+C` to return to the prompt.

Open the dashboard at http://localhost:3000 and Kafka UI at
http://localhost:8080.

## Step 7 - Understand when data reaches Snowflake

New registrations are stored in MongoDB immediately and are visible in the local
dashboard. Snowflake receives them only once per day:

1. 00:00 UTC - `exporter` writes every registration to `REGISTRATIONS` and the
   daily counts to `DAILY_REGISTRATIONS`.
2. 00:10 UTC - `REFRESH_DAILY_REGISTRATIONS_TASK` rebuilds the daily list from
   the raw table as a safety net.

00:00 UTC is 01:00 or 02:00 in Germany, depending on daylight saving time.

Run the export immediately when you do not want to wait:

Windows (PowerShell):

```powershell
docker compose exec exporter python spark/daily_registration_job.py
```

Linux (bash):

```bash
docker compose exec exporter python spark/daily_registration_job.py
```

The job refuses to write when MongoDB is empty, so an empty source never erases
existing Snowflake data.

Check the task state and its last runs:

Windows (PowerShell):

```powershell
snow sql -q "show tasks in schema USER_REGISTRATION_DB.ANALYTICS"
snow sql -q "select name, state, scheduled_time, completed_time from table(USER_REGISTRATION_DB.INFORMATION_SCHEMA.TASK_HISTORY()) where name = 'REFRESH_DAILY_REGISTRATIONS_TASK' order by scheduled_time desc limit 5"
```

Linux (bash):

```bash
snow sql -q "show tasks in schema USER_REGISTRATION_DB.ANALYTICS"
snow sql -q "select name, state, scheduled_time, completed_time from table(USER_REGISTRATION_DB.INFORMATION_SCHEMA.TASK_HISTORY()) where name = 'REFRESH_DAILY_REGISTRATIONS_TASK' order by scheduled_time desc limit 5"
```

## Step 8 - Deploy the Snowflake dashboard (optional)

The Streamlit app `USER_REGISTRATION_DB.ANALYTICS.REGISTRATION_DASHBOARD` shows
the daily list with the newest day on top. The notebook
`USER_REGISTRATION_DB.ANALYTICS.REGISTRATION_TABLE` shows the same data.

Run these commands from the project root. They are identical on both systems.

Windows (PowerShell):

```powershell
snow sql -q "PUT file://snowflake/app/streamlit_app.py @USER_REGISTRATION_DB.ANALYTICS.STREAMLIT_STAGE OVERWRITE = TRUE AUTO_COMPRESS = FALSE"
snow sql -q "PUT file://snowflake/app/registration_table.ipynb @USER_REGISTRATION_DB.ANALYTICS.STREAMLIT_STAGE OVERWRITE = TRUE AUTO_COMPRESS = FALSE"
snow sql -f snowflake/dashboard.sql
snow notebook execute USER_REGISTRATION_DB.ANALYTICS.REGISTRATION_TABLE --host your-host.eu-central-2.aws.snowflakecomputing.com
```

Linux (bash):

```bash
snow sql -q "PUT file://snowflake/app/streamlit_app.py @USER_REGISTRATION_DB.ANALYTICS.STREAMLIT_STAGE OVERWRITE = TRUE AUTO_COMPRESS = FALSE"
snow sql -q "PUT file://snowflake/app/registration_table.ipynb @USER_REGISTRATION_DB.ANALYTICS.STREAMLIT_STAGE OVERWRITE = TRUE AUTO_COMPRESS = FALSE"
snow sql -f snowflake/dashboard.sql
snow notebook execute USER_REGISTRATION_DB.ANALYTICS.REGISTRATION_TABLE --host your-host.eu-central-2.aws.snowflakecomputing.com
```

Open both objects in Snowsight under **Projects > Streamlit** and
**Projects > Notebooks**.

## Step 9 - Run the tests

Windows (PowerShell):

```powershell
python -m compileall producer consumer spark snowflake
python -m unittest discover -s snowflake -p "test_*.py" -v
docker compose config
cd backend
npm.cmd test
```

Linux (bash):

```bash
python3 -m compileall producer consumer spark snowflake
python3 -m unittest discover -s snowflake -p "test_*.py" -v
docker compose config
cd backend
npm test
```

Expected result: `compileall` prints nothing, the Python suite reports
`OK`, and the Node suite reports `pass 12`.

## API

The backend serves the static files in `frontend/` and three JSON endpoints.
There is no authentication, so anyone who can reach port 3000 can read the
counts and create registrations.

| Method | Path | Purpose | Response |
| --- | --- | --- | --- |
| `GET` | `/api/health` | liveness check for the HTTP server | `{"status":"ok"}` |
| `GET` | `/api/registrations/stats` | daily counts from MongoDB | `{"days":[{"registration_date":"2026-10-31","new_users":2}]}` |
| `POST` | `/api/registrations` | publishes one new registration to Kafka | `201` with `{"_id":"0f6d4d3e-...","registered_at":"2026-11-01T15:23:40.272123+00:00"}` |

`/api/registrations/stats` sorts by date ascending, so the local dashboard shows
the oldest day first. The Snowflake dashboard sorts descending and shows the
newest day first.

Unknown `/api` paths return `404` with `{"error":"Not found"}`. Unhandled
failures return `500` with `{"error":"Request failed"}`.

Windows (PowerShell):

```powershell
curl.exe http://localhost:3000/api/health
curl.exe http://localhost:3000/api/registrations/stats
curl.exe -X POST http://localhost:3000/api/registrations
```

Linux (bash) and macOS:

```bash
curl http://localhost:3000/api/health
curl http://localhost:3000/api/registrations/stats
curl -X POST http://localhost:3000/api/registrations
```

## Data contract

The Kafka message and the MongoDB document use the same two fields:

```json
{
  "_id": "0f6d4d3e-6f4a-4f6e-9f0a-2c1b7a5d8e11",
  "registered_at": "2026-11-01T15:23:40.272123+00:00"
}
```

`_id` is a UUID string, not an ObjectId. `registered_at` is a UTC ISO 8601
timestamp. The consumer stores exactly these fields and keeps the first version
of a document, so a redelivered message cannot change the timestamp.

Snowflake objects:

| Object | Column | Type | Meaning |
| --- | --- | --- | --- |
| `REGISTRATIONS` | `USER_ID` | `VARCHAR` | registration UUID |
| `REGISTRATIONS` | `REGISTERED_AT` | `TIMESTAMP_TZ` | registration time in UTC |
| `DAILY_REGISTRATIONS` | `REGISTRATION_DATE` | `DATE` | UTC day |
| `DAILY_REGISTRATIONS` | `NEW_USERS` | `BIGINT` | registrations on that day |
| `DAILY_REGISTRATIONS_TOTAL` | `TOTAL_USERS` | `BIGINT` | running total up to that day |

`REGISTRATIONS` and `DAILY_REGISTRATIONS` hold the full MongoDB snapshot after
each export, not just the last day. `DAILY_REGISTRATIONS_TOTAL` is a view on top
of `DAILY_REGISTRATIONS`.

Example rows in `DAILY_REGISTRATIONS_TOTAL`:

| REGISTRATION_DATE | NEW_USERS | TOTAL_USERS |
| --- | --- | --- |
| 2026-10-31 | 2 | 2 |
| 2026-11-01 | 4 | 6 |
| 2026-11-02 | 7 | 13 |

## Data flow and failure handling

| Situation | What happens |
| --- | --- |
| Kafka is unreachable while the producer sends | the Kafka client retries and blocks the send; if Kafka stays down the send raises, the process exits, and Docker restarts `app` |
| Kafka is unreachable while the consumer polls | the client retries its fetches, the messages stay in the topic, and consumption resumes when Kafka returns |
| Kafka is unreachable when the backend registers | `POST /api/registrations` returns `500` with `{"error":"Request failed"}`; nothing is stored |
| MongoDB is unreachable while consuming | the upsert raises, the consumer exits, and `app` restarts; it resumes from the last committed offset |
| A message does not match the contract | the consumer logs `Skipped malformed registration` and moves to the next offset |
| The consumer crashes | offsets commit automatically every 5 seconds, so up to about 5 seconds of messages are reprocessed or lost |
| The same message is delivered twice | the upsert uses `$setOnInsert` with `_id`, so the second write changes nothing |
| The Spark export fails at 00:00 UTC | the scheduler logs `Snowflake export failed; retrying tomorrow`, Snowflake keeps the previous snapshot, and the next automatic attempt is the following midnight |
| MongoDB is empty at export time | the job stops with `No registrations found in MongoDB; refusing to overwrite Snowflake` and leaves Snowflake untouched |
| Snowflake is unreachable during the export | the connector fails, the job logs the exception, and the previous Snowflake snapshot stays in place |
| The exporter container is stopped at 00:00 UTC | that day is not exported until someone runs the job by hand |

Consumer offsets start at `latest` when the group has no committed offset, so a
fresh consumer group ignores messages that were published before it started. On
restarts the group resumes from its last commit.

The export replaces both Snowflake tables through staging tables in one run
(`truncate_table` and `usestagingtable` are enabled), so readers see either the
old snapshot or the new one, never a half-written day.

## Monitoring and health

Container state:

```bash
docker compose ps
```

Health endpoint, identical on every system:

```bash
curl http://localhost:3000/api/health
```

Consumer lag for the group (identical on every system):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group user-registration-consumer
```

A non-zero `LAG` means Kafka holds messages the consumer has not stored yet.
Kafka UI at http://localhost:8080 shows the same numbers with graphs.

Exporter results appear in its logs:

Windows (PowerShell):

```powershell
docker compose logs --tail 50 exporter | Select-String "export|written"
```

Linux (bash) and macOS:

```bash
docker compose logs --tail 50 exporter | grep -i -E "export|written"
```

Look for `Next Snowflake export at`, `Registrations from MongoDB: N`,
`Registrations and daily counts written to Snowflake.`, or
`Snowflake export failed`. A successful run prints `Registrations from MongoDB:
N` and `Registrations and daily counts written to Snowflake.`

Row counts and the newest exported day:

```sql
SELECT COUNT(*) AS REGISTRATIONS FROM USER_REGISTRATION_DB.ANALYTICS.REGISTRATIONS;
SELECT MAX(REGISTRATION_DATE) AS LAST_DAY,
       SUM(NEW_USERS) AS TOTAL_USERS
FROM USER_REGISTRATION_DB.ANALYTICS.DAILY_REGISTRATIONS;
```

The exporter keeps no audit table. The logs above are the run record, and
`MAX(REGISTERED_AT)` in `REGISTRATIONS` shows the newest data that reached
Snowflake, not the time of the export. The 00:10 rebuild has its own history:

```sql
SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME
FROM TABLE(USER_REGISTRATION_DB.INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'REFRESH_DAILY_REGISTRATIONS_TASK'
ORDER BY SCHEDULED_TIME DESC
LIMIT 5;
```

Container logs live only as long as the container. When you recreate the
container with `docker compose up`, the history of earlier runs is gone unless
your Docker logging driver ships it elsewhere.

## Security and production notes

This repository is a local development setup. Treat every default in it as
local-only.

### The `LocalDev` password

The `.env` example uses `MONGO_ROOT_PASSWORD=LocalDev`. It exists so the stack
starts without editing, and it is not a production password. Change it before
you expose MongoDB beyond `127.0.0.1`, and never reuse it for anything else.

### Snowflake roles

`snowflake/setup.sql` starts with `USE ROLE ACCOUNTADMIN` because creating a
warehouse, a database, and a task needs account-level privileges. Keep that
role for setup only.

For the exporter, create a dedicated role and grant it only what the job uses:
`USAGE` on the warehouse, database, and schema, plus write access to
`REGISTRATIONS` and `DAILY_REGISTRATIONS` and `CREATE TABLE` / `CREATE STAGE`
on the schema for the staging tables. Put that role in `SNOWFLAKE_ROLE`.
`ACCOUNTADMIN` can read and change every object in the account and bypasses
most guards, so an application account should never hold it.

### Rotating the Snowflake key pair

Snowflake allows two active public keys, which makes rotation possible without
downtime.

1. Generate the new pair in a separate directory.

   Windows (PowerShell):

   ```powershell
   powershell -ExecutionPolicy Bypass -File .\snowflake\generate_rsa_key.ps1 -KeyDirectory "$env:USERPROFILE\.snowflake\rotation" -KeyName rsa_key
   ```

   Linux (bash) and macOS:

   ```bash
   bash snowflake/generate_rsa_key.sh ~/.snowflake/rotation rsa_key
   ```

2. Register the new public key as the second key:

   ```sql
   ALTER USER your_user SET RSA_PUBLIC_KEY_2='paste-the-new-public-key';
   ```

3. Replace the private key that the container reads. Compose mounts the
   directory from `SNOWFLAKE_KEY_DIR` at `/keys` and always reads
   `/keys/rsa_key.p8`, so the file name has to stay the same.

   Windows (PowerShell):

   ```powershell
   Copy-Item "$env:USERPROFILE\.snowflake\rotation\rsa_key.p8" "$env:USERPROFILE\.snowflake\rsa_key.p8" -Force
   ```

   Linux (bash) and macOS:

   ```bash
   cp ~/.snowflake/rotation/rsa_key.p8 ~/.snowflake/rsa_key.p8
   ```

4. Run one export by hand and confirm that it succeeds:

   ```bash
   docker compose exec exporter python spark/daily_registration_job.py
   ```

5. Promote the new key and remove the second slot:

   ```sql
   ALTER USER your_user SET RSA_PUBLIC_KEY='paste-the-new-public-key';
   ALTER USER your_user UNSET RSA_PUBLIC_KEY_2;
   ```

6. Delete the rotation directory and the old key copies.

For password authentication, rotate `SNOWFLAKE_PASSWORD` with
`ALTER USER your_user SET PASSWORD='...';` and update `.env` in the same step.

### Handling secrets in production

- Keep `.env` out of Git. It is already listed in `.gitignore`.
- Inject secrets from your platform's secret store at deploy time instead of
  shipping a file with the image.
- Never bake passwords or private keys into an image or a compose file.
- Restrict who can read the private key file, and mount it read-only as compose
  does with `/keys`.
- Rotate every credential that ever appeared in a log, a screenshot, or a commit.

### Hardening MongoDB and Kafka

- Both services publish their ports on `127.0.0.1` only. Keep it that way, or
  remove the `ports` entries and reach them through the Docker network.
- MongoDB already requires a root user. For production, create a dedicated
  database user with `readWrite` on `UserRegistrationDB` instead of using root.
- Kafka listens with `PLAINTEXT` and has no authentication. For production,
  enable SASL and TLS listeners and update `KAFKA_BOOTSTRAP_SERVERS` with the
  matching security settings.
- Back up the MongoDB volume and the Snowflake tables before destructive
  changes. `docker compose down -v` deletes registrations permanently.

### Dashboard and API access

The Express API and the static dashboard have no login. The Streamlit app and
the notebook run inside Snowflake and inherit your Snowflake session. If you
expose port 3000 beyond your machine, put an authenticated reverse proxy in
front of it.

## Known limitations

- Snowflake is updated once a day at 00:00 UTC. The local dashboard is the only
  near-real-time view.
- A missed 00:00 export is not retried automatically. Run
  `python spark/daily_registration_job.py` inside the `exporter` container.
- Every export rewrites the full snapshot. This is fine for small data sets and
  becomes slow when the collection grows.
- The consumer uses automatic offset commits, so it offers at-least-once
  delivery with a small window of loss or reprocessing.
- There is no dead-letter queue. Malformed messages are logged and skipped.
- The API, the dashboard, Kafka, and MongoDB have no authentication beyond the
  local bindings and MongoDB's root login.
- Kafka and MongoDB run as single nodes without replication or backups.
- The producer is a demo generator with a fixed interval, not a signup form.
- Spark runs with `local[*]` on one machine, not on a cluster.
- The Streamlit app and the notebook need a Snowflake login in Snowsight.
- Running Spark on the Windows host needs `winutils.exe`; the Docker image does
  not.

## Updating the project

Windows (PowerShell) and Linux (bash) use the same commands:

```bash
git pull
docker compose up -d --build
```

`docker compose up` recreates only the containers whose image or configuration
changed. After a change to `snowflake/setup.sql`, run it again with
`snow sql -f snowflake/setup.sql`. After a change to the Streamlit app or the
notebook, repeat Step 8.

## Run parts without Docker

Install the Python dependencies once.

Windows (PowerShell):

```powershell
py -m pip install -r requirements.txt
```

Linux (bash):

```bash
python3 -m pip install -r requirements.txt
```

The backend needs Node.js 22 or newer and a reachable Kafka and MongoDB.

Windows (PowerShell):

```powershell
cd backend
npm.cmd install
npm.cmd start
```

Linux (bash):

```bash
cd backend
npm install
npm start
```

The Spark job needs Python 3.13 or newer and access to Maven Central and
Snowflake on the first run.

Windows (PowerShell):

```powershell
python spark/daily_registration_job.py
```

Linux (bash):

```bash
python3 spark/daily_registration_job.py
```

Both read `.env` from the project root. Set `MONGO_HOST=mongodb` when running
inside the Docker network and `localhost` when running on the host. `MONGO_URI`
overrides the assembled connection string.

## Spark on Windows

Spark needs Hadoop's `winutils.exe` on Windows. Without it the job stops with
`HADOOP_HOME and hadoop.home.dir are unset`.

1. Download `winutils.exe` and `hadoop.dll` from
   [kontext-tech/winutils](https://github.com/kontext-tech/winutils/tree/master/hadoop-3.4.0-win10-x64/bin)
   into `%USERPROFILE%\hadoop\bin`.
2. Set `HADOOP_HOME=%USERPROFILE%\hadoop` and add `%HADOOP_HOME%\bin` to `PATH`.
3. Restart the terminal so the new variables are picked up.

`spark/daily_registration_job.py` also detects `%USERPROFILE%\hadoop\bin\winutils.exe`
on its own, so step 2 can be skipped for that script.

## Configuration reference

| Variable | Default | Required | Purpose |
| --- | --- | --- | --- |
| `MONGO_ROOT_USERNAME` | none | yes | MongoDB admin user created on the first start and used by every client |
| `MONGO_ROOT_PASSWORD` | none | yes | MongoDB admin password; `LocalDev` is a local placeholder only |
| `MONGO_HOST` | `localhost` | no | MongoDB host; compose sets `mongodb` |
| `MONGO_PORT` | `27017` | no | MongoDB port |
| `MONGO_URI` | assembled from the values above | no | full connection string; overrides host, port, user, and password |
| `MONGO_DATABASE` | `UserRegistrationDB` | no | database that holds the registrations |
| `MONGO_COLLECTION` | `User` | no | collection that holds the registrations |
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9094` | no | comma-separated broker list; compose sets `kafka:9092` |
| `KAFKA_TOPIC` | `user-registrations` | no | topic for registration events |
| `KAFKA_GROUP_ID` | `user-registration-consumer` | no | consumer group that tracks committed offsets |
| `PRODUCER_INTERVAL_SECONDS` | `5000` | no | seconds between demo registrations; the default is about 83 minutes |
| `PORT` | `3000` | no | host port for the backend; the container always listens on 3000 |
| `SNOWFLAKE_ACCOUNT` | none | yes | account identifier or full `.snowflakecomputing.com` hostname |
| `SNOWFLAKE_USER` | none | yes | Snowflake login for the exporter |
| `SNOWFLAKE_PASSWORD` | none | yes or key pair | password authentication for the exporter |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | none | yes or password | path to an unencrypted PKCS#8 private key |
| `SNOWFLAKE_KEY_DIR` | `~/.snowflake` | no | host directory mounted read-only at `/keys` |
| `SNOWFLAKE_DATABASE` | none | yes | target database, normalized to uppercase |
| `SNOWFLAKE_SCHEMA` | none | yes | target schema, normalized to uppercase |
| `SNOWFLAKE_WAREHOUSE` | none | yes | warehouse for the export and the refresh task |
| `SNOWFLAKE_TABLE` | none | yes | daily aggregate table, normally `DAILY_REGISTRATIONS` |
| `SNOWFLAKE_REGISTRATIONS_TABLE` | `REGISTRATIONS` | no | raw registration table |
| `SNOWFLAKE_ROLE` | none | no | role used by the exporter; the user's default role applies when unset |

The Snowflake role needs `USAGE` on the database, schema, and warehouse, plus
`CREATE TABLE` and `CREATE STAGE` on the schema. The Spark job uses connector
`net.snowflake:spark-snowflake_2.13:3.2.3-spark_4.1` and downloads it on the
first run.

## Troubleshooting

| Problem | Fix |
| --- | --- |
| `service "app" is not running` | `docker compose up -d` |
| Port `3000`, `8080`, `27017`, or `9094` already in use | stop the other program or change the published port in `compose.yaml` |
| `HADOOP_HOME and hadoop.home.dir are unset` | install `winutils.exe`, see the Windows section |
| `No registrations found in MongoDB` | MongoDB is empty, so Snowflake stays untouched. This is intended |
| Snowflake login or role errors | check `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, and the role grants |
| The Snowflake list is empty | the exporter only writes at 00:00 UTC. Run it by hand or wait |
| `NoBrokersAvailable` or `KafkaTimeoutError` in `docker compose logs app` | Kafka is still starting or unreachable; wait for the restart and check `docker compose logs kafka` |
| MongoDB connection errors in the consumer log | MongoDB is not ready; check `docker compose ps mongodb`; the consumer exits and `app` restarts |
| `POST /api/registrations` returns 500 | Kafka is unreachable from the backend; check `docker compose logs kafka backend` |
| Consumer lag keeps growing | check `docker compose logs app`; the consumer may be stuck on MongoDB |
| Exporter logs `Snowflake export failed` | fix the reported cause and run the export by hand; Snowflake keeps the last snapshot |
| A day is missing in Snowflake | the exporter was not running at 00:00 UTC; run the manual export |
| The dashboard shows a login page | sign in to Snowsight in the same browser |

## Stop and reset

Stop the containers but keep the data.

Windows (PowerShell):

```powershell
docker compose down
```

Linux (bash):

```bash
docker compose down
```

Delete the data as well. This removes the MongoDB volume, so every registration
is gone.

Windows (PowerShell):

```powershell
docker compose down -v
```

Linux (bash):

```bash
docker compose down -v
```
