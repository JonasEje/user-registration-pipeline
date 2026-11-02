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

## Before you start

| Tool | Windows | Linux |
| --- | --- | --- |
| Docker Desktop / Docker Engine with Compose | install Docker Desktop | `sudo apt install docker.io docker-compose-v2` or follow docs.docker.com |
| Git | `winget install Git.Git` | `sudo apt install git` |
| Python 3.13+ | `winget install Python.Python.3.13` | `sudo apt install python3 python3-pip`, use pyenv if the shipped version is older |
| Node.js 22+ | `winget install OpenJS.NodeJS.LTS` | `sudo apt install nodejs npm`, use nodesource.com if the shipped version is older |
| Snowflake CLI (optional) | `py -m pip install snowflake-cli` | `python3 -m pip install snowflake-cli` |
| OpenSSL (key pair, optional) | ships with Git for Windows | `sudo apt install openssl` |

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

| Variable | Default | Required |
| --- | --- | --- |
| `MONGO_ROOT_USERNAME` | none | yes |
| `MONGO_ROOT_PASSWORD` | none | yes |
| `MONGO_HOST` | `localhost` | no |
| `MONGO_PORT` | `27017` | no |
| `MONGO_URI` | assembled from the values above | no |
| `MONGO_DATABASE` | `UserRegistrationDB` | no |
| `MONGO_COLLECTION` | `User` | no |
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:9094` | no |
| `KAFKA_TOPIC` | `user-registrations` | no |
| `KAFKA_GROUP_ID` | `user-registration-consumer` | no |
| `PRODUCER_INTERVAL_SECONDS` | `5000` | no |
| `PORT` | `3000` | no |
| `SNOWFLAKE_ACCOUNT` | none | yes |
| `SNOWFLAKE_USER` | none | yes |
| `SNOWFLAKE_PASSWORD` | none | yes or key pair |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | none | yes or password |
| `SNOWFLAKE_KEY_DIR` | `~/.snowflake` | no |
| `SNOWFLAKE_DATABASE` | none | yes |
| `SNOWFLAKE_SCHEMA` | none | yes |
| `SNOWFLAKE_WAREHOUSE` | none | yes |
| `SNOWFLAKE_TABLE` | none | yes |
| `SNOWFLAKE_REGISTRATIONS_TABLE` | `REGISTRATIONS` | no |
| `SNOWFLAKE_ROLE` | none | no |

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
