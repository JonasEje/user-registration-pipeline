# user-registration-pipeline
Python, Docker, Spark, Kafka

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

The script loads credentials from `.env` and connects to the MongoDB container
through `localhost:27017`. Set `MONGO_HOST=mongodb` when running inside the
Docker network.
