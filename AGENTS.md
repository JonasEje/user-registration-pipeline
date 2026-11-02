# AGENTS.md

Instructions for OpenCode and any other AI coding agent working in this repository.
These rules apply to every file and every change unless the user explicitly overrides them.

## 0. Project context

The project ingests user registrations through Kafka, stores them in MongoDB, aggregates them with Apache Spark, and exports analytical data to Snowflake. The Node.js backend in `backend/` publishes registrations and serves the static dashboard in `frontend/`.

The Kafka producer, MongoDB consumer, Spark aggregation, Compose services, and Node backend are working. Do not rebuild or restart them without a specific reason. Extend them instead.

## 1. Rule zero: never write comments

This is a hard, non-negotiable rule.

- Do not add comments to Python, JavaScript, YAML, Dockerfiles, JSON, shell scripts, SQL, tests, or code samples in documentation.
- Do not add docstrings, module docstrings, section banners, or explanatory inline notes.
- Do not leave commented-out code, `TODO`, `FIXME`, `NOTE`, or `XXX` markers.
- Do not use comments to explain intent during a refactor. Rename variables, extract functions, introduce constants, or add types instead.
- Do not preserve old code as a comment. Delete it; Git keeps the history.
- Markdown prose is documentation, not a comment, and is allowed.
- The only permitted comments are directives the tooling requires, such as a shebang, Dockerfile `# syntax=`, `# noqa`, or `# type: ignore`. Keep them minimal and never use them as documentation.
- Do not mass-delete existing comments in untouched code. Remove comments only from the code you are already changing.
- Never use a comment to compensate for unclear code. Make the code self-explanatory.

If a change seems to require a comment, the design is unclear. Fix the design.

## 2. Repository map

- `producer/producer.py` publishes UUID-based registration events to Kafka.
- `consumer/consumer.py` consumes those events and idempotently upserts users into MongoDB.
- `spark/daily_registration_job.py` reads MongoDB with the Spark MongoDB connector, aggregates registrations per day, and writes the result to Snowflake.
- `backend/src/server.js` runs the Express API that publishes registrations to Kafka, reads daily counts from MongoDB, and serves `frontend/`.
- `frontend/` contains the static HTML, CSS, and JavaScript dashboard served by the backend.
- `snowflake/` contains the RSA key generator, the database setup SQL, the dashboard SQL, the Streamlit app and notebook under `snowflake/app/`, and the Python configuration tests.
- `compose.yaml` defines MongoDB, Kafka, the containerized Python app, the Snowflake exporter, the backend, and Kafka UI.
- `Dockerfile` builds the Python image with the producer, consumer, and Spark job; `requirements.txt` supports that image.
- `backend/Dockerfile` builds the backend image and serves `frontend/` from the same container.
- `README.Docker.md` is an unused Docker template artifact.
- `README.md` documents local setup, Snowflake configuration, the Spark job, and the backend and frontend.

## 3. Non-negotiable engineering rules

### 3.1 Secrets and configuration

- Never hardcode credentials, tokens, connection strings with passwords, or personal data.
- `.env` is gitignored. Never commit it, print it, log it, or echo its values.
- Read secrets and environment-specific values from environment variables. The existing names are `MONGO_ROOT_USERNAME`, `MONGO_ROOT_PASSWORD`, `MONGO_HOST`, `MONGO_PORT`, `MONGO_URI`, `MONGO_DATABASE`, `MONGO_COLLECTION`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, `PRODUCER_INTERVAL_SECONDS`, `PORT`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_PRIVATE_KEY_PATH`, `SNOWFLAKE_KEY_DIR`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_TABLE`, `SNOWFLAKE_REGISTRATIONS_TABLE`, and `SNOWFLAKE_ROLE`.
- When adding a new required variable, fail fast with an error that names the variable, and document it in `README.md`. Never include the missing value in the error.
- The consumer assembles the MongoDB URI from environment variables. Keep that behavior when touching the file.
- Never log full events or user documents in production code. Log identifiers and counts only.

### 3.2 Preserve user work

- Run `git status` and inspect `git diff` before editing.
- Never revert, overwrite, reformat, or delete changes you did not make.
- Do not rewrite unrelated files, rename directories, or reorganize the repository unless the task requires it.
- If existing uncommitted changes conflict with the task, stop and ask.

### 3.3 Scope and dependencies

- Implement the smallest coherent change that solves the problem.
- Do not add a dependency, framework, service, or build step without a concrete need and explicit justification.
- Do not invent files that the repository does not reference. The root `Dockerfile` builds the Python image and expects `requirements.txt`.
- Do not upgrade Python, Spark, Kafka, MongoDB, or pinned image versions as a side effect of an unrelated change.

## 4. Workflow

1. Read the relevant files, `README.md`, and this document before editing.
2. Inspect `git status`, `git diff`, and recent history to understand existing work.
3. State a short plan for multi-file changes.
4. Edit in small, focused steps. Keep the diff reviewable.
5. Verify with the commands in section 8.
6. Report exactly what changed, which commands ran, what passed or failed, and any remaining risk. Never claim a check passed if it was not run.

## 5. Python standards

- Target Python 3.13 or newer, matching the Docker images.
- Follow PEP 8: four-space indentation, snake_case for functions and variables, PascalCase for classes, UPPER_SNAKE_CASE for module constants.
- Add type hints to public functions and to any non-trivial internal function. Use built-in generics such as `list[str]` and `dict[str, object]`.
- Keep functions small and single-purpose. Prefer pure functions for transformation logic.
- Use descriptive names that make comments unnecessary. Avoid abbreviations and single-letter names outside short comprehensions.
- Use `pathlib` for filesystem paths and f-strings for formatting.
- Use context managers and `try`/`finally` for resources. Always close Kafka producers, consumers, MongoDB clients, and Spark sessions.
- Catch specific exception types only. Never use a bare `except`, never swallow an exception silently, and always preserve the original cause with `raise ... from error` when re-raising.
- Do not use mutable default arguments, wildcard imports, or module-level side effects outside executable entrypoints.
- Use `print` only in executable entrypoints. Use `logging` for reusable modules and background services.
- Validate external input before using it. Skip malformed Kafka messages explicitly rather than crashing the consumer loop.

## 6. Data contract

- Kafka topic: `user-registrations`.
- Registration event fields: `_id` (UUID string) and `registered_at` (UTC ISO 8601 string).
- Keep field names in snake_case and timestamps in UTC.
- Producer, consumer, and Spark job consume the same contract. Any schema change must update all three in one change.
- Preserve consumer idempotency. Upserts must use `$setOnInsert` with `_id` so redelivered messages cannot create duplicates.
- Snowflake target columns: `registration_date` (DATE) and `new_users` (BIGINT).
- Snowflake stores the raw feed in `REGISTRATIONS` with `user_id` (VARCHAR) and `registered_at` (TIMESTAMP_TZ), and the daily aggregate in `DAILY_REGISTRATIONS`.
- The Snowflake export replaces both target tables atomically through staging tables. Do not switch to an append-only write without handling duplicate days.
- Never overwrite Snowflake when the MongoDB source returns no registrations.
- The `REFRESH_DAILY_REGISTRATIONS_TASK` rebuilds `DAILY_REGISTRATIONS` from `REGISTRATIONS` every day at 00:10 UTC and must keep the empty-source guard.
- The `exporter` service runs the Spark export once a day at 00:00 UTC and must keep the empty-source guard.
- Do not change topic, database, collection, port, or service names without updating all code, `compose.yaml`, and `README.md`.

## 7. Spark and Docker standards

- Keep Spark transformations column-based. Do not call `collect()` or move unbounded data to the driver.
- Keep `local[*]` for local development only. Do not add cluster configuration unless the task asks for it.
- Stop the Spark session in a `finally` block.
- Keep the Snowflake connector version pinned and test package compatibility before upgrading Spark.
- Read Snowflake account, user, credentials, and identifiers only from environment variables.
- Keep the Snowflake write idempotent and atomic. Preserve the staging-table overwrite and explicit target schema unless the task requires different semantics.
- Refuse to write to Snowflake when the source DataFrame is empty.
- Pin image and runtime versions. Never introduce `latest`.
- Keep the non-root user in the root `Dockerfile` and do not add secrets to images.
- Keep published ports bound to `127.0.0.1` for local services.
- Validate Compose changes with `docker compose config` before starting services.
- Use `docker compose down` without `-v` unless the user explicitly asks to delete persisted data.

## 8. Testing and verification

Python tests live under `snowflake/` and use the standard library `unittest`. Node tests live under `backend/test/` and use the built-in `node --test` runner.

- Add Python tests under `snowflake/` and Node tests under `backend/test/` when adding non-trivial logic, a bug fix, or a data-contract change. Use `pytest` when it is available, otherwise use the standard library `unittest`.
- Unit tests must not require a live Kafka broker, MongoDB instance, Snowflake account, or network access. Extract pure logic or use fakes.
- Do not test framework internals or implementation details.
- Always run the fastest relevant checks before finishing:

```powershell
python -m compileall producer consumer spark snowflake
python -m unittest discover -s snowflake -p "test_*.py" -v
docker compose config
cd backend
npm.cmd test
```

- For integration changes, verify with `docker compose up -d` and targeted producer, consumer, or Spark runs. Report the exact commands and observed results.
- Do not commit code that fails syntax checks or a known test suite.
- If no test was added, explain why.

## 9. Documentation

- Update `README.md` when setup, configuration, commands, ports, or behavior changes.
- Keep commands PowerShell-compatible on Windows and accurate for the current repository.
- Prefer a short, correct document over speculative documentation.
- Do not replace `README.md` wholesale for a small change.
- Documentation code samples must also contain no comments.

## 10. Commits and final checks

- Make small, focused commits with imperative messages that explain why the change exists.
- Do not commit `.env`, virtual environments, `__pycache__`, compiled files, or IDE settings.
- Do not force-push, rewrite shared history, or amend commits authored by someone else.
- Before finishing, review the full diff and confirm no secrets, debug output, or unrelated formatting changes are included.
- If the user asks for a commit, append the agent attribution the repository already uses; otherwise do not commit without being asked.

## 11. Definition of done

- [ ] The requested behavior works.
- [ ] No comments, docstrings, or commented-out code were added.
- [ ] Names, types, and structure make the code self-explanatory.
- [ ] Secrets and environment-specific values come from environment variables.
- [ ] Errors are handled specifically and resources are cleaned up.
- [ ] Producer, consumer, and Spark contracts remain aligned.
- [ ] The Snowflake export remains idempotent and refuses to overwrite from an empty source.
- [ ] Tests were added or updated, or the absence of tests is justified.
- [ ] Verification commands were run and results reported.
- [ ] Documentation was updated if behavior or configuration changed.
- [ ] The diff is scoped and contains no unrelated changes.
