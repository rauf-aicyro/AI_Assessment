# Senior AI / Data Engineer — Take-Home Challenge

Welcome, and thanks for taking the time. This challenge mirrors the kind of work you'd do on
the team: adding a new vendor ingestion pipeline (Airflow + dbt) and the Terraform to support
it. It is a trimmed-down replica of our real platform.

## What's here

```
challenge/
├── airflow/               # Astro project (`astro dev start` -> local Airflow 2.10.5 + UI)
│   ├── Dockerfile, requirements.txt, airflow_settings.yaml, docker-compose.override.yml
│   └── dags/              # Airflow (MWAA) pipelines + shared utils + tests
│       ├── utils/         # shared helpers (DuckDB silver loader, file source)
│       ├── globex_files_to_silver/  # REFERENCE example pipeline — study it, don't change it
│       └── tests/airflow/ # DAG validation tests + a DuckDB example test to copy
├── dbt/duckdb/            # dbt project (sources + gold models)
└── terraform/             # scaffolded multi-env MWAA; you add the source S3 bucket (Part C)
```

> In production, the Airflow/dbt code and the Terraform live in **two separate repos**. We've
> combined them here for convenience.

## Your task

**[TASK.md](./TASK.md)** is the full brief — *what* to build and how it's judged. This README only
covers *how to run and verify* the repo. In short: build the **GitHub** vendor pipeline (a
paginated REST API source) end to end — Airflow DAG, dbt gold models, and the Terraform to
provision it across dev and prod.

Conventions you should follow are in **[AGENTS.md](./AGENTS.md)**.

## Ground rules

- **Time box**: ~3–4 hours. We're not looking for perfection — we're looking for production
thinking, sound trade-offs, and clean, reviewable work.
- **Using AI tools** (Cursor, Claude Code, Copilot, etc.) is **encouraged**. We care about how
you use them and how you verify their output. Be ready to walk us through your choices.
- **Open a Pull Request**: create a branch (e.g. `feature/<your-name>-github-pipeline`) and open
a PR against `main`. Put your reasoning, trade-offs, and any "what I'd do with more time" in
the PR description. **CI must be green** (`black`, the DAG-validation tests, and `sqlfluff`).
- You do **not** need live AWS / cloud credentials. The Airflow DAG and dbt models are
**testable locally** (Airflow UI via Astro CLI, DAG import tests + `unittest.mock`-ed unit tests +
`dbt run/test` on DuckDB). Terraform (Part C) needs no AWS account either — it's a design exercise
checked with `terraform fmt` (no `apply`).

## Getting started / testing locally

There is no S3 and no cloud: ingestion lands records straight into a local **DuckDB** warehouse
(an embedded engine — no server), and dbt builds `gold` from `silver` in that same database.

### Full Airflow UI via the Astronomer CLI

The whole Airflow stack (webserver UI, scheduler, triggerer, metadata DB) runs in Docker with one
command, on Astro Runtime 12.12.0 (Airflow 2.10.5; prod MWAA runs 2.10.3 — same 2.10 line).
**Prerequisites: Docker Desktop +
[Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)** (`brew install astro`).

```bash
cd airflow
astro dev start                 # builds the image + boots Airflow; UI at http://localhost:8080
                                # (login: admin / admin). The github_api connection + dbt
                                # Variables are pre-loaded from airflow_settings.yaml.

# Unpause + trigger globex_files_to_silver_dev and github_api_to_silver_dev in the UI — both go
# green: ingestion writes `silver` into DuckDB, then the dbt tasks build + test `gold` from it.
# (globex is the reference DAG and is there from the start; github_api_to_silver_dev only shows up
#  once you build it in Part A.)
astro dev stop
```

The `github_api` connection is auto-loaded from `airflow_settings.yaml`. To add it by hand instead
(e.g. on a fresh metadata DB), go to **Admin → Connections → + (Add a new record)** in the UI and set:


| Field                     | Value                                                      |
| ------------------------- | ---------------------------------------------------------- |
| **Connection Id**         | `github_api`                                               |
| **Connection Type**       | `HTTP`                                                     |
| **Host**                  | `https://api.github.com`                                   |
| **Password** *(optional)* | a GitHub personal access token, for higher API rate limits |


Leave the remaining fields blank and **Save**. CLI equivalent:

```bash
astro dev run airflow connections add github_api \
  --conn-type http --conn-host https://api.github.com
```

> Need a token? See GitHub's
> [Managing your personal access tokens](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens)
> (a fine-grained or classic token with **public read** is enough — no scopes needed for public repos).

How it fits together locally:

- The DuckDB warehouse file lives on a host bind mount (`include/duckdb/`), so the `silver` +
`gold` tables persist across restarts and stay writable by the non-root astro user.
- The Globex file drops are read from `include/lake_seed/`; GitHub is pulled live from the
public API.
- dbt is in the same image, the project is mounted at `/usr/local/airflow/dbt`, and the DAGs'
dbt tasks use `DBT_TARGET=duckdb`, building `gold` from the `silver` the ingestion just wrote.

### DAG / unit tests (no Docker)

Fast checks without booting the stack — `**pytest**` for DAG validation + your unit tests:

**Prerequisite:** **Python 3.10+** (CI runs 3.10 and `AGENTS.md` targets 3.10+; 3.10/3.11
recommended). `venv` ships with Python on macOS/Windows (on Debian/Ubuntu install `python3-venv`).
If your system Python is older, use `[pyenv](https://github.com/pyenv/pyenv)`
(`brew install pyenv`) to get one: `pyenv install 3.10.13 && pyenv local 3.10.13`.

```bash
python --version                                      # expect 3.10.x or 3.11.x
python -m venv .venv && source .venv/bin/activate
pip install -r airflow/dags/requirements.txt          # Airflow + utils + dbt-duckdb

cd airflow/dags && python -m pytest tests/airflow -q   # DAG validation + your unit tests
#   (mock the HTTP layer with unittest.mock; see tests/airflow/test_duckdb_operations_example.py)
```

The **dbt** models build `gold` from the `silver` your ingestion DAG writes — there's no separate
fixture. The Astro run already builds + tests `gold` (the DAG's `dbt_run`/`dbt_test` tasks). To
iterate on the models on their own, point dbt at the warehouse the DAG produced (stop Astro first —
DuckDB is single-writer):

```bash
cd dbt/duckdb
# Replace <PATH_TO_YOUR_CLONE> with the absolute path to where YOU cloned this repo.
export DBT_DUCKDB_PATH="<PATH_TO_YOUR_CLONE>/airflow/include/duckdb/local.duckdb"
dbt run -m gold.globex.globex_orders -t duckdb     # build gold
dbt test -m gold.globex.globex_orders -t duckdb     # test gold
```

### Inspecting the DuckDB warehouse (optional)

Handy if you want to eyeball what landed in `silver`/`gold`. Install the DuckDB CLI and open the
warehouse file directly (stop Astro first — DuckDB is single-writer):

```bash
brew install duckdb     # macOS (or see https://duckdb.org/docs/installation/)

# Replace <PATH_TO_YOUR_CLONE> with the absolute path to where YOU cloned this repo.
duckdb "<PATH_TO_YOUR_CLONE>/airflow/include/duckdb/local.duckdb"

-- then, at the duckdb> prompt:
SHOW ALL TABLES;
SELECT * FROM silver.globex_orders LIMIT 5;
SELECT * FROM silver.globex_order_lines LIMIT 5;
```

### Terraform (Part C)

Install Terraform ≥ 1.5.0 from the
[official downloads](https://developer.hashicorp.com/terraform/install) or a package manager:

```bash
terraform version                                                # confirm >= 1.5.0
```

Part C is a **design exercise** — no AWS account or credentials, and you do **not** `apply`. The
required check is just formatting, which is fast and what CI runs:

```bash
cd terraform
terraform fmt -recursive      # (or `-check` to verify only)
```

### What's actually runnable locally

- **Both DAGs run green end-to-end** under `astro dev start`: ingestion writes `silver` into
DuckDB, GitHub is pulled live, and the DAG's dbt tasks build + test `gold` from that same `silver`.
- **Offline (no Docker):** `pytest` for DAG validation + your unit tests. The dbt models build from
the `silver` the DAG produces, so to run/test them locally point `DBT_DUCKDB_PATH` at the
warehouse file the DAG wrote (`airflow/include/duckdb/local.duckdb`).
- **CI** runs `black` + the DAG validation tests + `sqlfluff` (SQL lint) + `terraform fmt`.

(Terraform Part C is a design exercise checked with `terraform fmt` — no AWS account; see TASK.md.)

Good luck — we're looking forward to seeing how you approach it.
