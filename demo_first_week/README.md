# The training week

This folder is the training week's project: a very small pipeline on one table, from the raw data to a dashboard.

```
raw.demo_accounts  --dlt-->  raw_dev.demo_accounts  --dbt-->  dev.demo_accounts_clean  -->  a dashboard
```

## What I built

* **The loader** (`ingestion.py`, dlt) copies the table `raw.demo_accounts` into `raw_dev.demo_accounts`. It uses `replace`: it empties the target and writes all 100 rows, so running it twice gives the same table.
* **The model** (`dbt_demo/models/demo_accounts_clean.sql`, dbt) builds `dev.demo_accounts_clean` from `raw_dev.demo_accounts`: the balance as money and a band (LOW, MEDIUM, HIGH). It is a table, rebuilt from scratch each run. `account_id` is tested: it must be unique and never empty.
* **Dagster** (`dagster_demo/definitions.py`) knows the loader and the model, in that order. Materialize all runs both.
* **The dashboard** (`dashboards/pages/index.md`, Evidence) shows `dev.demo_accounts_clean` as a table, with a title.
