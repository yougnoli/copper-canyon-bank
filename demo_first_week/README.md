# The training week

This folder is the training week's project: a very small pipeline on one table, from the raw data to a dashboard.
You build it step by step with Ray. Everything here is yours to read and change. The bank's real project (the other
folders of this repository) starts next Monday.

```
raw.demo_accounts  --dlt-->  raw_dev.demo_accounts  --dbt-->  dev.demo_accounts_clean  -->  a dashboard
 (loaded for you)              (your loader writes it)          (your model builds it)         (a title and a table)
```

* `ingestion.py`: the loader (dlt). It copies the table. Day 3.
* `dbt_demo/`: the dbt project. `models/` holds the model you write (day 4) and its test.
* `dagster_demo/`: Dagster, which runs the whole line in order (day 5).
* `dashboards/`: the dashboard page you write (day 5).

## What I built

TODO: on day 5, write here in your own words what each step does and where its result is stored (the schema and the table).
