"""The training week's loader: it copies the demo table into your warehouse with dlt.

Read the file from the top. It does three things:

* ``_pipeline`` says where to write: the training database, into the schema ``raw_dev`` (the
  login comes from the environment, never from this file);
* ``demo_accounts`` says what to copy (every row of ``raw.demo_accounts``) and how:
  ``replace`` means "empty the target table, then write all the rows", so running it twice
  gives the same table;
* ``load_demo_accounts`` runs it.

Run it by hand, in the editor's terminal:  cd demo_first_week && python ingestion.py
"""

import os
from urllib.parse import quote

import dlt
import psycopg


def _dsn() -> str:
    user, password = os.environ["CCB_DB_USER"], os.environ["CCB_DB_PASSWORD"]
    host, port = os.environ["CCB_DB_HOST"], os.environ["CCB_DB_PORT"]
    return f"postgresql://{quote(user)}:{quote(password)}@{host}:{port}/{os.environ['CCB_DEMO_DB_NAME']}"


def _pipeline() -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="demo_first_week",
        destination=dlt.destinations.postgres(credentials=_dsn()),
        dataset_name="raw_dev",
    )


@dlt.resource(name="demo_accounts", write_disposition="replace")
def demo_accounts():
    with psycopg.connect(_dsn()) as conn:
        cur = conn.execute(
            "select account_id, holder_name, city, opened_on, balance from raw.demo_accounts"
        )
        names = [d.name for d in cur.description]
        for row in cur:
            yield dict(zip(names, row, strict=True))


def load_demo_accounts() -> str:
    return str(_pipeline().run(demo_accounts()))


if __name__ == "__main__":
    print(load_demo_accounts())
