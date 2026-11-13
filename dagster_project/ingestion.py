"""Loads the card data into the warehouse with dlt.

* ``core_cards``: the card list of core banking, copied whole every time.
* ``card_transactions``: the processor's files (``cards_YYYYMMDD_HH.json``, one for each hour),
  read from the landing folder. A file the pipeline has not seen by name is loaded.
"""

import json
import os
from pathlib import Path
from urllib.parse import quote

import dlt
import psycopg


def _dsn(database: str) -> str:
    user, password = os.environ["CCB_DB_USER"], os.environ["CCB_DB_PASSWORD"]
    host, port = os.environ["CCB_DB_HOST"], os.environ["CCB_DB_PORT"]
    return f"postgresql://{quote(user)}:{quote(password)}@{host}:{port}/{database}"


def _pipeline(name: str) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.postgres(credentials=_dsn(os.environ["CCB_DB_NAME"])),
        dataset_name=os.environ.get("CCB_RAW_SCHEMA", "raw_dev"),
    )


@dlt.resource(name="core_cards", write_disposition="replace")
def core_cards():
    with psycopg.connect(_dsn(os.environ["CCB_CORE_DB_NAME"])) as conn:
        cur = conn.execute(
            "select card_id, account_id, card_type, issued_on, reissue_seq from core.cards"
        )
        names = [d.name for d in cur.description]
        for row in cur:
            yield dict(zip(names, row, strict=True))


@dlt.resource(name="card_transactions", write_disposition="append")
def card_transactions():
    seen = dlt.current.resource_state().setdefault("files", [])
    for path in sorted(Path(os.environ["CCB_LANDING_DIR"]).glob("cards_*.json")):
        if path.name in seen:
            continue
        yield from json.loads(path.read_text())["transactions"]
        seen.append(path.name)


def load_core_cards() -> str:
    return str(_pipeline("core_cards").run(core_cards()))


def load_card_files() -> str:
    return str(_pipeline("card_files").run(card_transactions()))
