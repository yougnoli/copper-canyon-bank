"""The training week's pipeline in Dagster: the order of the steps, and a button to run them.

Dagster only knows what is listed at the bottom, in ``Definitions``. Today it knows one thing,
the loader. On day 5 (CCB-09) you add the dbt model, so that Dagster loads first and builds
after.
"""

from dagster import AssetExecutionContext, AssetSelection, Definitions, asset, define_asset_job
from ingestion import load_demo_accounts

# CCB-09, step 1 of 2: remove the "# " at the start of the next line. It brings in the dbt
# model, made into an asset by dbt_models.py.
# from dagster_demo.dbt_models import dbt_resource, demo_models


@asset(group_name="demo")
def demo_accounts(context: AssetExecutionContext) -> None:
    """The demo table, copied into raw_dev."""
    context.log.info(load_demo_accounts())


# CCB-09, step 2 of 2: in the list assets=[...] below add  demo_models , and add the line
#     resources={"dbt": dbt_resource},
# inside Definitions(...).
defs = Definitions(
    assets=[demo_accounts],
    jobs=[define_asset_job("demo_pipeline", selection=AssetSelection.all())],
)
