"""The training week's pipeline in Dagster: the order of the steps, and a button to run them.

Dagster only knows what is listed at the bottom, in ``Definitions``: the loader, then the dbt model.
"""

from dagster import AssetExecutionContext, AssetSelection, Definitions, asset, define_asset_job
from dagster_demo.dbt_models import dbt_resource, demo_models
from ingestion import load_demo_accounts


@asset(group_name="demo")
def demo_accounts(context: AssetExecutionContext) -> None:
    """The demo table, copied into raw_dev."""
    context.log.info(load_demo_accounts())


defs = Definitions(
    assets=[demo_accounts, demo_models],
    jobs=[define_asset_job("demo_pipeline", selection=AssetSelection.all())],
    resources={"dbt": dbt_resource},
)
