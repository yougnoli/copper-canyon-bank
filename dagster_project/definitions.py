"""The orchestration: load the card data, build the models, every night at 01:00 (Phoenix time)."""

from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
    multiprocess_executor,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from dagster_project.ingestion import load_card_files, load_core_cards

dbt_project = DbtProject(project_dir=Path(__file__).parent.parent / "dbt_project")
dbt_project.prepare_if_dev()


@asset(group_name="ingestion")
def core_cards(context: AssetExecutionContext) -> None:
    """The card list of core banking, in the warehouse."""
    context.log.info(load_core_cards())


@asset(group_name="ingestion")
def card_transactions(context: AssetExecutionContext) -> None:
    """The processor's card files, in the warehouse."""
    context.log.info(load_card_files())


class Translator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey(dbt_resource_props["name"])  # the tables the ingestion assets load
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=Translator())
def models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


night_job = define_asset_job("night_job", selection=AssetSelection.all())
night_schedule = ScheduleDefinition(
    job=night_job, cron_schedule="0 1 * * *", execution_timezone="America/Phoenix"
)

defs = Definitions(
    assets=[core_cards, card_transactions, models],
    jobs=[night_job],
    schedules=[night_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
    # the workspace has 1.5 GB: two steps at a time
    executor=multiprocess_executor.configured({"max_concurrent": 2}),
)
