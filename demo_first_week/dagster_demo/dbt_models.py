"""Dagster's view of the dbt project: every dbt model becomes an asset that Dagster can run.

You do not change this file. Read it once: ``demo_models`` is the only part that matters to
you. It says "take every model of the dbt project in ``dbt_demo/`` and make an asset of it".
``Translator`` gives the table that the loader makes (the dbt source) the same name as the
loader's asset, so that Dagster draws an arrow from the loader to the model: that is how it
knows the order.
"""

from pathlib import Path

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

dbt_project = DbtProject(project_dir=Path(__file__).parent.parent / "dbt_demo")
dbt_project.prepare_if_dev()
dbt_resource = DbtCliResource(project_dir=dbt_project)


class Translator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey(dbt_resource_props["name"])  # the table the loader makes
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=Translator())
def demo_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
