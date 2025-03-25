import dagster as dg
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets
from dlt import pipeline

from dlt_sources.okta import okta_source

@dlt_assets(
    dlt_source=okta_source(),
    dlt_pipeline=pipeline(
        pipeline_name='rest_api_okta',
        destination='filesystem',
        dataset_name='rest_api_data'
    ),
    name='okta',
    group_name='okta'
)
def dagster_okta_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

defs = dg.Definitions(
    assets=[
        dagster_okta_assets,
    ],
    resources={
        "dlt": DagsterDltResource(),
    },
)