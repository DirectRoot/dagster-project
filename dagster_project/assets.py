import dagster as dg
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets
from dlt import pipeline

from dlt_sources.okta import (
    okta_users,
    okta_groups
)

@dlt_assets(
    dlt_source=okta_users(),
    dlt_pipeline=pipeline(
        pipeline_name='okta_users',
        destination='filesystem',
        dataset_name='okta_users'
    ),
    name='okta_users',
    group_name='okta'
)
def okta_user_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

@dlt_assets(
    dlt_source=okta_groups(),
    dlt_pipeline=pipeline(
        pipeline_name='okta_groups',
        destination='filesystem',
        dataset_name='okta_groups'
    ),
    name='okta_groups',
    group_name='okta'
)
def okta_group_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

defs = dg.Definitions(
    assets=[
        okta_user_assets,
        okta_group_assets
    ],
    resources={
        "dlt": DagsterDltResource(),
    },
)