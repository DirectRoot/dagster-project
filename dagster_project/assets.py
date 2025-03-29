import dagster as dg
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets
from dlt import pipeline

from dlt_sources.okta import (
    okta_apps,
    okta_devices,
    okta_groups,
    okta_access_policies,
    okta_users,
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

@dlt_assets(
    dlt_source=okta_apps(),
    dlt_pipeline=pipeline(
        pipeline_name='okta_apps',
        destination='filesystem',
        dataset_name='okta_apps'
    ),
    name='okta_apps',
    group_name='okta'
)
def okta_app_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

@dlt_assets(
    dlt_source=okta_devices(),
    dlt_pipeline=pipeline(
        pipeline_name='okta_devices',
        destination='filesystem',
        dataset_name='okta_devices'
    ),
    name='okta_devices',
    group_name='okta'
)
def okta_device_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

@dlt_assets(
    dlt_source=okta_access_policies(),
    dlt_pipeline=pipeline(
        pipeline_name='okta_access_policies',
        destination='filesystem',
        dataset_name='okta_access_policies'
    ),
    name='okta_access_policies',
    group_name='okta'
)
def okta_access_policy_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

defs = dg.Definitions(
    assets=[
        okta_user_assets,
        okta_group_assets,
        okta_app_assets,
        okta_device_assets,
        okta_access_policy_assets
    ],
    resources={
        "dlt": DagsterDltResource(),
    },
)