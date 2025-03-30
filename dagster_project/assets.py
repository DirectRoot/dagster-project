from typing import Dict, List
from os import environ

import dagster as dg
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets
import dlt
from dlt.sources.rest_api import (
    RESTAPIConfig,
    rest_api_resources,
)

from dlt_sources.okta import (
    okta_apps,
    okta_devices,
    okta_groups,
    okta_sign_on_policies,
    okta_password_policies,
    okta_mfa_enrollment_policies,
    okta_profile_enrollment_policies,
    okta_access_policies,
    #okta_users,
)
from models.client import Client
from models.dlt_rest_config import OktaUsers

dlt.config['normalize.data_writer.disable_compression'] = True # TODO: Only in local mode

CLIENTS_DB = [
    Client({
        'id': 'A0000001',
        'name': 'dev-44559887',
        'org_url': 'https://dev-44559887.okta.com',
        'api_token': '00C7g1V8AtbUNeLpnpVPEkhKuAXqbgHSZ6B9KmndFO'
        }),
    Client({
        'id': 'A0000002',
        'name': 'dev-14449001',
        'org_url': 'https://dev-14449001.okta.com',
        'api_token': '00pGgPKFqvwjLNuBYt5i5QatmmKaE3Onf11lmEp1m-'
        })
]

DLT_SOURCES = [
    OktaUsers,
]

def assets_factory(clients_db: List[Client]):
    assets = []
    for client in clients_db:
        for source in DLT_SOURCES:
            environ[f'{client.id}_{source.name.upper()}__BUCKET_URL'] = f'./data/{client.id}'


            @dlt.source(name=f'{client.id}_{source.name}')
            def source_func():

                def remove_links(data: Dict):
                    if '_links' in data:
                        del data['_links']
    
                    return data
                
                resources = rest_api_resources(OktaUsers(client.org_url, client.api_token).config)
                for resource in resources:
                    resource.add_map(
                        lambda data: remove_links(data)
                    )
                
                yield from resources


            @dlt_assets(
                dlt_source=source_func(),
                dlt_pipeline=dlt.pipeline(
                    pipeline_name=f'{client.id}_{source.name}',
                    destination='filesystem',
                    dataset_name=source.name
                ),
                name=f'{client.id}_{source.name}',
                group_name=f'{client.dagster_safe_prefix}_okta'
            )
            def okta_user_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
                yield from dlt.run(context=context)

            assets.append(okta_user_assets)

    return assets


defs = dg.Definitions(
    assets=assets_factory(CLIENTS_DB),
    resources={
        "dlt": DagsterDltResource(),
    },
)

#@dlt_assets(
#    dlt_source=okta_groups(),
#    dlt_pipeline=pipeline(
#        pipeline_name='okta_groups',
#        destination='filesystem',
#        dataset_name='okta_groups'
#    ),
#    name='okta_groups',
#    group_name='okta'
#)
#def okta_group_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
#    yield from dlt.run(context=context)
#
#@dlt_assets(
#    dlt_source=okta_apps(),
#    dlt_pipeline=pipeline(
#        pipeline_name='okta_apps',
#        destination='filesystem',
#        dataset_name='okta_apps'
#    ),
#    name='okta_apps',
#    group_name='okta'
#)
#def okta_app_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
#    yield from dlt.run(context=context)
#
#@dlt_assets(
#    dlt_source=okta_devices(),
#    dlt_pipeline=pipeline(
#        pipeline_name='okta_devices',
#        destination='filesystem',
#        dataset_name='okta_devices'
#    ),
#    name='okta_devices',
#    group_name='okta'
#)
#def okta_device_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
#    yield from dlt.run(context=context)
#
#@dlt_assets(
#    dlt_source=okta_sign_on_policies(),
#    dlt_pipeline=pipeline(
#        pipeline_name='okta_sign_on_policies',
#        destination='filesystem',
#        dataset_name='okta_sign_on_policies'
#    ),
#    name='okta_sign_on_policies',
#    group_name='okta'
#)
#def okta_sign_on_policy_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
#    yield from dlt.run(context=context)
#
#@dlt_assets(
#    dlt_source=okta_password_policies(),
#    dlt_pipeline=pipeline(
#        pipeline_name='okta_password_policies',
#        destination='filesystem',
#        dataset_name='okta_password_policies'
#    ),
#    name='okta_password_policies',
#    group_name='okta'
#)
#def okta_password_policy_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
#    yield from dlt.run(context=context)
#
#@dlt_assets(
#    dlt_source=okta_mfa_enrollment_policies(),
#    dlt_pipeline=pipeline(
#        pipeline_name='okta_mfa_enrollment_policies',
#        destination='filesystem',
#        dataset_name='okta_mfa_enrollment_policies'
#    ),
#    name='okta_mfa_enrollment_policies',
#    group_name='okta'
#)
#def okta_mfa_enrollment_policy_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
#    yield from dlt.run(context=context)
#
#@dlt_assets(
#    dlt_source=okta_profile_enrollment_policies(),
#    dlt_pipeline=pipeline(
#        pipeline_name='okta_profile_enrollment_policies',
#        destination='filesystem',
#        dataset_name='okta_profile_enrollment_policies'
#    ),
#    name='okta_profile_enrollment_policies',
#    group_name='okta'
#)
#def okta_profile_enrollment_policy_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
#    yield from dlt.run(context=context)
#
#@dlt_assets(
#    dlt_source=okta_access_policies(),
#    dlt_pipeline=pipeline(
#        pipeline_name='okta_access_policies',
#        destination='filesystem',
#        dataset_name='okta_access_policies'
#    ),
#    name='okta_access_policies',
#    group_name='okta'
#)
#def okta_access_policy_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
#    yield from dlt.run(context=context)
