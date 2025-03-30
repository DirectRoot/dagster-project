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

CLIENTS_DB = {
    'dev-44559887' : {
        'name': 'dev-44559887',
        'pipeline_prefix': 'dev_44559887',
        'org_url': 'https://dev-44559887.okta.com',
        'api_token': '00C7g1V8AtbUNeLpnpVPEkhKuAXqbgHSZ6B9KmndFO'
        },
    'dev-14449001' : {
        'name': 'dev-14449001',
        'pipeline_prefix': 'dev_14449001',
        'org_url': 'https://dev-14449001.okta.com',
        'api_token': '00pGgPKFqvwjLNuBYt5i5QatmmKaE3Onf11lmEp1m-'
        }
}

def pipelines_factory(clients_db):
    assets = []
    for client, config in clients_db.items():
        pipeline_prefix_upper = config['pipeline_prefix'].upper() # uppercase for env var names

        environ[f'{pipeline_prefix_upper}_OKTA_USERS__BUCKET_URL'] = f'./data/{config['name']}'


        @dlt.source(name=f'{config["pipeline_prefix"]}_okta_users')
        def okta_users(okta_api_token, okta_org_url):
            config: RESTAPIConfig = {
                'client': {
                    'base_url': f'{okta_org_url}/api/v1',
                    'auth': {   
                            'type': 'api_key',
                            'api_key': f'SSWS {okta_api_token}',
                        }
                    },
                'resource_defaults': {
                    'primary_key': 'id',
                    'write_disposition': 'replace',
                },
                'resources': [
                    {
                        'name': 'users',
                        'endpoint': {
                            'path': 'users'
                        },
                    },
                    {
                        'name': 'user',
                        'endpoint': {
                            'path': '/users/{resources.users.id}',
                        },
                    },
                ],
            }

            yield from rest_api_resources(config)


        @dlt_assets(
            dlt_source=okta_users(config['api_token'], config['org_url']),
            dlt_pipeline=dlt.pipeline(
                pipeline_name=f'{config["pipeline_prefix"]}_okta_users',
                destination='filesystem',
                dataset_name=f'okta_users'
            ),
            name=f'{config["pipeline_prefix"]}_okta_users',
            group_name=f'{config["pipeline_prefix"]}_okta'
        )
        def okta_user_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
            yield from dlt.run(context=context)
        
        assets.append(okta_user_assets)

    return assets


defs = dg.Definitions(
    assets=pipelines_factory(CLIENTS_DB),
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
