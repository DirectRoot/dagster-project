# TODO: Generate DBT's profiles.yml as an asset, with dynamic content based on the clients DB. This will allow for running models with the --profile flag and targetting the client's specific database

from typing import Dict, List
from os import environ
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_dbt import DagsterDbtTranslator, DbtProject, DbtCliResource, dbt_assets
from dagster_dlt import DagsterDltResource, dlt_assets
import dlt
from dlt.sources.rest_api import (
    RESTAPIConfig,
    rest_api_resources,
)
import pandas as pd

from models.client import Client
from models.dlt_rest_config import (
    OktaUsers,
    OktaGroups,
    OktaApps,
    OktaDevices,
    OktaAccessPolicies,
    OktaMfaEnrollmentPolicies,
    OktaPasswordPolicies,
    OktaProfileEnrollmentPolicies,
    OktaSignOnPolicies,
    OktaLogEvents,
    )

DLT_SOURCES = [
    OktaUsers,
    OktaGroups,
    OktaApps,
    OktaDevices,
    OktaAccessPolicies,
    OktaMfaEnrollmentPolicies,
    OktaPasswordPolicies,
    OktaProfileEnrollmentPolicies,
    OktaSignOnPolicies,
    OktaLogEvents,
]

dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath('..', '..', 'dbt_project').resolve()
)

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

# Have been able to get DBT working by creating a DBT translator that appends client ID to the asset key of
# each DBT model. Haven't linked the models to their upstream dependencies though. It's not clear how to do that
# without hardcoding

class ClientAwareDbtTranslator(DagsterDbtTranslator):
    def __init__(self, settings = None, client: Client = None):
        super().__init__(settings)
        self._client = client

    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props['resource_type']
        name = dbt_resource_props['name']
        
        if resource_type == 'model':
            return dg.AssetKey(f'{self._client.id}_{name}')
        else:
            return super().get_asset_key(dbt_resource_props)

def add_data_maps(resources, config):
    for resource in resources: # add all the data maps for the given resource
        for map in config.data_maps:
            resource.add_map(
                lambda data: map(data)
            )
    return resources


def definitions_for_a_single_client(client: Client, dlt_resource: DagsterDltResource, dbt_resource: DbtCliResource):
    # TODO: name this better and sort out if you're keeping this hacky map thing
    dlt_assets_map = {}

    # Generate assets for all of the DLT sources. You can access them later for deps etc. via assets_map
    for config_class in DLT_SOURCES:

        @dlt.source(name=f'{client.id}_{config_class.name}')
        def dlt_source_func():
            config = config_class(client.org_url, client.api_token)
            resources = rest_api_resources(config.rest) # returns a variable number of resources depending on REST config
            resources = add_data_maps(resources, config)

            yield from resources

        @dlt_assets(
            dlt_source=dlt_source_func(),
            dlt_pipeline=dlt.pipeline(
                pipeline_name=f'{client.id}_{config_class.name}',
                destination=dlt.destinations.postgres(f'postgresql://postgres:mysecretpassword@localhost:5433/{client.id}'),
                dataset_name=config_class.name
            ),
            name=f'{client.id}_{config_class.name}',
            group_name=f'{client.dagster_safe_prefix}_okta'
        )
        def dlt_assets_func(context: AssetExecutionContext, dlt: DagsterDltResource):
            yield from dlt.run(context=context)
        

        dlt_assets_map[config_class.name] = dlt_assets_func

    @dg.asset(deps=[
        dlt_assets_map[OktaLogEvents.name]
        ],
        name=f'{client.id}-new-users')
    def new_users():
        import pandas as pd

        df = pd.read_sql(
            con=f'postgresql://postgres:mysecretpassword@localhost:5433/{client.id}',
            sql="""
        select t.alternate_id, e.actor__display_name
        from okta_logs.log_events as e
        left join okta_logs.log_events__target as t
        on e."_dlt_id"  = t._dlt_parent_id
        where 
        	e.event_type = 'user.lifecycle.activate'
        	and e.published > now() - interval '72 hours';
        """
            )

        df.to_csv(f'./data/{client.id}-new-users.csv', index=False)

    dlt_assets_map['new_users'] = new_users

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        dagster_dbt_translator=ClientAwareDbtTranslator(client=client),
        name=f'{client.id}_dbt'
    )
    def dbt_assets_func(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(['run', '--profile', client.id], context=context).stream()

    dlt_assets_map['dbt_assets'] = dbt_assets_func

    return dg.Definitions(
            assets=list(dlt_assets_map.values()),
            resources={
                'dlt': dlt_resource,
                'dbt': dbt_resource
            }
        )

dlt_resource = DagsterDltResource()
dbt_resource = DbtCliResource(
    project_dir=dbt_project
)

definitions_for_all_clients = []
for client in CLIENTS_DB:
    definitions_for_all_clients.append(definitions_for_a_single_client(client, dlt_resource, dbt_resource))

defs = dg.Definitions.merge(*definitions_for_all_clients)
