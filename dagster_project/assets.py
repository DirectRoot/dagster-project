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

def add_data_maps(resources, config):
    for resource in resources: # add all the data maps for the given resource
        for map in config.data_maps:
            resource.add_map(
                lambda data: map(data)
            )
    return resources


def definitions_for_a_single_client(client: Client, dlt_resource: DagsterDltResource):
    assets_map = {}

    # Generate assets for all of the DLT sources. You can access them later for deps etc. via assets_map
    for config_class in DLT_SOURCES:

        @dlt.source(name=f'{client.id}_{config_class.name}')
        def source_func():
            config = config_class(client.org_url, client.api_token)
            resources = rest_api_resources(config.rest) # returns a variable number of resources depending on REST config
            resources = add_data_maps(resources, config)

            yield from resources

        @dlt_assets(
            dlt_source=source_func(),
            dlt_pipeline=dlt.pipeline(
                pipeline_name=f'{client.id}_{config_class.name}',
                destination=dlt.destinations.postgres(f'postgresql://postgres:mysecretpassword@localhost:5433/{client.id}'),
                dataset_name=config_class.name
            ),
            name=f'{client.id}_{config_class.name}',
            group_name=f'{client.dagster_safe_prefix}_okta'
        )
        def assets_func(context: AssetExecutionContext, dlt: DagsterDltResource):
            yield from dlt.run(context=context)
        

        assets_map[config_class.name] = assets_func

    @dg.asset(deps=[
        assets_map[OktaUsers.name]
        ],
        name=f'{client.id}-report')
    def report():
        print('it works!')

    assets_map['report'] = report

    return dg.Definitions(
            assets=list(assets_map.values()),
            resources={
                'dlt': dlt_resource
            }
        )

dlt_resource = DagsterDltResource()
definitions_for_all_clients = []
for client in CLIENTS_DB:
    definitions_for_all_clients.append(definitions_for_a_single_client(client, dlt_resource))

defs = dg.Definitions.merge(*definitions_for_all_clients)
