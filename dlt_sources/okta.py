from typing import Any, Optional

import dlt
from dlt.sources.rest_api import (
    RESTAPIConfig,
    rest_api_resources,
)

def _client_config(okta_api_token, okta_org_url):
    return {
            'base_url': f'{okta_org_url}/api/v1',
            'auth': {   
                    'type': 'api_key',
                    'api_key': f'SSWS {okta_api_token}',
                }
            }

@dlt.source(name='okta_users')
def okta_users(
    okta_api_token: Optional[str] = dlt.secrets.value,
    okta_org_url: Optional[str] = dlt.config.value
    ) -> Any:
    config: RESTAPIConfig = {
        'client': _client_config(okta_api_token, okta_org_url),
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


@dlt.source(name='okta_groups')
def okta_groups(
    okta_api_token: Optional[str] = dlt.secrets.value,
    okta_org_url: Optional[str] = dlt.config.value
    ) -> Any:
    config: RESTAPIConfig = {
        'client': _client_config(okta_api_token, okta_org_url),
        'resource_defaults': {
            'primary_key': 'id',
            'write_disposition': 'replace',
        },
        'resources': [
            {
                'name': 'groups',
                'endpoint': {
                    'path': '/groups'
                }
            },
            {
                'name': 'group',
                'endpoint': {
                    'path': '/groups/{resources.groups.id}'
                }
            },
            {
                'name': 'group_members',
                'endpoint': {
                    'path': '/groups/{resources.groups.id}/users'
                },
                'include_from_parent': ['id'],
            },
            {
                'name': 'group_apps',
                'endpoint': '/groups/{resources.groups.id}/apps',
                'include_from_parent': ['id'],
            },
            {
                'name': 'group_owners',
                'endpoint': {
                    'path': '/groups/{resources.groups.id}/owners',
                    'response_actions': [
                        {'status_code': 401, 'action': 'ignore'},
                    ],
                },
                'include_from_parent': ['id'],
            },
        ],
    }

    yield from rest_api_resources(config)

@dlt.source(name='okta_apps')
def okta_apps(
    okta_api_token: Optional[str] = dlt.secrets.value,
    okta_org_url: Optional[str] = dlt.config.value
    ) -> Any:
    config: RESTAPIConfig = {
        'client': _client_config(okta_api_token, okta_org_url),
        'resource_defaults': {
            'primary_key': 'id',
            'write_disposition': 'replace',
        },
        'resources': [
            {
                'name': 'apps',
                'endpoint': {
                    'path': '/apps'
                }
            },
            {
                'name': 'app',
                'endpoint': '/apps/{resources.apps.id}',
                'include_from_parent': ['id'],
            },
            {
                'name': 'app_users',
                'endpoint': '/apps/{resources.apps.id}/users',
                'include_from_parent': ['id'],
            },
        ],
    }

    yield from rest_api_resources(config)
            
#            {
#                'name': 'devices',
#                'endpoint': '/devices',
#            },
#            {
#                'name': 'device',
#                'endpoint': '/devices/{resources.devices.id}',
#                'include_from_parent': ['id'],
#            },
#            {
#                'name': 'device_users',
#                'endpoint': '/devices/{resources.devices.id}/users',
#                'include_from_parent': ['id'],
#            },
#            {
#                'name': 'policy_sign_on',
#                'endpoint': {
#                    'path': '/policies',
#                    'params': {
#                        'type': 'OKTA_SIGN_ON'
#                    }
#                }
#            }
