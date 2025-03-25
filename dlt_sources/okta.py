from typing import Any, Optional

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_resources,
    rest_api_source,
)


@dlt.source(name='okta')
def okta_source(
    okta_api_token: Optional[str] = dlt.secrets.value,
    okta_org_url: Optional[str] = dlt.config.value
    ) -> Any:
    config: RESTAPIConfig = {
        'client': {
            'base_url': f'{okta_org_url}/api/v1',
            'auth': (
                {   
                    'type': 'api_key',
                    'api_key': f'SSWS {okta_api_token}',
                }
                if okta_api_token
                else None
            ),
        },
        'resource_defaults': {
            'primary_key': 'id', # id for state
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
                    'path': 'users/{resources.users.id}',
                },
            },
            {
                'name': 'groups',
                'endpoint': {
                    'path': 'groups'
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
            {
                'name': 'apps',
                'endpoint': {
                    'path': 'apps'
                }
            },
            {
                'name': 'app',
                'endpoint': '/apps/{resources.apps.id}',
                'include_from_parent': ['id'],
            },
        ],
    }

    yield from rest_api_resources(config)


#            {
#                'name': 'issues',
#                'endpoint': {
#                    'path': 'issues',
#                    # Query parameters for the endpoint
#                    'params': {
#                        'sort': 'updated',
#                        'direction': 'desc',
#                        'state': 'open',
#                        # Define `since` as a special parameter
#                        # to incrementally load data from the API.
#                        # This works by getting the updated_at value
#                        # from the previous response data and using this value
#                        # for the `since` query parameter in the next request.
#                        'since': '{incremental.start_value}',
#                    },
#                    # For incremental to work, we need to define the cursor_path
#                    # (the field that will be used to get the incremental value)
#                    # and the initial value
#                    'incremental': {
#                        'cursor_path': 'updated_at',
#                        'initial_value': pendulum.today().subtract(days=30).to_iso8601_string(),
#                    },
#                },
#            }

def load_okta() -> None:
    pipeline = dlt.pipeline(
        pipeline_name='rest_api_okta',
        destination='filesystem',
        dataset_name='rest_api_data',
    )

    load_info = pipeline.run(
        okta_source())
    print(load_info)  # noqa: T201



if __name__ == '__main__':
    load_okta()
