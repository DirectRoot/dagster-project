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
            # we add an auth config if the auth token is present
            'auth': (
                {   
                    'type': 'api_key',
                    'api_key': f'SSWS {okta_api_token}',
                }
                if okta_api_token
                else None
            ),
        },
        # The default configuration for all resources and their endpoints
        'resource_defaults': {
            'primary_key': 'id', # id for state, uuid for logs
            'write_disposition': 'replace',
        },
        'resources': [
            # This is a simple resource definition,
            # that uses the endpoint path as a resource name:
            # 'pulls',
            # Alternatively, you can define the endpoint as a dictionary
            # {
            #     'name': 'pulls', # <- Name of the resource
            #     'endpoint': 'pulls',  # <- This is the endpoint path
            # }
            # Or use a more detailed configuration:
            {
                'name': 'users',
                'endpoint': {
                    'path': 'users',
                    'params': {
                        'limit': '1'
                    },
                },
            },
            # The following is an example of a resource that uses
            # a parent resource (`issues`) to get the `issue_number`
            # and include it in the endpoint path:
            {
                'name': 'user',
                'endpoint': {
                    # The placeholder `{resources.issues.number}`
                    # will be replaced with the value of `number` field
                    # in the `issues` resource data
                    'path': 'users/{resources.users.id}',
                },
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
