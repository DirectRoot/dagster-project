from abc import ABC, abstractmethod

class DltRestConfig(ABC):

    def __init__(self, org_url, api_token):
        self._org_url = org_url
        self._api_token = api_token

    @property
    def _client(self):
        return  {
                'base_url': f'{self._org_url}/api/v1',
                'auth': {   
                        'type': 'api_key',
                        'api_key': f'SSWS {self._api_token}',
                    }
                }
    
    @property
    @abstractmethod
    def config(self):
        pass
    
class OktaUsers(DltRestConfig):

    name = 'okta_users'

    @property
    def config(self):
        return {
                'client': self._client,
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
    