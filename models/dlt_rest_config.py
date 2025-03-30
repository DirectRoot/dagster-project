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
    def _map_remove_links(self):
        def remove_links(data):
            if '_links' in data:
                del data['_links']
            return data
        return remove_links

    @property
    def data_maps(self):
        return [
            self._map_remove_links
        ]

    @property
    @abstractmethod
    def rest(self):
        pass
    

class OktaUsers(DltRestConfig):

    name = 'okta_users'

    @property
    def rest(self):
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


class OktaGroups(DltRestConfig):

    name = 'okta_groups'

    @property
    def rest(self):
        return  {
            'client': self._client,
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