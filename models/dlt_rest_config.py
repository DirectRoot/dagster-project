from abc import ABC, abstractmethod

# TODO: Handle Okta 429 responses
# TODO: Get the IdP Discovery Policy as a way to check if data has been munged from two tenants?

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
    def primary_key(self):
        return 'id'

    @property
    def write_disposition(self):
        return {
            'disposition': 'merge'
            }
    
    @property
    def _map_remove_links(self):
        def remove_links(data):
            if '_links' in data:
                del data['_links']
            return data
        return remove_links
    
    @property
    def _map_remove_credentials(self):
        def remove_credentials (data):
            if 'credentials' in data:
                del data['credentials']
            return data
        return remove_credentials

    @property
    def data_maps(self):
        return [
            self._map_remove_links,
            self._map_remove_credentials
        ]

    # TODO: Check this works & add it to sub classes
    def _gt_filter_expression(self, timestamp):
        return f'lastUpdated gt "{timestamp}"'

    def incremental_params(self, start_param='filter'):
        return {
            'start_param': start_param,
            'cursor_path': 'lastUpdated',
            'initial_value': '1970-01-01T00:00:00.000Z',
            'convert': lambda timestamp: self._gt_filter_expression(timestamp)
            }

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
                    'primary_key': self.primary_key,
                    'write_disposition': self.write_disposition
                },
                'resources': [
                    {
                        'name': 'users',
                        'endpoint': {
                            'path': 'users',
                            'incremental': self.incremental_params()
                        },
                    }
                ],
            }



class OktaGroups(DltRestConfig):

    name = 'okta_groups'

    @property
    def rest(self):
        return  {
            'client': self._client,
            'resource_defaults': {
                'primary_key': self.primary_key,
                'write_disposition': self.write_disposition,
            },
            'resources': [
                {
                    'name': 'groups',
                    'endpoint': {
                        'path': '/groups',
                        'incremental': self.incremental_params(),
                        'params': {
                            'limit': 250
                        }
                    }
                },
                {
                    'name': 'group_members',
                    'endpoint': {
                        'path': '/groups/{resources.groups.id}/users',
                        'params': {
                            'limit': 1000
                        }
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

class OktaApps(DltRestConfig):

    name = 'okta_apps'

    @property
    def rest(self):
        return {
                'client': self._client,
                'resource_defaults': {
                    'primary_key': self.primary_key,
                    'write_disposition': 'replace',
                },
                'resources': [
                    {
                        'name': 'apps',
                        'endpoint': {
                            'path': '/apps',
                            'params': {
                                'limit': 200
                            }
                        }
                    },
                    {
                        'name': 'app_users',
                        'endpoint': {
                            'path': '/apps/{resources.apps.id}/users',
                            'params': {
                                'limit': 500
                            }
                        },
                        'include_from_parent': ['id'],
                    },
                ],
            }
    
# TODO: OIE only
class OktaDevices(DltRestConfig):

    name = 'okta_devices'

    @property
    def rest(self):
        return {
                'client': self._client,
                'resource_defaults': {
                    'write_disposition': self.write_disposition,
                },
                'resources': [
                    {
                        'name': 'devices',
                        'endpoint': {
                            'path': '/devices',
                            'incremental': self.incremental_params('search')
                        },
                    },
                    {
                        'name': 'device',
                        'endpoint': '/devices/{resources.devices.id}',
                        'include_from_parent': ['id'],
                    },
                    {
                        'name': 'device_users',
                        'endpoint': '/devices/{resources.devices.id}/users',
                        'include_from_parent': ['id'],
                    },
                ],
            }


class OktaSignOnPolicies(DltRestConfig):

    name = 'okta_sign_on_policies'

    @property
    def rest(self):
        return {
                'client': self._client,
                'resource_defaults': {
                    'primary_key': self.primary_key,
                    'write_disposition': 'replace',
                },
                'resources': [
                    {
                        'name': 'sign_on_policies',
                        'endpoint': {
                            'path': '/policies',
                            'params': {
                                'type': 'OKTA_SIGN_ON'
                            }
                        }
                    },
                    {
                        'name': 'sign_on_policy_mappings',
                        'endpoint': {
                            'path': '/policies/{resources.sign_on_policies.id}/mappings',
                            'response_actions': [
                                {'status_code': 404, 'action': 'ignore'},
                            ],
                        }
                    },
                    {
                        'name': 'sign_on_policy_rules',
                        'endpoint': {
                            'path': '/policies/{resources.sign_on_policies.id}/rules',
                        }
                    },
                ],
            }


class OktaPasswordPolicies(DltRestConfig):

    name = 'okta_password_policies'

    @property
    def rest(self):
        return {
                'client': self._client,
                'resource_defaults': {
                    'primary_key': self.primary_key,
                    'write_disposition': 'replace',
                },
                'resources': [
                    {
                        'name': 'password_policies',
                        'endpoint': {
                            'path': '/policies',
                            'params': {
                                'type': 'PASSWORD'
                            }
                        }
                    },
                    {
                        'name': 'password_policy_mappings',
                        'endpoint': {
                            'path': '/policies/{resources.password_policies.id}/mappings',
                            'response_actions': [
                                {'status_code': 404, 'action': 'ignore'},
                            ],
                        }
                    },
                    {
                        'name': 'password_policy_rules',
                        'endpoint': {
                            'path': '/policies/{resources.password_policies.id}/rules',
                        }
                    },
                ],
            }


class OktaMfaEnrollmentPolicies(DltRestConfig):

    name = 'okta_mfa_enrollment_policies'

    @property
    def rest(self):
        return {
                'client': self._client,
                'resource_defaults': {
                    'primary_key': self.primary_key,
                    'write_disposition': 'replace',
                },
                'resources': [
                    {
                        'name': 'mfa_enrollment_policies',
                        'endpoint': {
                            'path': '/policies',
                            'params': {
                                'type': 'MFA_ENROLL'
                            }
                        }
                    },
                    {
                        'name': 'mfa_enrollment_policy_mappings',
                        'endpoint': {
                            'path': '/policies/{resources.mfa_enrollment_policies.id}/mappings',
                            'response_actions': [
                                {'status_code': 404, 'action': 'ignore'},
                            ],
                        }
                    },
                    {
                        'name': 'mfa_enrollment_policy_rules',
                        'endpoint': {
                            'path': '/policies/{resources.mfa_enrollment_policies.id}/rules',
                        }
                    },
                ],
            }
    

class OktaProfileEnrollmentPolicies(DltRestConfig):

    name = 'okta_enrollment_policies'

    @property
    def rest(self):
        return {
                'client': self._client,
                'resource_defaults': {
                    'primary_key': self.primary_key,
                    'write_disposition': 'replace',
                },
                'resources': [
                    {
                        'name': 'profile_enrollment_policies',
                        'endpoint': {
                            'path': '/policies',
                            'params': {
                                'type': 'PROFILE_ENROLLMENT'
                            }
                        }
                    },
                    {
                        'name': 'profile_enrollment_policy_mappings',
                        'endpoint': {
                            'path': '/policies/{resources.profile_enrollment_policies.id}/mappings',
                            'response_actions': [
                                {'status_code': 404, 'action': 'ignore'},
                            ],
                        }
                    },
                    {
                        'name': 'profile_enrollment_policy_rules',
                        'endpoint': {
                            'path': '/policies/{resources.profile_enrollment_policies.id}/rules',
                        }
                    },
                ],
            }
    

class OktaAccessPolicies(DltRestConfig):

    name = 'okta_access_policies'

    @property
    def rest(self):
        return {
            'client': self._client,
            'resource_defaults': {
                'primary_key': self.primary_key,
                'write_disposition': 'replace',
            },
            'resources': [
                {
                    'name': 'access_policies',
                    'endpoint': {
                        'path': '/policies',
                        'params': {
                            'type': 'ACCESS_POLICY'
                        }
                    }
                },
                {
                    'name': 'access_policy_mappings',
                    'endpoint': {
                        'path': '/policies/{resources.access_policies.id}/mappings',
                        'response_actions': [
                            {'status_code': 404, 'action': 'ignore'},
                        ],
                    }
                },
                {
                    'name': 'access_policy_rules',
                    'endpoint': {
                        'path': '/policies/{resources.access_policies.id}/rules',
                    }
                },
            ],
        }
    
class OktaLogEvents(DltRestConfig):

    name = 'okta_logs'

    @property
    def rest(self):
        return {
            'client': self._client,
            'resource_defaults': {
                    'primary_key': 'uuid',
                    'write_disposition': 'append'
                },
            'resources': [
                {
                    'name': 'log_events',
                    'endpoint': {
                        'path': '/logs',
                        'incremental': {
                            'start_param': 'since',
                            'cursor_path': 'published',
                            'initial_value': '1970-01-01T00:00:00.000Z',
                            'convert': lambda timestamp: timestamp # suspect some weirdness where no 'convert' breaks the incremental??
                        },
                        'params': {
                            'limit': 1000,
                            'until': '9999-01-01T00:00:00.000Z' # static until, to ensure a bounded query
                        }
                    }
                }
            ]
        }
