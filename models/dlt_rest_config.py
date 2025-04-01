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

    # TODO: Check this works & add it to sub classes
    def _gt_filter_expression(self, timestamp):
        return f'lastUpdated gt "{timestamp}"'

    @property
    def state_incremental_params(self):
        return {
            'start_param': 'filter',
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
                    'primary_key': 'id',
                    'write_disposition': {
                        'disposition': 'merge'
                    }
                },
                'resources': [
                    {
                        'name': 'users',
                        'endpoint': {
                            'path': 'users',
                            'incremental': self.state_incremental_params
                        },
                    }
                ],
            }

    @property
    def _map_remove_credentials(self):
        def remove_credentials (data):
            if 'credentials' in data:
                del data['credentials']
            return data
        return remove_credentials

    @property
    def data_maps(self):
        maps = super().data_maps
        maps.append(self._map_remove_credentials)
        return maps



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

class OktaApps(DltRestConfig):

    name = 'okta_apps'

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
    
# TODO: OIE only
class OktaDevices(DltRestConfig):

    name = 'okta_devices'

    @property
    def rest(self):
        return {
                'client': self._client,
                'resource_defaults': {
                    'write_disposition': 'replace',
                },
                'resources': [
                    {
                        'name': 'devices',
                        'endpoint': '/devices',
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
                    'write_disposition': 'replace',
                },
                'resources': [
                    {
                        'name': 'policies_sign_on',
                        'endpoint': {
                            'path': '/policies',
                            'params': {
                                'type': 'OKTA_SIGN_ON'
                            }
                        }
                    },
                    {
                        'name': 'policy_sign_on',
                        'endpoint': {
                            'path': '/policies/{resources.policies_sign_on.id}',
                        }
                    },
                    {
                        'name': 'policy_sign_on_mappings',
                        'endpoint': {
                            'path': '/policies/{resources.policies_sign_on.id}/mappings',
                            'response_actions': [
                                {'status_code': 404, 'action': 'ignore'},
                            ],
                        }
                    },
                    {
                        'name': 'policy_sign_on_rules',
                        'endpoint': {
                            'path': '/policies/{resources.policies_sign_on.id}/rules',
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
                    'write_disposition': 'replace',
                },
                'resources': [
                    {
                        'name': 'policies_password',
                        'endpoint': {
                            'path': '/policies',
                            'params': {
                                'type': 'PASSWORD'
                            }
                        }
                    },
                    {
                        'name': 'policy_password',
                        'endpoint': {
                            'path': '/policies/{resources.policies_password.id}',
                        }
                    },
                    {
                        'name': 'policy_password_mappings',
                        'endpoint': {
                            'path': '/policies/{resources.policies_password.id}/mappings',
                            'response_actions': [
                                {'status_code': 404, 'action': 'ignore'},
                            ],
                        }
                    },
                    {
                        'name': 'policy_password_rules',
                        'endpoint': {
                            'path': '/policies/{resources.policies_password.id}/rules',
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
                    'write_disposition': 'replace',
                },
                'resources': [
                    {
                        'name': 'policies_mfa_enrollment',
                        'endpoint': {
                            'path': '/policies',
                            'params': {
                                'type': 'MFA_ENROLL'
                            }
                        }
                    },
                    {
                        'name': 'policy_mfa_enrollment',
                        'endpoint': {
                            'path': '/policies/{resources.policies_mfa_enrollment.id}',
                        }
                    },
                    {
                        'name': 'policy_mfa_enrollment_mappings',
                        'endpoint': {
                            'path': '/policies/{resources.policies_mfa_enrollment.id}/mappings',
                            'response_actions': [
                                {'status_code': 404, 'action': 'ignore'},
                            ],
                        }
                    },
                    {
                        'name': 'policy_mfa_enrollment_rules',
                        'endpoint': {
                            'path': '/policies/{resources.policies_mfa_enrollment.id}/rules',
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
                    'write_disposition': 'replace',
                },
                'resources': [
                    {
                        'name': 'policies_profile_enrollment',
                        'endpoint': {
                            'path': '/policies',
                            'params': {
                                'type': 'PROFILE_ENROLLMENT'
                            }
                        }
                    },
                    {
                        'name': 'policy_profile_enrollment',
                        'endpoint': {
                            'path': '/policies/{resources.policies_profile_enrollment.id}',
                        }
                    },
                    {
                        'name': 'policy_profile_enrollment_mappings',
                        'endpoint': {
                            'path': '/policies/{resources.policies_profile_enrollment.id}/mappings',
                            'response_actions': [
                                {'status_code': 404, 'action': 'ignore'},
                            ],
                        }
                    },
                    {
                        'name': 'policy_profile_enrollment_rules',
                        'endpoint': {
                            'path': '/policies/{resources.policies_profile_enrollment.id}/rules',
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
                'write_disposition': 'replace',
            },
            'resources': [
                {
                    'name': 'policies_access',
                    'endpoint': {
                        'path': '/policies',
                        'params': {
                            'type': 'ACCESS_POLICY'
                        }
                    }
                },
                {
                    'name': 'policy_access',
                    'endpoint': {
                        'path': '/policies/{resources.policies_access.id}',
                    }
                },
                {
                    'name': 'policy_access_mappings',
                    'endpoint': {
                        'path': '/policies/{resources.policies_access.id}/mappings',
                        'response_actions': [
                            {'status_code': 404, 'action': 'ignore'},
                        ],
                    }
                },
                {
                    'name': 'policy_access_rules',
                    'endpoint': {
                        'path': '/policies/{resources.policies_access.id}/rules',
                    }
                },
            ],
        }
    
class OktaLogEvents(DltRestConfig):

    name = 'okta_logs'

    @property
    def rest(self):
        import datetime

        today = datetime.date.today().strftime('%Y-%m-%dT%H:%M:%SZ')
        tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        return {
            'client': self._client,
            'resource_defaults': {
                    'primary_key': 'uuid',
                },
            'resources': [
                {
                    'name': 'log_events',
                    'endpoint': {
                        'path': '/logs',
                        'params': {
                            'since': today,
                            'until': tomorrow,
                            'limit': 1000,
                        }
                    }
                }
            ]
        }
    
if __name__ == '__main__':
    l = OktaLogEvents(1, 1)
    pass