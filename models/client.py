# Client as in customer, not client as in API client

class Client():

    def __init__(self, config):
        self.id = config['id']
        self.name = config['name']
        self.org_url = config['org_url']
        self.api_token = config['api_token']

    @property
    def dagster_safe_name(self):
        return self.name.replace('-', '_')