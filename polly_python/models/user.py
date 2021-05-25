class User():

    id = None
    email = None
    active = False
    first_name = None
    last_name = None
    confirmed_at = None
    organization = None

    def __init__(self,
                 active=False,
                 confirmed_at=None,
                 email=None,
                 first_name=None,
                 last_name=None,
                 organization=None,
                 id=None):
        self.active = active
        self.confirmed_at = confirmed_at
        self.email = email
        self.first_name = first_name
        self.last_name = last_name
        self.organization = organization
        self.id = id

    @classmethod
    def from_api_response(cls, response):
        user_data = response.json()['data']['attributes']
        return cls(**user_data)
