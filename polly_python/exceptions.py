class MalFormedConfigurationException(Exception):
    def __init__(self, message=None):
        if not message:
            message = "Malformed Configuration."
        Exception.__init__(self, message)

class PollyLoginException(Exception):
    def __init__(self, message=None):
        if not message:
            message = "Polly Login Failed."
        Exception.__init__(self, message)
