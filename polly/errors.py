import sys

sys.tracebacklimit = 0


class RequestException(Exception):
    def __init__(self, title, detail=None):
        self.title = title
        self.detail = detail

    def __str__(self):
        if self.detail:
            return f"{self.title}: {self.detail}"
        return self.title


class UnauthorizedException(Exception):
    def __str__(self):
        return "Expired or Invalid Token"


class UnfinishedQueryException(Exception):
    def __init__(self, query_id):
        self.query_id = query_id

    def __str__(self):
        return f"Query \"{self.query_id}\" has not finished executing"


class QueryFailedException(Exception):
    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return f"Query failed to execute\n\treason: {self.reason}"


class OperationFailedException(Exception):
    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return f"{self.reason}"


class InvalidPathException(Exception):
    def __str__(self):
        return "This path does not represent a file or a directory. Please try again."


class MissingKeyException(Exception):
    def __init__(self, key):
        self.reason = key

    def __str__(self):
        return f"Missing keys {self.key}"


class InvalidParameterException(Exception):
    def __init__(self, parameter):
        self.parameter = parameter

    def __str__(self):
        return f"Empty or Invalid Parameters = {self.parameter}."


class InvalidFormatException(Exception):
    def __str__(self):
        return "File format not supported."


def error_handler(response):
    if has_error_message(response):
        title, detail = extract_json_api_error(response)
        raise RequestException(title, detail)
    elif response.status_code == 401:
        raise UnauthorizedException

    response.raise_for_status()


def has_error_message(response):
    try:
        for key in response.json().keys():
            if key in {'error', 'errors'}:
                return True
        return False
    except Exception:
        return False


def extract_json_api_error(response):
    error = response.json().get('error')
    if error is None:
        error = response.json().get('errors')[0]

    title = error.get("title")
    detail = error.get("detail")
    return title, detail


def extract_error_message_details(error_response):
    error = error_response.json().get('error')
    if error is None:
        error = error_response.json().get('errors')[0]

    type_ = error.get("type", None) or error.get("code", None)
    reason = error.get("reason", None) or error.get("title", None)
    details = error.get("details", None) or error.get("detail", None)

    return type_, reason, details


def is_unfinished_query_error(exception):
    return isinstance(exception, UnfinishedQueryException)
