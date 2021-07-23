import sys

sys.tracebacklimit = 0


class ElasticException(Exception):
    def __str__(self):
        return f"{self.reason}: {self.details}"


class UnauthorizedException(Exception):
    def __str__(self):
        return "Expired or Invalid Token"


def error_handler(response):
    if has_error_message(response):
        type_, reason, details = extract_error_message_details(response)
        custom_error = type(
            type_,  # Name of the Class
            (ElasticException,),  # Inherit the __str__ from this class
            # Pass the error as attribute
            {"reason": reason, "details": details},
        )
        raise custom_error
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


def extract_error_message_details(error_response):
    error = error_response.json().get('error')
    if error is None:
        error = error_response.json().get('errors')[0]

    type_ = error.get("type", None) or error.get("code", None)
    reason = error.get("reason", None) or error.get("title", None)
    details = error.get("details", None) or error.get("detail", None)

    return type_, reason, details
