import sys


class ElasticException(Exception):
    def __str__(self):
        return f"{self.error.get('reason')}: {self.error.get('details')}"


def error_handler(response):
    if response.status_code == 400 and "error" in response.json():
        error = response.json().get("error")
        sys.tracebacklimit = 0
        custom_error = type(
            error.get("type"),  # Name of the Class
            (ElasticException,),  # Inherit the __str__ from this class
            {"error": error},  # Pass the error as attribute
        )
        raise custom_error
    response.raise_for_status()
