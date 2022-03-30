from cerberus import Validator


def validate(document, schema, require_all=False, allow_unknown=True, update=False):
    """
    Validate the document against the schema

    Philosophy here is to allow unknown keys,
    but validate the fields you're expecting to have

    Args:
        document (dict): Document to be validate
        schema (dict): Cerberus schema
        require_all (bool, optional): Whether all fields are required.
            Defaults to False.
        allow_unknown (bool, optional): Whether allow fields other than
            specified. Defaults to True.
        update (bool, optional): Ignore required flags while validating
            Defaults to False.

    Raises:
        ValueError: if Document is invalid
    """
    validator = Validator(require_all=require_all, allow_unknown=allow_unknown)
    if not validator.validate(document, schema, update=update):
        raise ValueError(validator.errors)
