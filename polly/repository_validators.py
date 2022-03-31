from polly.validators import validate


def validate_frontend_info(info):
    schema = REPOSITORY_SCHEMA["frontend_info"]["schema"]
    validate(info, schema, update=True)


def validate_components_input(input):
    input_schema = {"components": COMPONENTS_INPUT_SCHEMA}
    validate({"components": input}, input_schema)


def validate_studio_presets_input(input):
    input_schema = {"studio_presets": STUDIO_PRESETS_INPUT_SCHEMA}
    validate({"studio_presets": input}, input_schema)


def validate_repository_schema(repository, update=False):
    validate(repository, REPOSITORY_SCHEMA, update=update)


STUDIO_PRESETS_INPUT_SCHEMA = {
    "keysrules": {"type": "string"},
    "valuesrules": {"type": "list", "schema": {"type": "string"}},
}

COMPONENTS_INPUT_SCHEMA = {
    "keysrules": {"type": "integer"},
    "valuesrules": {"type": "list", "schema": {"type": "string"}},
}

REPOSITORY_SCHEMA = {
    "components": {
        "type": "list",
        "schema": {
            "type": "dict",
            "schema": {
                "component_id": {"type": "integer"},
                "data_type": {"type": "list", "schema": {"type": "string"}},
            },
        },
    },
    "frontend_info": {
        "type": "dict",
        "schema": {
            "description": {
                "type": "string",
                "minlength": 1,
                "maxlength": 50,
                "required": True,
            },
            "display_name": {
                "type": "string",
                "minlength": 1,
                "maxlength": 20,
                "required": True,
            },
            "explorer_enabled": {"type": "boolean", "default": False},
            "initials": {
                "type": "string",
                "minlength": 1,
                "maxlength": 4,
                "required": True,
            },
        },
    },
    "repo_id": {"type": "string"},
    "repo_name": {"type": "string", "required": True},
    "studio_presets": {
        "type": "list",
        "schema": {
            "type": "dict",
            "schema": {
                "data_type": {"type": "list", "schema": {"type": "string"}},
                "preset_id": {
                    "type": "string",
                },
            },
        },
    },
}
