import json
from src.schemas.config_schema import validate_config


def read_json(file_path):

    with open(file_path, "r") as f:
        data = json.load(f)

    validate_config(data)

    return data