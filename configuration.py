from typing import Any

import yaml


def read_config(config_file: str) -> dict[str, Any]:
    config: dict[str, Any] = {}
    with open(config_file, encoding="UTF-8") as stream:
        config |= yaml.safe_load(stream)
    return config
