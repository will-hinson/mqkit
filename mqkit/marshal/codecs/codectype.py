from enum import Enum


class CodecType(str, Enum):
    JSON = "json"
    YAML = "yaml"
