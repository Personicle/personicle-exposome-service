import json
from fastavro import parse_schema, validate
import os

__file_path = os.path.abspath(__file__)
__dir_path = os.path.dirname(__file_path)

SCHEMA_LOC = __dir_path
EVENT_SCHEMA = "event_schema.avsc"

def validate_event_schema(records):
    with open(os.path.join(SCHEMA_LOC, EVENT_SCHEMA), "r") as fp:
        schema = json.load(fp)
    parsed_schema = parse_schema(schema)
    if type(records) == type({}):
        if validate(records, parsed_schema):
            return records
    elif type(records) == type([]):
        return list(filter(lambda record: validate(record, parsed_schema), records))
    return None


def validate_datastream_schema(records, schema_file):
    with open(os.path.join(SCHEMA_LOC, schema_file), "r") as fp:
        schema = json.load(fp)
    parsed_schema = parse_schema(schema)

    if records is not None and validate(records, parsed_schema):
        return records
    return None