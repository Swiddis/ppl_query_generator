"""
Given a sample data file as json input, generate a corresponding schema file.
For now these are hardcoded params at the end, make it into CLI args if you wish.
"""

from collections import defaultdict, Counter
import json
import re

ISO_TIME_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z")


def scan_record(record, target, prefix=""):
    for key, value in record.items():
        if isinstance(value, dict):
            scan_record(value, target, prefix + f"{key}.")
            continue

        target[prefix + key].append(value)


def columnar_aggregate_records(records):
    result = defaultdict(lambda: [])
    # We don't especially care about null handling here, since we want to only sample
    # non-nulls in schema generation
    for record in records:
        scan_record(record, result)
    return result


def find_type(value):
    if isinstance(value, str) and re.match(ISO_TIME_RE, value):
        return "time"
    elif isinstance(value, str):
        return "str"
    elif isinstance(value, int):
        return "int"
    elif isinstance(value, float):
        return "float"
    elif isinstance(value, list):
        return "list"
    else:
        raise ValueError(f"unable to find type of {repr(value)}")


def find_schema(agg_records):
    # Assume at least one column is always populated, to test nullability
    record_count = max(len(v) for v in agg_records.values())
    result = {}
    for key, values in agg_records.items():
        first_value = next((v for v in values if v is not None), None)
        value_type = find_type(first_value) if first_value else "none"
        nullable = len(values) < record_count
        match value_type:
            case "time":
                result[key] = {
                    "type": "time",
                    "min": min(values),
                    "max": max(values),
                    "nullable": nullable,
                }
            case "str":
                ctr = Counter(values)
                if len(ctr) <= 10:
                    result[key] = {
                        "type": "keyword",
                        "values": sorted(ctr),
                        "nullable": nullable,
                    }
                else:
                    examples = sorted(ctr.keys(), key=lambda k: ctr[k], reverse=True)[
                        :10
                    ]
                    result[key] = {
                        "type": "text",
                        "values": examples,
                        "nullable": nullable,
                        "unique": max(ctr.values()) == 1,
                    }
            case "int":
                ctr = Counter(values)
                result[key] = {
                    "type": "int",
                    "min": min(values),
                    "max": max(values),
                    "nullable": nullable,
                    "unique": max(ctr.values()) == 1,
                }
            case "float":
                ctr = Counter(values)
                result[key] = {
                    "type": "float",
                    "min": min(values),
                    "max": max(values),
                    "nullable": nullable,
                    "unique": max(ctr.values()) == 1,
                }
            case "list":
                ctr = Counter(v for l in values for v in l)
                max_len = max(len(l) for l in values)
                min_len = min(len(l) for l in values)
                if len(ctr) <= 10:
                    item_set = sorted(ctr)
                else:
                    item_set = sorted(ctr.keys(), key=lambda k: ctr[k], reverse=True)[
                        :10
                    ]
                result[key] = {
                    "type": "list",
                    "items": item_set,
                    "min_len": min_len,
                    "max_len": max_len,
                    "nullable": nullable,
                }
            case "none":
                result[key] = {"type": "none"}
    return result


if __name__ == "__main__":
    DATA_FILE = "data/apache_raw.json"
    SCHEMA_FILE = "schemas/ss4o_logs_apache-apache-sample-sample.json"

    with open(DATA_FILE, "r") as in_file:
        records = json.load(in_file)

    agg_records = columnar_aggregate_records(records)
    schema = find_schema(agg_records)

    with open(SCHEMA_FILE, "w") as out_file:
        json.dump(schema, out_file, indent=2)
