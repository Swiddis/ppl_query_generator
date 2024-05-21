import json
import sys
import copy
import random


def pull_value(props):
    match props["type"]:
        case "keyword" | "text":
            return repr(random.choice(props["values"]))
        case "int":
            return repr(random.randint(props["min"], props["max"]))
        case "list":
            # TODO this probably needs better handling but it's not clear what lists
            # actually do
            return repr(random.choice(props["items"]))
        case "time":
            # TODO
            return repr(props["min"])
        case unknown:
            raise ValueError(f"Unknown prop type: {unknown}")


def filter_context(context, keys):
    old_keys = list(context.keys())
    for key in old_keys:
        if key not in keys:
            del context[key]


def where(context):
    items = list(context.items())
    key, props = random.choice(items)
    # TODO there should be a dedicated way to create expressions instead of doing it
    # ad_hoc here. Probably we need to standardize expression generation per prop type,
    # and work from there.
    value = pull_value(props)
    op = random.choice(["<", ">", "=", "!=", ">=", "<="])
    return f"where {key} {op} {value}"


def fields(context):
    keys = list(context.keys())
    take_count = random.randint(1, min(5, len(context)))
    fields = random.sample(keys, take_count)

    filter_context(context, fields)

    return f"fields {', '.join(fields)}"


def stats(context):
    # TODO for now we assume stats is terminal and only implement count() -- a better
    # implementation will add min/max and other functions, and better detect termination.

    keys = list(context.keys())
    key = random.choice(keys)

    for k in keys:
        del context[k]

    return f"stats count() as count by {key}"


def generate_segment(context, allow_terminals=False):
    choices = [where, fields]
    if allow_terminals:
        choices += [stats]
    segment = random.choice(choices)
    return segment(context)


def generate_query(index_name, context):
    query = f"source = {index_name}"
    segment_count = random.randint(1, 5)
    for segment_idx in range(segment_count):
        if context == {}:
            break
        query += " | " + generate_segment(context, segment_idx + 1 == segment_count)

    return query


def make_context(schema):
    schema = copy.deepcopy(schema)
    for value in schema.values():
        value["guards"] = set()
    return schema


if __name__ == "__main__":
    SCHEMA_NAME = "nginx"

    if len(sys.argv) == 2:
        gen_count = int(sys.argv[1])
    elif len(sys.argv) == 1:
        gen_count = 10
    else:
        print("Usage: gen_queries.py [amount]")
        exit(1)

    with open(f"schemas/{SCHEMA_NAME}.json", "r") as schema_file:
        schema = json.load(schema_file)

    for _ in range(gen_count):
        context = make_context(schema)
        query = generate_query(SCHEMA_NAME, context)
        print(query)
