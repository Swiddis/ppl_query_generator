import json
import sys
import random
from context import QueryContext


def where(context: QueryContext):
    # TODO there should be a dedicated way to create expressions instead of doing it
    # ad_hoc here. Probably we need to standardize expression generation per prop type,
    # and work from there.
    key = context.random_key()
    value = context.sample_value(key)
    op = random.choice(["<", ">", "=", "!=", ">=", "<="])
    return f"where {key} {op} {value}"


def fields(context: QueryContext):
    keys = list(context.keys())
    take_count = random.randint(1, min(5, len(context)))
    fields = random.sample(keys, take_count)

    context.filter(fields)

    return f"fields {', '.join(fields)}"


def stats(context: QueryContext):
    # TODO for now we assume stats is terminal and only implement count() -- a better
    # implementation will add min/max and other functions, and better detect termination.
    key = context.random_key()
    context.clear()
    return f"stats count() as count by {key}"


def generate_segment(context: QueryContext, allow_terminals=False):
    choices = [where, fields]
    if allow_terminals:
        choices += [stats]
    segment = random.choice(choices)
    return segment(context)


def generate_query(index_name: str, context: QueryContext):
    query = f"source = {index_name}"
    segment_count = random.randint(1, 5)
    for segment_idx in range(segment_count):
        if len(context) == 0:
            break
        query += " | " + generate_segment(context, segment_idx + 1 == segment_count)

    return query


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
        context = QueryContext(schema)
        query = generate_query(SCHEMA_NAME, context)
        print(query)
