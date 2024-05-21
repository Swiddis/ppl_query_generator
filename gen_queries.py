import json
import sys
import random
import re
from context import QueryContext

class Retry(Exception):
    """
    Custom type to abort a generation attempt and retry from scratch. Retry must only be raised if
    context is still in its original state: do not modify context before retry.
    """
    pass


def dedup(context: QueryContext):
    key = context.random_key()
    return f"dedup {key}"


def fields(context: QueryContext):
    keys = list(context.keys())
    take_count = random.randint(1, min(5, len(context)))
    fields = random.sample(keys, take_count)

    context.filter(fields)

    return f"fields {', '.join(fields)}"


def rename(context: QueryContext):
    key = context.random_key()
    tail = re.split(r"[\._]", key)[-1]
    if tail == key:
        raise Retry()

    context[tail] = context[key]
    del context[key]
    return f"rename {key} as {tail}"


def stats(context: QueryContext):
    # TODO for now we assume stats is terminal and only implement count() -- a better
    # implementation will add min/max and other functions, and better detect termination.
    key = context.random_key()
    context.clear()
    return f"stats count() by {key}"


def where(context: QueryContext):
    key = context.random_key()
    expr = context.generate_boolean_expression(key)
    return f"where {expr}"


def generate_segment(context: QueryContext, allow_terminals=False):
    choices = [dedup, fields, rename, where]
    if allow_terminals:
        choices += [stats]
    segment = random.choice(choices)
    try:
        return segment(context)
    except Retry:
        return generate_segment(context, allow_terminals)


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
