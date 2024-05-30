import json
import sys
import random
import re
from typing import Any
from context import QueryContext
from functools import reduce


class Retry(Exception):
    """
    Custom type to abort a generation attempt and retry from scratch. Retry must only be raised if
    context is still in its original state: do not modify context before retry.
    """

    pass


def dedup(context: QueryContext):
    key = context.random_key()
    return f"dedup {key}"


def eval_cmd(context: QueryContext):
    # TODO handle more than just time evals
    try:
        key = context.random_key(time=True)
    except IndexError:
        raise Retry()
    
    func_parts = {
        "SECOND": ("second", 0, 59),
        "MINUTE": ("minute", 0, 59),
        "HOUR": ("hour", 0, 59),
        "DAY": ("day", 1, 31),
        "WEEK": ("week", 1, 52),
        "MONTH": ("month", 1, 12),
        "YEAR": ("year", 1970, 2024),
        "DAY_OF_WEEK": ("day", 1, 7),
        "DAY_OF_YEAR": ("day", 1, 366)
    }
    part, (var, vmin, vmax) = random.choice(list(func_parts.items()))
    context[var] = {
        "type": "int",
        "min": vmin,
        "max": vmax,
        "nullable": False,
        "unique": False,
    }

    context.forced_fields.add(var)

    return f"eval {var} = {part}({key})"


def fields(context: QueryContext):
    keys = list(context.keys())
    take_count = random.randint(1, min(5, len(context)))
    fields = random.sample(keys, take_count)
    # Require forced fields
    fields = sorted(set(fields) | context.forced_fields)

    context.filter_to(fields)

    return f"fields {', '.join(fields)}"


def head(_context: QueryContext):
    return random.choice(["head 1", "head 5", "head", "head 20", "head 50"])


def parse(context: QueryContext):
    key, params = context.random_item(parse=True)
    parser = params["parser"]
    context.forced_fields.add(parser["fields"][0])
    for field in parser["fields"]:
        matches = [re.match(parser["pattern"], v) for v in params["values"]]
        context[field] = {
            "type": params["type"],
            "values": [m.group(field) for m in matches],
            "nullable": False,
            "unique": False,
        }
    # Convert from python regex dialect for group to PPL dialect
    repr_re = repr(parser['pattern'].replace('?P<', '?<'))
    return f"parse {key} {repr_re}"

def rare(context: QueryContext):
    key = context.random_key()
    by = context.random_key(prefer_forced=True)
    if by != key and random.random() < 0.75:
        context.filter_to([key, by])
        return f"rare {key} by {by}"
    if by != key and by in context.forced_fields:
        key = by
    context.filter_to([key])
    return f"rare {key}"


def top(context: QueryContext):
    top = random.choice(["top 1", "top 5", "top", "top 20", "top 50"])
    key = context.random_key()
    by = context.random_key(prefer_forced=True)
    if by != key and random.random() < 0.75:
        context.filter_to([key, by])
        return f"{top} {key} by {by}"
    if by != key and by in context.forced_fields:
        key = by
    context.filter_to([key])
    return f"{top} {key}"


def rename(context: QueryContext):
    key = context.random_key()
    unquoted = key.strip("`")
    tail = re.split(r"(_|[^\w])", unquoted)[-1]
    if tail == unquoted:
        raise Retry()

    context[tail] = context[key]
    del context[key]
    context.forced_fields.add(tail)

    return f"rename {key} as {tail}"


def sort(context: QueryContext):
    try:
        key = context.random_key(sortable=True)
        return random.choice([f"sort {key}", f"sort - {key}"])
    except IndexError:
        # No sortable keys in context
        raise Retry()


def stats(context: QueryContext):
    # TODO for now we assume stats is terminal and don't deal with context enrichment.
    stats = random.sample(["count", "sum", "avg", "max", "min"], random.randint(1, 3))
    aggs, agg_keys = [], []
    for stat in stats:
        try:
            key = context.random_key(numeric=stat != "count")
            if stat == "count" and random.random() < 0.5:
                stat_call = "count()"
            else:
                stat_call = f"{stat}({key})"
            aggs.append(f"{stat_call}")
            if stat_call != "count()":
                agg_keys.append(key)
        except IndexError:
            continue

    if aggs == []:
        # All stat loops failed
        raise Retry()

    by = context.random_key(prefer_forced=True)
    context.filter_to([by] + agg_keys)

    if not any(by in agg for agg in aggs) and (by in context.forced_fields or random.random() < 0.5):
        return f"stats {', '.join(aggs)} by {by}"
    else:
        return f"stats {', '.join(aggs)}"


def where(context: QueryContext):
    exprs = []
    seen_keys = set()
    for _ in range(random.choice([1, 1, 1, 2, 2, 3])):
        key, retries = context.random_key(), 0
        while key in seen_keys:
            if retries > 10:
                raise Retry()
            key = context.random_key()
            retries += 1
        seen_keys.add(key)

        expr = context.generate_boolean_expression(key)
        # No "NOT" since it's equivalent to flipping the operation and engines tend to do poorly
        # with generating correct queries corresponding to results with negation
        exprs.append(expr)
    # Not including XOR here since engine struggles a lot with phrasing the questions
    result = reduce(lambda a, b: a + random.choice([" AND ", " OR "]) + b, exprs)
    return f"where {result}"


def enable_parsing_if_applicable(context: QueryContext, commands: list[Any]):
    for _, params in context.items():
        if "parser" in params:
            commands.append(parse)
            return


def generate_segment(
    context: QueryContext, allow_terminals=False, retries=0
) -> str | None:
    if retries >= 10:
        # Assume this query is at a dead end
        return None
    # Forcing commands should mutually exclude each other to prevent death by too-much-context
    forcing = [eval_cmd, rename, parse]
    choices = [eval_cmd, fields, rename, sort, where]
    if allow_terminals:
        # Terminal conditions should only go at the end of a query. Generally not strictly necessary
        # that the query ends after one of these, but it's not clear why it'd be necessary
        choices += [dedup, head, rare, stats, top]
    enable_parsing_if_applicable(context, choices)

    for cmd in context.seen_segments:
        if cmd in choices:
            choices.remove(cmd)
        if cmd in forcing:
            choices = [c for c in choices if c not in forcing]
    segment = random.choice(choices)
    try:
        return segment(context)
    except Retry:
        return generate_segment(context, allow_terminals, retries=retries + 1)


def generate_query(index_name: str, context: QueryContext):
    query = f"source = {index_name}"
    segment_count = random.randint(1, 5)
    for segment_idx in range(segment_count):
        if len(context) == 0:
            break
        segment = generate_segment(context, segment_idx + 1 == segment_count)
        if segment is None:
            break
        query += " | " + segment
        context.seen_segments.append(QUERY_FN_MAP[segment.split()[0]])

    return query


QUERY_FN_MAP = {
    "dedup": dedup,
    "eval": eval_cmd,
    "fields": fields,
    "head": head,
    "parse": parse,
    "rare": rare,
    "top": top,
    "rename": rename,
    "sort": sort,
    "stats": stats,
    "where": where,
}


if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: gen_queries.py [schema] [quantity?]")
        exit(1)
    schema_name = sys.argv[1]

    if len(sys.argv) == 3:
        gen_count = int(sys.argv[2])
    else:
        gen_count = 10

    with open(f"schemas/{schema_name}.json", "r") as schema_file:
        schema = json.load(schema_file)

    for _ in range(gen_count):
        context = QueryContext(schema)
        query = generate_query(schema_name, context)
        print(query)
