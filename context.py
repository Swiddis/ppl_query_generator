import copy
import random
import datetime


class QueryContext(dict):
    """
    Wrapper around dict that has some helper methods for usage in gen_queries
    """

    def __init__(self, context):
        # Initialize self as dict for all the familiar methods
        context = copy.deepcopy(context)

        context = dict((k, v) for k, v in context.items() if v["type"] != "none")

        super(QueryContext, self).__init__(context)
        self.__dict__ == self

        # Guards are conditions that block values from being generated
        self.seen_segments = []
        # When introducing new fields (e.g. rename, eval), we always want to use those fields later
        # in the query. Otherwise we get queries like `source=example | rename a as b | fields c`.
        # Force fields tracks fields that should always be in the final context.
        self.forced_fields = set()

    """
    sortable: key types where operations like `<` are well-defined (e.g. text, but not keywords)
    numeric: any number types
    prefer_forced: if there exists forced keys in the context, choose from them.
    Generally used by the caller to conditionally avoid dropping a forced field from the context.
    """
    def random_key(self, sortable=False, numeric=False, prefer_forced=False):
        items = list(self.items())
        if sortable:
            items = [(k, v) for k, v in items if v["type"] in ("text", "int", "float", "time")]
        if numeric:
            items = [(k, v) for k, v in items if v["type"] in ("float", "int")]
        if prefer_forced and len(self.forced_fields) > 0:
            items = [(k, v) for k, v in items if k in self.forced_fields]
        return random.choice(items)[0]

    def random_item(self):
        return random.choice(list(self.items()))

    def sample_value(self, key):
        props = self[key]
        match props["type"]:
            case "keyword" | "text":
                return repr(random.choice(props["values"]))
            case "int":
                return repr(random.randint(int(props["min"]), int(props["max"])))
            case "float":
                return repr(round(random.random() * (props["max"] - props["min"]) + props["min"]), 1)
            case "list":
                # TODO this probably needs better handling but it's not clear what lists
                # actually do
                return repr(random.choice(props["items"]))
            case "time":
                tmin, tmax = (
                    datetime.datetime.fromisoformat(props["min"]),
                    datetime.datetime.fromisoformat(props["max"]),
                )

                stime, etime = (
                    tmin.timestamp(),
                    tmax.timestamp(),
                )
                prop = random.random()
                ptime = stime + prop * (etime - stime)
                result_time = datetime.datetime.fromtimestamp(ptime)
                result = result_time.strftime("%Y-%m-%d %H:%M:%S")

                return f"TIMESTAMP('{result}')"
            case unknown:
                raise ValueError(f"Unknown prop type: {unknown}")

    def generate_boolean_expression(self, key):
        """
        Given some key, generate a simple boolean expression component based on that key. This tries
        to use semantic information about the key in the schema to intelligently choose expression
        operators that make sense. E.g. since HTTP methods are keywords there isn't much value in
        trying to query `where http_method > 'GET'`.
        """
        props = self[key]
        sample_value = self.sample_value(key)
        match props["type"]:
            case "keyword":
                op = random.choice(["=", "!=", "IN"])
                if op == "IN":
                    if len(props["values"]) == 1:
                        # If there's only one value, IN is functionally equiv. to =
                        op = "="
                    else:
                        # Otherwise, generate a random tuple of 2+ elements from available values
                        sample_value = repr(
                            tuple(
                                random.sample(
                                    props["values"],
                                    random.randint(2, min(len(props["values"]), 4)),
                                )
                            )
                        )
                return f"{key} {op} {sample_value}"
            case "text":
                op = random.choice(["=", ">", ">=", "<", "<=", "LIKE"])
                if len(sample_value) > 20:
                    op = "LIKE" # Equality checking for large strings gets messy fast
                if op == "LIKE":
                    idx = random.randint(2, 20)
                    # For now, only prefix or suffix is counted
                    sample_value = repr(
                        random.choice([sample_value[0:idx] + "%", "%" + sample_value[-idx:]])
                    )
                return f"{key} {op} {sample_value}"
            case "int":
                op = random.choice(["=", "!=", ">", ">=", "<", "<="])
                return f"{key} {op} {sample_value}"
            case "float":
                op = random.choice(["=", "!=", ">", ">=", "<", "<="])
                return f"{key} {op} {sample_value}"
            case "list":
                # It's not entirely clear what list operations are supported, but '=' and '!=' at
                # least cover CONTAINS/NOT CONTAINS
                op = random.choice(["=", "!="])
                return f"{key} {op} {sample_value}"
            case "time":
                op = random.choice(["=", ">", ">=", "<", "<="])
                return f"{key} {op} {sample_value}"
            case unknown:
                raise ValueError(f"Unknown prop type: {unknown}")


    def filter_to(self, keys):
        old_keys = list(self.keys())
        for key in old_keys:
            if key not in keys and key in self.forced_fields:
                raise ValueError(f"Attempted to filter a forced field: key {key} is forced but not in {keys}")
            if key not in keys:
                del self[key]

    def clear(self, keep_forced=False):
        self.filter_to([] if not keep_forced else list(self.force_fields))
