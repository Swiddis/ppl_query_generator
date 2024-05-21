import copy
import random

"""
Light wrapper around dict that has some helper methods for usage in gen_queries
"""
class QueryContext(dict):
    def __init__(self, context):
        # Initialize self as dict for all the familiar methods
        context = copy.deepcopy(context)
        super(QueryContext, self).__init__(context)
        self.__dict__ == self
        
        # Guards are conditions that block values from being generated
        self.guards = {key: set() for key in context}
    
    def random_key(self):
        return random.choice(list(self.keys()))

    def random_item(self):
        return random.choice(list(self.items()))

    def sample_value(self, key):
        props = self[key]
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

    def filter(self, keys):
        old_keys = list(self.keys())
        for key in old_keys:
            if key not in keys:
                del self[key]

    def clear(self):
        self.filter([])
