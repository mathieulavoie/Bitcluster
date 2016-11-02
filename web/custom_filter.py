from jinja2 import Environment

def format_usd(value):
    return "{:,.2f}".format(value)

def format_btc(value):
    return "{:,.8f}".format(value)

def get_node_mapping(node_id,mapping_collection):
    if node_id in mapping_collection:
        return mapping_collection[node_id]
    return node_id

env = Environment()
env.filters['format_usd'] = format_usd
env.filters['format_btc'] = format_btc
env.filters['get_node_mapping'] = get_node_mapping