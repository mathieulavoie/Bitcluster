
from jinja2 import Environment, Undefined

def format_usd(value):
    return "{:,.2f}".format(value)

def format_btc(value):
    return "{:,.8f}".format(value)

env = Environment()
env.filters['format_usd'] = format_usd
env.filters['format_btc'] = format_btc