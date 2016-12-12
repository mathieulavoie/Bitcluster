from web.web import app

@app.template_filter('format_usd')
def format_usd(value):
    return "${:,.2f}".format(value)

@app.template_filter('format_btc')
def format_btc(value):
    return "{:,.8f}".format(value)

@app.template_filter('get_node_tag')
def get_node_tag(node_id,tags_collection,include_prefix = False):
    if node_id in tags_collection:
        return tags_collection[node_id]
    return "Node Id %d" % node_id if include_prefix else node_id