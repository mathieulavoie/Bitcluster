from flask import Flask
from flask import jsonify
from web.dao import getNodeFromAddress, getNodeInformation, getTransations, groupByAllDistribution, groupbyNode, \
    groupbyAmount, groupbyDate

	
app = Flask(__name__)

@app.route('/')
def api_root():
    return "Bitcoin API"

@app.route('/addresses')
def getAddressesStatsRequest():
    collection = db.addresses
    stats ={"count":collection.count()}
    return jsonify(stats)


@app.route('/addresses/<address>')
def getAddressInformationRequest(address):
    node_id = db.addresses.find_one({"_id": address})['n_id']
    information = {"address": address, "node_information": getNodeInformation(node_id)}
    return jsonify(information)

@app.route('/addresses/<address>/node_id')
def getAddressNodeIdRequest(address):
    return jsonify({"node_id": db.addresses.find_one({"_id": address})['n_id']})


@app.route('/nodes')
def getNodesStatsRequest():
    collection = db.addresses
    pipeline = [{"$group": {"_id": "$n_id"}}, {"$group": { "_id": 1, "count": { "$sum": 1 } }}]
    stats ={"count":collection.aggregate(pipeline)}
    return jsonify(stats)

@app.route('/nodes/<node_id>')
def getNodeRequest(node_id):
    return jsonify(getNodeInformation(node_id))

@app.route('/nodes/<node_id>/addresses')
def getNodeAddressesRequest(node_id):
    return jsonify({"addresses" :getAddresses(node_id)})

@app.route('/nodes/<node_id>/transactions')
def getTransactionsRequest(node_id):
    incomes = getTransations(node_id,"in")
    outcomes = getTransations(node_id,"out")
    return jsonify({"node_id" : node_id,
                    "amounts_received" : getAmountTotal(incomes),
                    "amounts_sent" : getAmountTotal(outcomes),
                    "incomes": incomes,
                    "outcomes":outcomes })

@app.route('/nodes/<node_id>/transactions/<direction>/')
def getTransactionsByDirectionRequest(node_id, direction):
    transactions = getTransations(node_id,direction)
    return jsonify({"transactions":transactions})

@app.route('/nodes/<node_id>/transactions/<direction>/by_node')
def getTransactionsReceivedByNodeRequest(node_id, direction):
    transactions = getTransations(node_id,direction)
    transaction_group_node = groupbyNode(transactions,direction)
    return jsonify({"by_node":transaction_group_node})

@app.route('/nodes/<node_id>/transactions/<direction>/by_amount')
def getTransactionsReceivedByAmountRequest(node_id, direction):
    transactions = getTransations(node_id,direction)
    transaction_group_amount = groupbyAmount(transactions)
    return jsonify({"by_amount":transaction_group_amount})

@app.route('/nodes/<node_id>/transactions/<direction>/by_date')
def getTransactionsReceivedByDateRequest(node_id, direction):
    transactions = getTransations(node_id,direction)
    transaction_group_date = groupbyDate(transactions)
    return jsonify({"by_date":transaction_group_date})

@app.route('/nodes/<node_id>/transactions/<direction>/by_all_grouping')
def getTransactionsReceivedAllDispositionRequest(node_id, direction):
    return jsonify(groupByAllDistribution(getTransations(node_id,direction)))

