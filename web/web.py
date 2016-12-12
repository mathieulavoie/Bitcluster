import re
import csv
import io
from web.dao import getNodeFromAddress, getNodeInformation, getTransations, groupByAllDistribution, groupbyNode, \
    groupbyAmount, groupbyDate, getNodesTags
from flask import *
from datetime import datetime, timedelta


app = Flask(__name__)
import web.custom_filter



@app.route('/',methods=['POST', 'GET'])
def web_root():
    if request.method == 'POST':
        user_input = request.form['q']
        if user_input.isnumeric():
            return redirect(url_for('get_node_request',node_id=user_input))
        else:
            pattern = re.compile("^([1-9ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz])+$")
            if pattern.match(user_input): # Match Address Format
                node_id = getNodeFromAddress(user_input)
                if node_id is not None:
                    return redirect(url_for('get_node_request',node_id=node_id))

            pattern = re.compile("^([A-Za-z0-9\-\.])+$")
            if pattern.match(user_input): #Check in description.
                possible_matches = []
                for i in getNodesTags().items():
                    if user_input.lower() in i[1].lower():
                        possible_matches.append(i)

                if len(possible_matches) == 1: #Only One result
                    return redirect(url_for('get_node_request', node_id=possible_matches[0][0]))
                elif len(possible_matches) > 1:
                    return render_template('results.html', suggestions=possible_matches)


            return render_template('index.html',message="Invalid or inexistant address/description")

            
        


    return render_template('index.html')



@app.route('/hackfest')
def hello():
    return redirect("https://goo.gl/forms/QK9tHBX9Jrv7rmqw1", code=302)

@app.route('/nodes/<int:node_id>')
def get_node_request(node_id):
    infos = getNodeInformation(node_id)
    limit =100
    truncated_trx_in,trx_in = trim_collection(infos['transactions']['in'],limit)
    truncated_trx_out,trx_out = trim_collection(infos['transactions']['out'],limit)
    truncated_by_node_in,infos['incomes_grouped']['by_node'] = trim_collection(infos['incomes_grouped']['by_node'],limit)
    truncated_by_node_out,infos['outcomes_grouped']['by_node'] = trim_collection(infos['outcomes_grouped']['by_node'],limit)
    truncated_by_amount_in,infos['incomes_grouped']['by_amount']['amount_usd'] = trim_collection(infos['incomes_grouped']['by_amount']['amount_usd'],limit)
    truncated_by_amount_out,infos['outcomes_grouped']['by_amount']['amount_usd'] = trim_collection(infos['outcomes_grouped']['by_amount']['amount_usd'],limit)
    infos['transactions'] = {'in': trx_in, 'out':trx_out}


    displayed_nodes_id = set(map(lambda x: x[0],infos['incomes_grouped']['by_node']))
    displayed_nodes_id |= set(map(lambda x: x[0],infos['outcomes_grouped']['by_node']))
    for trx in trx_in + trx_out:
        displayed_nodes_id.update([trx['source_n_id'],trx['destination_n_id']])
    tags = getNodesTags(set(displayed_nodes_id))

    return render_template('node_details.html',informations=infos, tags_collection=tags, truncated=(truncated_trx_in or truncated_trx_out or truncated_by_node_in or truncated_by_node_out or truncated_by_amount_in or truncated_by_amount_out))


def trim_collection(collection, limit):
    if len(collection) > limit:
        return True, collection[0:limit]
    return False, collection



@app.route('/nodes/<int:node_id>/download/json/<direction>')
def download_transations_json(node_id,direction):
    if direction not in ["in","out"]:
        return Response(response="Invalid direction",status=500)

    transactions = getTransations(node_id,direction)
    grouped = groupByAllDistribution(transactions,direction)
    response = jsonify({"transactions":transactions, "groups":grouped})
    response.headers['Content-disposition'] = "attachment;filename=transactions_%d_%s.json"% (node_id, direction)
    return response


@app.route('/nodes/<int:node_id>/download/csv/<direction>')
def download_transations_csv(node_id,direction):
    if direction not in ["in","out"]:
        return Response(response="Invalid direction",status=500)
    
    output = io.StringIO()
    fieldnames = ['trx_date','block_id','source_n_id','destination_n_id','amount', 'amount_usd','source','destination']
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for trx in getTransations(node_id,direction):
        writer.writerow(trx)

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-disposition":"attachment; filename=transactions_%d_%s.csv"% (node_id, direction)})


@app.route('/nodes/<int:node_id>/download/csv/<direction>/<grouping>')
def download_grouped_transactions(node_id,direction,grouping):
    if direction not in ["in","out"]:
        return Response(response="Invalid direction",status=500)
    
    output = io.StringIO()
    transactions = getTransations(node_id,direction)

    writer = csv.writer(output)
    if grouping == "by_node":
        tags = getNodesTags()
        if -1 in tags: 
            del tags[-1]
        writer.writerow(['node_id','node_desc','amount_usd','amount_btc','transaction_count'])
        for k,v in groupbyNode(transactions,direction):
            writer.writerow([k,tags[k] if k in tags else "",v['amount_usd'],v['amount_btc'],len(v['transactions'])])

    elif grouping == "by_amount":
        writer.writerow(['amount_usd','frequency'])
        for k,v in groupbyAmount(transactions)['amount_usd']:
            writer.writerow([k,v])

    elif grouping == "by_date":
        date_format = '%Y-%m-%d'
        sorted_by_date = groupbyDate(transactions)

        min_date = datetime.strptime(sorted_by_date[0][0],date_format)
        max_date = datetime.strptime(sorted_by_date[-1][0],date_format)
        delta = max_date - min_date

        index = 0
        writer.writerow(['date','amount_usd','amount_btc','transaction_count'])
        for date in [min_date + timedelta(days=x) for x in range(0,delta.days+1)]:
            strdate = date.strftime(date_format)
            k,v = sorted_by_date[index]
            if k == strdate:    
                writer.writerow([k,v['amount_usd'],v['amount_btc'],len(v['transactions'])])
                index +=1
            else:
                writer.writerow([strdate,0,0,0])
    else:
        return Response(response="Invalid grouping. Possible options : by_node , by_amount , by_date",status=500)


    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-disposition":"attachment; filename=transactions_%d_%s_%s.csv"% (node_id, direction,grouping)})


