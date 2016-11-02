from itertools import groupby
from operator import itemgetter
import build_cluster
import map_money
from settings import settings
from pymongo import MongoClient, ASCENDING, DESCENDING


def auto_build_cluster():
    client = MongoClient(settings.db_server,settings.db_port)
    collection = client.bitcoin.auto_update
    cluster_blocks_ids = [int(x['_id']) for x in collection.find({"cluster":False}).sort("_id",ASCENDING)]

    for k, g in groupby(enumerate(cluster_blocks_ids), lambda i :i[0]-i[1]): #Enumerate all missing cluster ranges
        for r in map(itemgetter(1), g):
            build_cluster.start(r[0],r[-1])

    assert(collection.count({"cluster":False}) == 0)

    last_record = collection.find_one(sort=[("_id",DESCENDING)])
    start_block = 1
    if last_record is not None:
        start_block = last_record['_id']

    build_cluster.start(start_block)
    last_block_id = collection.find_one(sort=[("_id",DESCENDING)])['_id']
    client.close()
    return last_block_id

def auto_map_money(end_block):
    client = MongoClient(settings.db_server,settings.db_port)
    collection = client.bitcoin.auto_update
    transactions_blocks_ids = [int(x['_id']) for x in collection.find({"$or":[{"transactions":False},{"transactions":{"$exists":False}}]}).sort("_id",ASCENDING)]

    for k, g in groupby(enumerate(transactions_blocks_ids), lambda i :i[0]-i[1]): #Enumerate all missing cluster ranges
        for r in map(itemgetter(1), g):
            map_money.start(r[0],r[-1])

    assert(collection.count({"$or":[{"transactions":False},{"transactions":{"$exists":False}}]}) == 0)

    client.close()

if __name__ == "__main__":
   last_block = auto_build_cluster();
   auto_map_money(last_block)
   print("Auto updated to block id %d completed"%last_block)