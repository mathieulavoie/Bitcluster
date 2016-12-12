from itertools import groupby
from operator import itemgetter
import build_cluster
import map_money
from settings import settings
from pymongo import MongoClient, ASCENDING, DESCENDING


def auto_build_cluster(client):   
    collection = client.bitcoin.auto_update
    cluster_blocks_ids = [int(x['_id']) for x in collection.find({"cluster":False}).sort("_id",ASCENDING)]

    for k, g in groupby(enumerate(cluster_blocks_ids), lambda i :i[0]-i[1]): #Enumerate all missing cluster ranges
        r = list(map(itemgetter(1), g))
        build_cluster.start(r[0],r[-1])

    assert(collection.count({"cluster":False}) == 0)

    last_record = collection.find_one(sort=[("_id",DESCENDING)])
    start_block = 1
    if last_record is not None:
        start_block = last_record['_id']

    build_cluster.start(start_block+1)
    last_block_id = collection.find_one(sort=[("_id",DESCENDING)])['_id']
    return last_block_id

def auto_map_money(client, end_block):
    collection = client.bitcoin.auto_update
    transactions_blocks_ids = [int(x['_id']) for x in collection.find({"$or":[{"transactions":False},{"transactions":{"$exists":False}}]}).sort("_id",ASCENDING)]
    for k, g in groupby(enumerate(transactions_blocks_ids), lambda i :i[0]-i[1]): #Enumerate all missing cluster ranges
        r = list(map(itemgetter(1), g))
        map_money.start(r[0],r[-1])

    assert(collection.count({"$or":[{"transactions":False},{"transactions":{"$exists":False}}]}) == 0)


def initialize_autoupdate_table(client):
    auto_update_collection = client.bitcoin.auto_update
    print("It's seem to be the first use of the auto-update mechanism. However, you already have an existing dataset.")
    last_crawled_block = client.bitcoin.transactions.find_one(sort=[("block_id",DESCENDING)])['block_id']
    choice = input("Mark all blocks from 1 to block id %d as crawled? (Y/N)"%last_crawled_block).lower().strip()
    if choice == 'y' or choice == 'yes' or choice =="":
        print("Blocks from 1 to %d will be marked as crawled."%(last_crawled_block))
        auto_update_collection.insert_many([{"_id":x,"cluster":True, "transactions":True} for x in range(1,last_crawled_block+1)])
        print("Blocks marked")
    else:
        print("User refused to setup auto-update. MonngoDb collection auto_update can be created manually. Exiting.")
        exit()

if __name__ == "__main__":
    client = MongoClient(settings.db_server,settings.db_port)
    if client.bitcoin.auto_update.count() == 0 and (client.bitcoin.addresses.count() > 0 or client.bitcoin.transactions.count() > 0):
        print("Warning: No Auto-update table found. Initializing auto-update mecanism")
        initialize_autoupdate_table(client)
    last_block = auto_build_cluster(client);
    auto_map_money(client, last_block)
    client.close()
    print("Auto updated to block id %d completed"%last_block)
