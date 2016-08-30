from crawler import address_utils
from crawler.node import Node
from settings import settings
from pymongo import MongoClient, DESCENDING


class ClusterNetwork:
    def __init__(self, db_server, db_port,):
        self.db_server = db_server
        self.db_port = db_port
        self.addr_utils = address_utils.Addressutils()
        self.nodes = {}
        self.next_node_id = 1
        self.address_registry = {}


    def check_integrity(self):
        addresses_repertory = []
        for node in self.nodes.values():
            addresses_repertory += node.addresses
        addresses_repertory = sorted(addresses_repertory)
        print("Nb addr : ",len(addresses_repertory))
        print("Nb nodes : ",len(self.nodes))
        for i in range(len(addresses_repertory)-1):
            if addresses_repertory[i] == addresses_repertory[i+1]:
                print("duplicate for addr :",addresses_repertory[i])
                raise Exception("Invalid Graph Consistancy: duplicates addresses")

    def chunks(self,l, n):
        n = max(1, n)
        return [l[i:i + n] for i in range(0, len(l), n)]


    def process_transaction_data(self,inputs, outputs):
        self.merge_into_graph(inputs)

    def merge_into_graph(self,addresses_in):
        new_node_addresses = []
        destination_node_id = -1
        for address in addresses_in:
            if address in self.address_registry:
                current_node_id = self.address_registry[address]
                if current_node_id == destination_node_id : continue;
                if destination_node_id >= 0:
                    self.nodes[destination_node_id].merge(self.address_registry,self.nodes,self.nodes[current_node_id])
                else:
                    destination_node_id = current_node_id
            else:
                new_node_addresses.append(address)

        if destination_node_id < 0:
            destination_node_id = self.next_node_id
            node = Node(destination_node_id)
            self.nodes[destination_node_id] = node
            self.next_node_id +=1

        self.nodes[destination_node_id].add_new_unique_adddresses(self.address_registry,new_node_addresses)


    def synchronize_mongo_db(self):
        client = MongoClient(self.db_server, self.db_port)
        db = client.bitcoin
        collection = db.addresses
        transactions = db.transactions
        db_next_node_id = 1

        #Ensure index existence
        collection.create_index([("n_id", DESCENDING)])

        for x in collection.find().sort("n_id",DESCENDING).limit(1):
            db_next_node_id = x['n_id'] +1

        for node in self.nodes.values():

            existing_addresses = set()
            distinct_nodes_id = set()

            for addr in self.chunks(node.addresses, settings.max_batch_insert):
                addresses_nodes = collection.find({"_id": {'$in':addr}})

                for x in addresses_nodes:
                    existing_addresses.add(x['_id'])
                    distinct_nodes_id.add(x['n_id'])

            merge_node_id = -1;
            if len(existing_addresses) > 0:
                min_node_id = min(distinct_nodes_id)
                merge_node_id = min_node_id
                if len(distinct_nodes_id) > 1: # More than one node in DB, merge required
                    distinct_nodes_id.remove(merge_node_id)
                    collection.update_many({'n_id':{'$in':[x for x in distinct_nodes_id]}}, {'$set':{'n_id':merge_node_id}}) #Update Addresses Table

                    transactions.update_many({'source_n_id':{'$in':[x for x in distinct_nodes_id]}}, {'$set':{'source_n_id':merge_node_id}}) #Update trx Table
                    transactions.update_many({'destination_n_id':{'$in':[x for x in distinct_nodes_id]}}, {'$set':{'destination_n_id':merge_node_id}}) #Update trx Table


            else:
                merge_node_id = db_next_node_id
                db_next_node_id +=1

            new_addresses = (set(node.addresses) - existing_addresses)
            to_insert = [{'_id':x,'n_id':merge_node_id} for x in new_addresses]
            if len(to_insert) > 0:
                collection.insert_many(to_insert)
                for new_addresses_chunk in self.chunks([x for x in new_addresses], settings.max_batch_insert):
                    addr_to_update_trx  = [x for x in new_addresses_chunk]
                    transactions.update_many( {'$and':[{'source_n_id':-1}, {'source':{'$in':addr_to_update_trx}}]}, {'$set':{'source_n_id':merge_node_id}})
                    transactions.update_many( {'$and':[{'destination_n_id':-1}, {'destination':{'$in':addr_to_update_trx}}]}, {'$set':{'destination_n_id':merge_node_id}})

        client.close()
        print("DB Sync Finished")