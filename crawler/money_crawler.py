import datetime
import json
import urllib.request


from pymongo import MongoClient, ASCENDING, DESCENDING

from crawler import address_utils, base_crawler
from settings import settings


class MoneyCrawler(base_crawler.BaseCrawler):

    def __init__(self):
        super().__init__()
        self.money_movements = []
        self.addr_utils = address_utils.Addressutils()
        self.client = MongoClient(settings.db_server, settings.db_port)
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        url = "https://api.coindesk.com/v1/bpi/historical/close.json?start=2011-01-01&end="+today
        self.conversion_table = json.loads(urllib.request.urlopen(url).read().decode('utf8'))['bpi']
        self.conversion_table[today] = json.loads(urllib.request.urlopen("https://api.coindesk.com/v1/bpi/currentprice/USD.json").read().decode('utf8'))['bpi']['USD']['rate_float']

        self.cache_nodeid_addresses = dict()


    def do_work(self,inputs_addresses, outputs, block, trx_hash):
        if len(inputs_addresses) == 0: #No Valid Tx, an empty block with only one mining tx
            return
        try:
            source = inputs_addresses.pop()

            if source in self.cache_nodeid_addresses:
                source_n_id = self.cache_nodeid_addresses[source]
            else:
                cursor_source_n_id = self.client.bitcoin.addresses.find_one({"_id":source})
                if cursor_source_n_id is not None:
                    source_n_id = cursor_source_n_id['n_id']
                else:
                    source_n_id = -1
                self.cache_nodeid_addresses[source] = source_n_id


            for out in outputs:
                dest = self.addr_utils.get_hash160_from_cscript(out.scriptPubKey)
                if dest in self.cache_nodeid_addresses:
                    destination_n_id = self.cache_nodeid_addresses[dest]
                else:
                    cursor_destination_n_id = self.client.bitcoin.addresses.find_one({"_id":dest})
                    if cursor_destination_n_id is not None:
                        destination_n_id = cursor_destination_n_id['n_id']
                    else:
                        destination_n_id = -1
                    self.cache_nodeid_addresses[dest] = destination_n_id

                amount_btc = (out.nValue/100000000)
                date = datetime.datetime.fromtimestamp(block.nTime).strftime('%Y-%m-%d')
                amount_usd = 0
                if date in self.conversion_table:
                    amount_usd = self.conversion_table[date] * amount_btc
                elif settings.debug:
                    print("Warning. Conversion rate from BTC to USD not found for date %s:"%date)

                entry = {'block_id':self.block_id,'source_n_id':source_n_id,'source':source,'destination_n_id':destination_n_id,'destination':dest,'amount':amount_btc, 'amount_usd':amount_usd, 'trx_date':date, 'trx_hash':trx_hash}
                self.money_movements.append(entry)
        except Exception as ex:
            if settings.debug:
                print("Unable to parse Tx for Money : %s" %  repr(outputs))
                print(ex)
            return


    def insert_into_db(self):
        if len(self.money_movements) == 0:
            if settings.debug:
                print("Warning: no money movements to insert. Aborting.")
            return

        db = self.client.bitcoin
        collection = db.transactions
        collection.insert_many(self.money_movements, ordered=False)
        print("DB Sync Finished")

    def ensure_indexes(self):
        #Ensure index existence
        db = self.client.bitcoin
        collection = db.transactions
        collection.create_index([("source_n_id", ASCENDING)])
        collection.create_index([("destination_n_id", ASCENDING)])
        collection.create_index([("source", ASCENDING)])
        collection.create_index([("destination", ASCENDING)])
        collection.create_index([("block_id",DESCENDING)])
