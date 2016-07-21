import datetime
import json
import urllib.request

from pymongo import MongoClient

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


    def do_work(self,inputs, outputs, block):
        if len(inputs) == 0: #No Valid Tx, an empty block with only one mining tx
            return
        try:
            source = self.addr_utils.convert_hash160_to_addr(self.addr_utils.convert_public_key_to_hash160(inputs[0]))

            cursor_source_n_id = self.client.bitcoin.addresses.find_one({"_id":source})
            if cursor_source_n_id is not None:
                source_n_id = cursor_source_n_id['n_id']
            else:
                source_n_id = -1


            for out in outputs:
                dest = self.addr_utils.get_hash160_from_cscript(out.scriptPubKey)
                cursor_destination_n_id = self.client.bitcoin.addresses.find_one({"_id":dest})
                if cursor_destination_n_id is not None:
                    destination_n_id = cursor_destination_n_id['n_id']
                else:
                    destination_n_id = -1

                amount_btc = (out.nValue/100000000)
                date = datetime.datetime.fromtimestamp(block.nTime).strftime('%Y-%m-%d')
                amount_usd = self.conversion_table[date] * amount_btc
                entry = {'block_id':self.block_id,'source_n_id':source_n_id,'source':source,'destination_n_id':destination_n_id,'destination':dest,'amount':amount_btc, 'amount_usd':amount_usd, 'trx_date':date}
                self.money_movements.append(entry)
        except Exception:
            #print("Unable to parse Tx for Money : %s" %  repr(outputs))
            return


    def insert_into_db(self):
        if len(self.money_movements) == 0:
            print("Warning: no money movements to insert. Aborting.")
            return

        db = self.client.bitcoin
        collection = db.transactions
        collection.insert_many(self.money_movements, ordered=False)
        print("DB Sync Finished")


