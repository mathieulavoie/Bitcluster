import logging
import bitcoin
import bitcoin.rpc
import bitcoin.core.script
import socket
import binascii
import http.client
from crawler.address_utils import  Addressutils
from bitcoin.core import CTransaction
from settings import settings
from pymongo import MongoClient


class BaseCrawler:
    def __init__(self):
        self.block_id = -1
        self.proxy = None
        self.connect_to_bitcoind_rpc()
        self.address_utils = Addressutils()
        self.crawled_blocks_ids = []

    def connect_to_bitcoind_rpc(self):
        for i in range(1,settings.rcp_reconnect_max_retry+1):
            try:
                self.proxy = bitcoin.rpc.Proxy()
                return
            except http.client.HTTPException:
                print("Caught a connection error from Bitcoind RCP, Reconnecting...(%d/%d)" %(i,settings.rcp_reconnect_max_retry))



    def crawl_block(self,block_id):
            for i in range(1,settings.rcp_reconnect_max_retry+1):
                try:
                    try:
                        self.block_id = block_id
                        block_hash = self.proxy.getblockhash(block_id)
                    except IndexError:
                        print("Block not found")
                        return False

                    block = self.proxy.getblock(block_hash)
                    for tx in block.vtx[1:]: #ignore mining tx
                        self.parse_transaction(tx,block)
                    self.crawled_blocks_ids.append(block_id)
                    return True
                except socket.error:
                    print("Caught an error from Bitcoind RCP, Reconnecting and retrying...(%d/%d)" %(i,settings.rcp_reconnect_max_retry))
                    self.connect_to_bitcoind_rpc()
                except KeyboardInterrupt:
                    print("Caught interrupt signal,exiting")
                    return False
                except Exception as e:
                    print("Caught an unhandled exception. See stacktrace:")
                    logging.exception(e)
                    print("Reconnecting and retrying...(%d/%d)" %(i,settings.rcp_reconnect_max_retry))
                    self.connect_to_bitcoind_rpc()

    def parse_transaction(self,transaction,block):
            assert isinstance(transaction,CTransaction)
            input_addresses = set()
            trx_hash = binascii.hexlify(transaction.GetHash()[::-1]).decode('utf-8')
            for vin in transaction.vin:
                try:
                    sign_script = vin.scriptSig
                    push_data_sig = sign_script[0]
                    sign_script = sign_script[1:]
                    sign_script = sign_script[push_data_sig:]

                    if len(sign_script) > 0:
                        input_addresses.add(self.address_utils.convert_hash160_to_addr(self.address_utils.convert_public_key_to_hash160(sign_script)))
                    else:
                        prevtxout = self.proxy.getrawtransaction(vin.prevout.hash).vout[vin.prevout.n]
                        input_addresses.add(self.address_utils.get_hash160_from_cscript(prevtxout.scriptPubKey))
                except Exception as ex:
                    if settings.debug:
                        print("Transaction %s Unable To Parse SigScript %s"%(trx_hash,binascii.hexlify(vin.scriptSig)))
                        print(ex)

            self.do_work(input_addresses, transaction.vout,block,trx_hash)

    def do_work(self,inputs_addresses,outputs_scripts,block,trx_hash):
        raise NotImplementedError("Not implemented method do_work")

    def mark_blocks(self, column_name):
        if len(self.crawled_blocks_ids) == 0:
            if settings.debug:
                print("Warning: Attempting to mark zero block as crawled.")
            return

        client = MongoClient(settings.db_server, settings.db_port)
        auto_update_collection = client.bitcoin.auto_update
        min_block = min(self.crawled_blocks_ids)
        if auto_update_collection.count() == 0 and min_block == 1: #Setup time!
            print("It's seem to be the first use of the auto-update mechanism.")
            while True:
                choice = input("Would you like to mark all blocks previous to block id %d as crawled? (Y/N)[Y]"\
                        .format(min_block)).lower().strip()
                if choice == "y" or choice == "yes":
                    mark_previous = True
                    print("blocks from 1 to %d will be marked as crawled."%(min_block-1))
                    break
                elif choice == "n" or choice =="no":
                    mark_previous = False
                    print("No block will be marked as crawled.")
                    break
                else:
                    print("Invalid choice.")
            auto_update_collection.insert_many([{"_id":x,"cluster":mark_previous, "transactions":mark_previous} for x in range(1,min_block)])

        for block_id in self.crawled_blocks_ids:
            auto_update_collection.update_one({'_id':block_id},{column_name:True})

        self.crawled_blocks_ids = []
        client.close()

