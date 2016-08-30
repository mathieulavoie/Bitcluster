import bitcoin
import bitcoin.rpc
import bitcoin.core.script
import socket
import binascii
import http.client
from crawler.address_utils import  Addressutils
from bitcoin.core import CTransaction
from settings import settings
from bitcoin.core.script import OP_FALSE


class BaseCrawler:
    def __init__(self):
        self.block_id = -1
        self.proxy = None
        self.connect_to_bitcoind_rpc()
        self.address_utils = Addressutils()

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
                    return True
                except socket.error:
                    print("Caught an error from Bitcoind RCP, Reconnecting and retrying...(%d/%d)" %(i,settings.rcp_reconnect_max_retry))
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