import bitcoin
import bitcoin.rpc
import bitcoin.core.script
from datetime import datetime
import binascii
from bitcoin.core import CTransaction


class BaseCrawler:
    def __init__(self):
        self.proxy = bitcoin.rpc.Proxy()
        self.start = datetime.now()
        self.block_id = -1

    def crawl_block(self,block_id):

            try:
                self.block_id = block_id
                hash = self.proxy.getblockhash(block_id)
            except IndexError as ex:
                print("Block not found")
                return False

            block = self.proxy.getblock(hash)
            for tx in block.vtx[1:]: #ignore mining tx
                self.parse_transaction(tx,block)
            return True

    def parse_transaction(self,transaction,block):
            assert isinstance(transaction,CTransaction)
            transaction.GetHash()
            signed_script_input = []
            try:
                for vin in transaction.vin:
                    push_data_sig = vin.scriptSig[0]
                    signed_script = vin.scriptSig[1:]
                    signed_script = signed_script[push_data_sig:]
                    signed_script_input.append(signed_script)
                self.do_work(signed_script_input, transaction.vout,block)
            except:
                print("WARNING : Unable to process transaction ", binascii.hexlify(transaction.GetHash()[::-1]) )

    def do_work(self,inputs,outputs,block):
        raise NotImplementedError("Not implemented method do_work")