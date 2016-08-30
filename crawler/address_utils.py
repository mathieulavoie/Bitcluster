import hashlib
import bitcoin.base58
from bitcoin.core.script import OP_DUP, OP_HASH160, OP_EQUALVERIFY, OP_CHECKSIG, OP_EQUAL


OP_FALSE = ord('0')
# Elliptic curve parameters (secp256k1)
P = 2**256 - 2**32 - 977
N = 115792089237316195423570985008687907852837564279074904382605163141518161494337
A = 0
B = 7
Gx = 55066263022277343669578718895168534326250603453777594175500187360389116729240
Gy = 32670510020758816978083085130507043184471273380659243275938904335757337482424
G = (Gx, Gy)

class Addressutils:
    def get_hash160_from_cscript(self,script):
        if script[0] == OP_DUP and script[1] == OP_HASH160 and script[-2] == OP_EQUALVERIFY and script[-1] == OP_CHECKSIG: #P2PKH
            script = script[2:]
            script = script[1:script[0]+1]
            return self.convert_hash160_to_addr(script)

        elif  script[0] == OP_HASH160 and script[-1] == OP_EQUAL:#P2SH
            script = script[1:]
            script = script[1:script[0]+1]
            return self.convert_hash160_to_addr(script,network_id=b'\x05') #Multi-Sign Address

        elif  script[-1] == OP_CHECKSIG: #V1 Validation With Public Key
            return self.convert_hash160_to_addr(self.convert_public_key_to_hash160(script))

        raise AttributeError("CScript Format not supported")


    def convert_public_key_to_hash160(self,pub_key_data):
        h = hashlib.new('ripemd160')

        len_pub_key  = pub_key_data[0]
        pub_key_data = pub_key_data[1:] #LengthValue format.
        pub_key_data = pub_key_data[:len_pub_key]
        self.get_pubkey_format(pub_key_data)

        h.update(hashlib.sha256(pub_key_data).digest())
        return h.digest()


    def convert_hash160_to_addr(self,hash_160, network_id=b'\x00'):
        hash_160 = network_id + hash_160
        hex_addr = (hash_160 + hashlib.sha256(hashlib.sha256(hash_160).digest()).digest()[:4])
        return bitcoin.base58.encode(hex_addr)


    '''
    Taken From pybitcointools
    Converted to Python3
    '''
    def decompress(self,pubkey):
        format = self.get_pubkey_format(pubkey)
        assert(format == 'bin_compressed')
        type = pubkey[0]
        x = int.from_bytes(pubkey[1:33],byteorder='big')
        beta = pow(x*x*x+A*x+B, (P+1)//4, P)
        y = (P-beta) if ((beta + type) % 2) else beta
        coords = (x, y)
        bin_coords = (bytearray.fromhex((hex(coords[0])[2:]).zfill(64)), bytearray.fromhex((hex(coords[1])[2:]).zfill(64)))
        return b'\x04' + bin_coords[0] + bin_coords[1]



    def get_pubkey_format(self,pub):
        if len(pub) == 65 and pub[0] == 0x04: return 'bin'
        elif len(pub) == 33 and pub[0] in [0x02, 0x03]: return 'bin_compressed'
        else: raise Exception("Pubkey not in recognized format")