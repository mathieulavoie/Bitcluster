import hashlib
import bitcoin.base58
from bitcoin.core.script import OP_DUP, OP_HASH160, OP_EQUALVERIFY, OP_CHECKSIG, OP_EQUAL
from .py3specials import *


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
    Updated to fix invalid public key issues
    '''
    def decompress(self, pubkey):
        f = self.get_pubkey_format(pubkey)
        if 'compressed' not in f:
            return pubkey
        elif f == 'bin_compressed':
            return self.encode_pubkey(self.decode_pubkey(pubkey, f), 'bin')
        elif f == 'hex_compressed' or f == 'decimal':
            return self.encode_pubkey(self.decode_pubkey(pubkey, f), 'hex')



    def get_pubkey_format(self,pub):
        two = 2
        three = 3
        four = 4
        if isinstance(pub, (tuple, list)):
            return 'decimal'
        elif len(pub) == 65 and pub[0] == four:
            return 'bin'
        elif len(pub) == 130 and pub[0:2] == '04':
            return 'hex'
        elif len(pub) == 33 and pub[0] in [two, three]:
            return 'bin_compressed'
        elif len(pub) == 66 and pub[0:2] in ['02', '03']:
            return 'hex_compressed'
        elif len(pub) == 64:
            return 'bin_electrum'
        elif len(pub) == 128:
            return 'hex_electrum'
        else:
            raise Exception("Pubkey not in recognized format")

    def encode_pubkey(self, pub, formt):
        if not isinstance(pub, (tuple, list)):
            pub = self.decode_pubkey(pub)
        if formt == 'decimal':
            return pub
        elif formt == 'bin':
            return b'\x04' + encode(pub[0], 256, 32) + encode(pub[1], 256, 32)
        elif formt == 'bin_compressed':
            return from_int_to_byte(2 + (pub[1] % 2)) + encode(pub[0], 256, 32)
        elif formt == 'hex':
            return '04' + encode(pub[0], 16, 64) + encode(pub[1], 16, 64)
        elif formt == 'hex_compressed':
            return '0' + str(2 + (pub[1] % 2)) + encode(pub[0], 16, 64)
        elif formt == 'bin_electrum':
            return encode(pub[0], 256, 32) + encode(pub[1], 256, 32)
        elif formt == 'hex_electrum':
            return encode(pub[0], 16, 64) + encode(pub[1], 16, 64)
        else:
            raise Exception("Invalid format!")

    def decode_pubkey(self, pub, formt=None):
        if not formt: formt = self.get_pubkey_format(pub)
        if formt == 'decimal':
            return pub
        elif formt == 'bin':
            return (decode(pub[1:33], 256), decode(pub[33:65], 256))
        elif formt == 'bin_compressed':
            x = decode(pub[1:33], 256)
            beta = pow(int(x * x * x + A * x + B), int((P + 1) // 4), int(P))
            y = (P - beta) if ((beta + from_byte_to_int(pub[0])) % 2) else beta
            return (x, y)
        elif formt == 'hex':
            return (decode(pub[2:66], 16), decode(pub[66:130], 16))
        elif formt == 'hex_compressed':
            return self.decode_pubkey(safe_from_hex(pub), 'bin_compressed')
        elif formt == 'bin_electrum':
            return (decode(pub[:32], 256), decode(pub[32:64], 256))
        elif formt == 'hex_electrum':
            return (decode(pub[:64], 16), decode(pub[64:128], 16))
        else:
            raise Exception("Invalid format!")