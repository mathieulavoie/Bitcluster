class Node:
    def __init__(self,id):
        self.id = id
        self.addresses = []

    def __contains__(self, addr_tuple):
        return any(addr in self.addresses for addr in addr_tuple)

    def merge(self,global_address_registry,node_registry, node):
        self.addresses.extend(node.addresses)
        self.addresses =  list(set(self.addresses))
        for address in node.addresses:
            global_address_registry[address] = self.id
        del node_registry[node.id]


    def add_new_unique_adddresses(self, global_address_registry, new_addresses):
        self.addresses.extend(new_addresses)
        for address in new_addresses:
            global_address_registry[address] = self.id

