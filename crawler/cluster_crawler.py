from crawler import base_crawler
from crawler import cluster_network
from settings import settings


class ClusterCrawler(base_crawler.BaseCrawler):
    def __init__(self):
        super().__init__()
        self.network_graph = cluster_network.ClusterNetwork(settings.db_server, settings.db_port)

    def do_work(self,inputs_addresses, outputs,block,trx_hash):
        if len(inputs_addresses) == 0:
            return
        self.network_graph.process_transaction_data(inputs_addresses, outputs)

    def start_new_graph(self):
        self.network_graph = cluster_network.ClusterNetwork(settings.db_server, settings.db_port)