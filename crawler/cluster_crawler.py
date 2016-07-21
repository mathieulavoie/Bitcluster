from crawler import base_crawler
from crawler import cluster_network
from settings import settings


class ClusterCrawler(base_crawler.BaseCrawler):
    def __init__(self):
        super().__init__()
        self.to_crawl =[]
        self.crawled = []
        self.network_graph = cluster_network.ClusterNetwork(settings.db_server, settings.db_port)

    def do_work(self,inputs, outputs,block):
        self.network_graph.process_transaction_data(inputs, outputs)