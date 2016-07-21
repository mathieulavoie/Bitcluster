import sys

import bitcoin.rpc

from crawler.money_crawler import MoneyCrawler
from settings import settings


def start(start_block_id, end_block_id):
        mapper = MoneyCrawler()
        block_id = start_block_id

        try:
            while block_id <= end_block_id and mapper.crawl_block(block_id):
                print("Money of Block %d mapped" % block_id)

                if block_id - start_block_id > 0 and (block_id - start_block_id) % settings.block_crawling_limit == 0:
                    mapper.insert_into_db()
                    mapper.money_movements = []
                    mapper.proxy = bitcoin.rpc.Proxy()

                block_id+=1

            #Sync the rest
            mapper.insert_into_db()
            mapper.client.close()
            #DONE!

        #For Debugging purpose
        except:
            input("An exception will rise ")
            raise


if __name__ == "__main__":
    mapper = MoneyCrawler()
    if len(sys.argv) == 3 :
        start(int(sys.argv[1]), int(sys.argv[2]))
    else:
        print("Usage: python %s  <starting block id> <ending block id>" % sys.argv[0])