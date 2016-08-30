import sys

from crawler.money_crawler import MoneyCrawler
from settings import settings


def start(start_block_id, end_block_id):
        mapper = MoneyCrawler()
        block_id = start_block_id

        try:
            while block_id <= end_block_id and mapper.crawl_block(block_id):
                if settings.debug or block_id %100 == 0:
                    print("Money of Block %d mapped" % block_id)

                if block_id - start_block_id > 0 and (block_id - start_block_id) % settings.block_crawling_limit == 0:
                    mapper.insert_into_db()
                    mapper.money_movements = []
                    mapper.cache_nodeid_addresses = dict()
                    mapper.connect_to_bitcoind_rpc()

                block_id+=1

            #Sync the rest
            print("Inserting the last records in the DB")
            mapper.insert_into_db()

            print("Ensure that indexes are created")
            mapper.ensure_indexes()

            print("Done!")
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