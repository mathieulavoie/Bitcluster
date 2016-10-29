from crawler import cluster_network
from crawler import cluster_crawler
from settings import settings


import sys
from multiprocessing.context import  Process




def start(start_block_id, end_block_id = None):
        builder = cluster_crawler.ClusterCrawler()

        block_id = start_block_id
        process = None
        while builder.crawl_block(block_id):
            if end_block_id is not None and block_id > end_block_id: #Outside of specified block range
                break

            if settings.debug or block_id % 100 == 0:
                print("Block %d crawled" % block_id)

            if block_id - start_block_id > 0 and (block_id - start_block_id) % settings.block_crawling_limit == 0:
                builder.network_graph.check_integrity()
                while  process is not None and process.is_alive():
                    print("Waiting for insertion thread to complete...")
                    process.join()

                if process is not None and process.exitcode > 0 : #error
                    raise Exception("Errorcode %d in DB Sync Thread, aborting" % process.exitcode)
                process = Process(target=builder.network_graph.synchronize_mongo_db)
                process.start()
                builder.network_graph = cluster_network.ClusterNetwork(settings.db_server, settings.db_port) #Starting a new graph while other graph data is inserted.
                builder.connect_to_bitcoind_rpc()

            if process is not None and not process.is_alive() and process.exitcode > 0 : #error
                    raise Exception("Errorcode %d in DB Sync Thread, aborting" % process.exitcode)
            block_id+=1

        #Finished Crawling, Flushing to DB.
        #Waiting for any previous DB Sync
        while  process is not None and process.is_alive():
            print("Waiting for insertion thread to complete...")
            process.join()

        #Sync the rest
        print("Inserting into the DB")
        process = Process(target=builder.network_graph.synchronize_mongo_db)
        process.start()
        process.join()
        #DONE!


if __name__ == "__main__":
    if len(sys.argv) == 2 or len(sys.argv) == 3 :
        start_block_id  = int(sys.argv[1])
        end_block_id = int(sys.argv[2]) if len(sys.argv) == 3 else None
        start(start_block_id,end_block_id)
    else:
        print("Usage: python %s <starting block id> [ending_block_id]" % sys.argv[0])