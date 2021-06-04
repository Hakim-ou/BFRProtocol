from http.server import BaseHTTPRequestHandler, HTTPServer 
from urllib.parse import urlparse, parse_qs
from redisbloom.client import Client
import redis
import time
import calendar;
import threading
import grequests
import json

# RedisBloom Client
redis_client = Client()

# load neighbors ip addresses.
ips = set()
with open("ips.txt", "r") as f:
    for line in f:
        ips.add(line.strip())

# redis standard port
STANDARD_PORT = 6379

# local bloom standard name
LOCAL_BLOOM = "bf:localBF"

# local bloom old value
BLOOM_DUMP = [] # table of chunks
BLOOM_UP_TO_DATE = True # no new data

# PUSH-BASED BFR Simplified:
# description: in this version we will implement push-based BFR but in a particular
#              case where the nodes forme a complete graph, so there will be no
#              retransmission of CAIs. All we need to implement is reception of a
#              request, where we will check if we have the requested key, and if not
#              we will check our neighbors (all the other nodes in this case) to see
#              any node got the needed information. We will send a request then to this
#              node in order to get the requested value, and we will send it as an answer to
#              the requester. Otherwise, if no node has the needed information, we will
#              answer with a 404. 
#              Besides this, every node has to check regularly (we choosed one second in this
#              implementation) if it has new keys, in wich case it will send a POST request
#              to all its neighbors (all the other nodes in our case) with a json string
#              containing a dump of its local bloom filter (that sumerize the keys it
#              contains).

class ProxyHTTPRequestHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.0'

    def do_GET(self, body=True):
        """
        do_GET will deal with client Interests
        """
        try:
            # extract the key
            query_components = parse_qs(urlparse(self.path).query)
            key = query_components['key']
            value = redis_client.get(key)
            if value is None:
                # content not in cache, so we will check if it is in
                # one of the neighbors (all the other caches are our
                # neighbors for the moment). 
                timestamps = redis_client.mget((f"ts:{ip}" for ip in ips))
                for ip, timestamp in zip(ips, timestamps):
                    if redis_client.bfExists(f"bf:{ip}:{timestamp}", key):
                        # send request to redis ip (standard port 6379)
                        distant_client = redis.Redis(ip, STANDARD_PORT)
                        value = distant_client.get(key) # we suppose that the returned value 
                                                        # is of string type.
                        self.send_response(200)
                        self.wfile.write(value)
                        return
                self.send_response(404)
            else:
                # content is in cache, return it directly
                self.send_response(200)
                self.wfile.write(value)
        finally:
            self.finish()

    def do_POST(self, body=True):
        """
        do_POST will deal with Content Advertisments
        """
        try:
            content_len = int(self.headers.get('Content-Length'))
            post_body = self.rfile.read(content_len)
            bloom_chunks = json.load(post_body)
            ip = self.client_address[0]
            assert(ip in ips, "Got POST request from unknown source") # make sure this post is from a known neighboor
            timestamp = calendar.timegm(time.gmtime())
            redis_client.set(f"ts:{ip}", timestamp)
            restoreBF(bloom_chunks, f"bf:{ip}:{timestamp}")
        finally:
            self.finish()

def saveBF():
    """
    dumps the local bloom. Multiple chunks are used in case
    the BF is too large to be SAVEd in one chunk
    """
    chunks = []
    iter = 0
    while True:
        iter, data = redis_client.bfScandump(LOCAL_BLOOM, iter)
        if iter == 0:
            return chunks
        else:
            chunks.append([iter, data])

def chekckForNews():
    """
    checks if there is any new keys in redis. In this case,
    global variables BLOOM_UP_TO_DATE and BLOOM_DUMP are updated
    """
    global BLOOM_DUMP, BLOOM_UP_TO_DATE
    # iterate over keys and BF.ADD them to local BF
    # NB: This step wont be necessary when we'll have
    #     control over writes (BF.ADD after evry write)
    for key in redis_client.keys:
        if not key.startswith("bf:"):
            redis_client.bfAdd(LOCAL_BLOOM, key)
    new_dump = saveBF()

    if (len(new_dump) != len(BLOOM_DUMP)) or any((new_dump[i][1] != BLOOM_DUMP[i][1] for i in range(len(new_dump)))):
            BLOOM_UP_TO_DATE = False
            BLOOM_DUMP = new_dump
    else:
        BLOOM_UP_TO_DATE = False
        BLOOM_DUMP = new_dump

def restoreBF(chunks, key):
    """
    restores the given chunks in redis under the given key name

    :param: chunks the data to restore in the BF
    :param: key the name to give to the BF
    """
    for chunk in chunks:
        iter, data = chunk
        redis_client.bfLoadChunk(key, iter, data)

def CAIsProducer():
    """
    Checks every second if there is any new keys in redis,
    and sends update to neighbors if we found any
    """
    while True:
        chekckForNews()
        if not BLOOM_UP_TO_DATE:
            # send update to neighbors. We used grequests for multithreading
            json_bloom = json.dump(BLOOM_DUMP)
            urls = [f"http://{ip}:{STANDARD_PORT}" for ip in ips]
            rs = (grequests.post(u, json_bloom) for u in urls)
            grequests.map(rs)
        time.sleep(1)

if __name__ == '__main__':
    # lunch Interests and CAIs handlers
    server_address = ('127.0.0.1', 6380)
    httpd = HTTPServer(server_address, ProxyHTTPRequestHandler)
    print('http server is running')
    httpd.serve_forever()

    # lunch CA producer
    CAIsProducerThread = threading.Thread(target=CAIsProducer)
    CAIsProducerThread.start()



# AN OTHER IDEA:
# receive request, if this redis server can fulfill it than
# everything is okey. If not, we redirect the request to all
# redis neighbors telling them "client 'ip' sent this".
# Before all this, we check the nounce to verify that we did
# redirect or served this request before.
# Benifits: no iterests to store, and the way back is not
# necessarly the same, plus a gain in bandwidthn. But probably
# these problems are alredy solved with NC protocol
