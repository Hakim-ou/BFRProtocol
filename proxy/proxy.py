#!/usr/bin/env python3

from gevent import monkey
monkey.patch_all()

from http.server import BaseHTTPRequestHandler, HTTPServer 
from urllib.parse import urlparse, parse_qs
from redisbloom.client import Client
import redis
import time
import calendar;
import threading
import grequests
import json
import sys
import base64

# Local address
LOCAL_IP = sys.argv[1]
LOCAL_PORT = int(sys.argv[2])

# RedisBloom Client
redis_client = Client(host=LOCAL_IP, port=LOCAL_PORT)

# load neighbors ip addresses.
ips = list()
with open("ips.txt", "r") as f:
    for line in f:
        adr = line.strip()
        ip, port, db = adr.split(":")
        ips.append([ip, int(port), int(db)])

# redis standard port
STANDARD_PORT = 6379

# local bloom standard name
LOCAL_BLOOM = "bf:localBF"

# local bloom old value
BLOOM_DUMP = [] # table of chunks
BLOOM_UP_TO_DATE = True # no new data

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                        Strategies
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

class ContentNotFound(Exception):
    def __init__(self):
        super().__init__("Content Not found")


"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                        PUSH-BASED BFR Simplified:

description: in this version we will implement push-based BFR but in a particular
             case where the nodes forme a complete graph, so there will be no
             retransmission of CAIs. All we need to implement is reception of a
             request, where we will check if we have the requested key, and if not
             we will check our neighbors (all the other nodes in this case) to see
             any node got the needed information. We will send a request then to this
             node in order to get the requested value, and we will send it as an answer to
             the requester. Otherwise, if no node has the needed information, we will
             answer with a 404. 
             Besides this, every node has to check regularly (we choosed one second in this
             implementation) if it has new keys, in wich case it will send a POST request
             to all its neighbors (all the other nodes in our case) with a json string
             containing a dump of its local bloom filter (that sumerize the keys it
             contains).
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

class ProxyPushBaedBFR(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.0'

    def do_GET(self, body=True):
        """
        do_GET will deal with client Interests
        """
        print("Get request recieved...")
        try:
            # extract the key
            query_components = parse_qs(urlparse(self.path).query)
            key = str(query_components['key'][0]) # query_components['key'] returns a list of arguments
            value = redis_client.get(key)
            if value is None:
                # content is not in cache, so we will check if it is in
                # one of the neighbors (all the other caches are our
                # neighbors for the moment). 
                print("Content not in cache. Checking neighboors...")
                # TODO lookup timestamps with mget (one network call to redis)
                try:
                    value = checkFIBForContent(key)
                    self.send_response(200)
                    self.wfile.write(value)
                except ContentNotFound:
                    self.send_response(404)
            else:
                print("Content found in cache...200 OK")
                # content is in cache, return it directly
                self.send_response(200)
                self.wfile.write(value)
        finally:
            self.finish()

    def do_POST(self, body=True):
        """
        do_POST will deal with Content Advertisments
        """
        print("Post request recieved...")
        try:
            content_len = int(self.headers.get('Content-Length'))
            post_body = self.rfile.read(content_len)
            bloom_chunks = json.loads(post_body.decode('utf-8'))
            ip, port = str(bloom_chunks["address"]).split(":")
            print(f"From: {ip}:{port}")
            bloom_chunks = bloom_chunks['bf']
            #assert(ip in ips, "Got POST request from unknown source") # make sure this post is from a known neighboor
            timestamp = calendar.timegm(time.gmtime())
            redis_client.set(f"ts:{ip}:{port}", timestamp)
            restoreBF(bloom_chunks, f"bf:{ip}:{port}:{timestamp}")
        finally:
            self.finish()



"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                        PULL-BASED BFR Simplified:

description: TODO
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# A set of pending interests
pending_interests = set()

class ProxyPullBasedBF(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.0'

    def do_GET(self, body=True):
        """
        do_GET will deal with client Interests
        """
        print("Get request recieved...")
        try:
            # extract the key
            query_components = parse_qs(urlparse(self.path).query)
            key = str(query_components['key'][0]) # query_components['key'] returns a list of arguments
            value = redis_client.get(key)
            if value is None:
                # Content not in cache.
                try:
                    # Check if we know where to find it.
                    value = checkFIBForContent(key)
                except ContentNotFound:
                    # Otherwise add the key to the CAR BF
                    pending_interests.add(key)
                    timestamp = redis_client.get(f"ts:localTS").decode('utf-8')
                    redis_client.bfAdd(f"bf:localCAR:{timestamp}", key)
                    return
            else:
                print("Content found in cache...200 OK")
                # content is in cache, return it directly
                self.send_response(200)
                self.wfile.write(value)
        finally:
            self.finish()

    def do_POST(self, body=True):
        """
        do_POST will deal with CAIs and CARs reception
        """
        print("Post request recieved...")
        try:
            # reteive the post body
            content_len = int(self.headers.get('Content-Length'))
            post_body = self.rfile.read(content_len)
            bloom_chunks = json.loads(post_body.decode('utf-8'))
            ip, port = str(bloom_chunks["address"]).split(":")
            print(f"From: {ip}:{port}")
            bloom_chunks = bloom_chunks['bf']
            if str(bloom_chunks['code']) == "CAR":
                # If this is a CAR request, we will verify if we have
                # any of the requested contents
                # NB: timestamps are used only to avoid key name duplication error
                #     since redis do not allow BFs overriding
                # 1) we will write the received BF to redis in order to decode it
                timestamp = calendar.timegm(time.gmtime())
                tmp_BF = f"bf:{ip}:{port}:{timestamp}"
                keysFoundInLocalCache = set()
                restoreBF(bloom_chunks, tmp_BF)
                # 2) we will check for every content we have if it is requested in this
                #    CAR or not
                for key in redis_client.keys():
                    if not (str(key).startswith("b'bf:") or str(key).startswith("b'ts:")): # just to spare some redis network calls
                        if redis_client.bfExists(tmp_BF, key):
                            keysFoundInLocalCache.add(key)
                # 3) we will check if we found out that some of our content is requested
                #    If that's the case, we will respond with a CAI containing the required content
                #    we possess. 
                # NB: We used grequests for threading reasons
                if len(keysFoundInLocalCache) != 0:
                    timestamp = calendar.timegm(time.gmtime())
                    tmp_BF = f"bf:{ip}:{port}:{timestamp}"
                    redis_client.bfMAdd(keysFoundInLocalCache)
                    response = saveBF(tmp_BF)
                    json_bloom = {"code":"CAI", "address":f"{LOCAL_IP}:{LOCAL_PORT}", "bf":response}
                    json_bloom = json.dumps(json_bloom)
                    url = [f"http://{ip}:{port+1}"]
                    rs = [grequests.post(url[0], data=json_bloom)]
                    grequests.map(rs)
            elif str(bloom_chunks['code']) == "CAI":                    
                # If this is a CAI request, we will populate our FIB first,
                # then we will check if we have pending interests that we can answer now,
                # we will try to resolve them.
                # 1) we insert the received CAI for the right source (next hope) in our redis
                timestamp = calendar.timegm(time.gmtime())
                redis_client.set(f"ts:{ip}:{port}", timestamp)
                restoreBF(bloom_chunks, f"bf:{ip}:{port}:{timestamp}")
                # 2) we check if there are any pending interests
                keysToFetch = set()
                for key in pending_interests:
                    if redis_client.bfExists(f"bf:{ip}:{port}:{timestamp}", key):
                        keysToFetch.add(key)
                        pending_interests.remove(key) # TODO this is to verify
                # TODO we can make the following queries into one single query
                rs = [grequests.get(f"http://{ip}:{port+1}", params={"key":key}) for key in keysToFetch]
                response = grequests.map(rs)
                return
            else:
                raise Exception("Unknow action code")
        finally:
            self.finish()



"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                    FIB and PIT managers
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

def checkFIBForContent(key):
    """
    Check if the key is in the BF of one of the neighbors.
    If so, it sends a request to the neighbor to ask for the
    value. Otherwise, it raises an error for content not found
    """
    found = False
    timestamps = redis_client.mget((f"ts:{ip[0]}:{ip[1]}" for ip in ips))
    for ip, timestamp in zip(ips, timestamps):
        #timestamp = redis_client.get(f"ts:{ip[0]}:{ip[1]}").decode('utf-8')
        if timestamp is None:
            continue
        timestamp = timestamp.decode('utf-8')
        if redis_client.bfExists(f"bf:{ip[0]}:{ip[1]}:{timestamp}", key):
            found = True
            print(f"Content maybe at {ip[0]}:{ip[1]} .. Sending request...")
            # send request to redis ip (standard port 6379)
            distant_client = redis.Redis(ip[0], ip[1])
            value = distant_client.get(key) # we suppose that the returned value 
                                            # is of string type.
            return value
    if not found:
        print("Content not found...404")
        raise ContentNotFound()



"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                        CAIs and CARs producers
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

def saveBF(bloom=LOCAL_BLOOM):
    """
    dumps the local bloom. Multiple chunks are used in case
    the BF is too large to be SAVEd in one chunk
    """
    chunks = []
    iter = 0
    while True:
        iter, data = redis_client.bfScandump(bloom, iter)
        if iter == 0:
            return chunks
        else:
            chunks.append([iter, base64.b64encode(data).decode('ascii')])

def checkForNews():
    """
    checks if there is any new keys in redis. In this case,
    global variables BLOOM_UP_TO_DATE and BLOOM_DUMP are updated
    """
    global BLOOM_DUMP, BLOOM_UP_TO_DATE
    # iterate over keys and BF.ADD them to local BF
    # NB: This step wont be necessary when we'll have
    #     control over writes (BF.ADD after evry write)
    for key in redis_client.keys():
        if not (str(key).startswith("b'bf:") or str(key).startswith("b'ts:")):
            redis_client.bfAdd(LOCAL_BLOOM, key)
    new_dump = saveBF()

    if (len(new_dump) != len(BLOOM_DUMP)) or any((new_dump[i][1] != BLOOM_DUMP[i][1] for i in range(len(new_dump)))):
            BLOOM_UP_TO_DATE = False
            BLOOM_DUMP = new_dump
    else:
        BLOOM_UP_TO_DATE = True

def restoreBF(chunks, key):
    """
    restores the given chunks in redis under the given key name

    :param: chunks the data to restore in the BF
    :param: key the name to give to the BF
    """
    for chunk in chunks:
        iter, data = chunk
        redis_client.bfLoadChunk(key, iter, base64.b64decode(data))

def CAIsProducer():
    """
    Checks every second if there is any new keys in redis,
    and sends update to neighbors if we found any
    """
    while True:
        print("Cheking for new content")
        checkForNews()
        if not BLOOM_UP_TO_DATE:
            # send update to neighbors. We used grequests for multithreading
            print("Sending CAI to neighbors...")
            json_bloom = {"code":"CAI", "address":f"{LOCAL_IP}:{LOCAL_PORT}", "bf":BLOOM_DUMP}
            json_bloom = json.dumps(json_bloom)
            urls = [f"http://{ip[0]}:{ip[1]+1}" for ip in ips if ip[1] != LOCAL_PORT]
            rs = (grequests.post(u, data=json_bloom) for u in urls)
            grequests.map(rs)
        time.sleep(1)

def CARProcucer():
    """
    Checks periodicaly if we have pending intersets. If there is any, we send
    CAR to neighbors, asking them for content advertisment
    """
    while True:
        print("Cheking for pending interests...")
        if len(pending_interests) != 0:
            # TODO send CAR
            print("Sending CAR to neighbors...")
            timestamp = redis_client.get(f"ts:localTS").decode('utf-8')
            json_bloom = {"code":"CAR", "address":f"{LOCAL_IP}:{LOCAL_PORT}", "bf":saveBF(f"bf:localCAR:{timestamp}")}
            json_bloom = json.dumps(json_bloom)
            urls = [f"http://{ip[0]}:{ip[1]+1}" for ip in ips if ip[1] != LOCAL_PORT]
            rs = (grequests.post(u, data=json_bloom) for u in urls)
            grequests.map(rs)
        time.sleep(1)



if __name__ == '__main__':
    # lunch CA producer
    CAIsProducerThread = threading.Thread(target=CAIsProducer)
    CAIsProducerThread.start()

    # lunch Interests and CAIs handlers
    server_address = (LOCAL_IP, LOCAL_PORT+1) # if port for redis is 6379, the port for the proxy is 6380
    httpd = HTTPServer(server_address, ProxyPushBaedBFR)
    print('http server is running')
    httpd.serve_forever()



# AN OTHER IDEA:
# receive request, if this redis server can fulfill it than
# everything is okey. If not, we redirect the request to all
# redis neighbors telling them "client 'ip' sent this".
# Before all this, we check the nounce to verify that we did
# redirect or served this request before.
# Benifits: no iterests to store, and the way back is not
# necessarly the same, plus a gain in bandwidthn. But probably
# these problems are alredy solved with NC protocol
