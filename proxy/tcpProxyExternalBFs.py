#!/usr/bin/env python3

import asyncio
from asyncio import exceptions
from random import shuffle
import sys, os
import json
import time
import calendar
import threading
from multiprocessing import Process, Manager
import redis
from probables import BloomFilter
from collections import deque

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                                    Configuration
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

MANAGER = Manager()

# Local address
#LOCAL_HOST = sys.argv[1]
#LOCAL_PORT = int(sys.argv[2])
LOCAL_HOST = os.environ['LOCAL_HOST']
LOCAL_PORT = int(os.environ['LOCAL_PORT'])
REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = int(os.environ['REDIS_PORT'])

# local bloom old value
BLOOM_UP_TO_DATE = True # no new data

# initializing localBF
FALSE_RATE = 0.01
CAPACITY = 3000
LOCAL_BLOOM = BloomFilter(est_elements=CAPACITY, false_positive_rate=FALSE_RATE)
# in here we will store the bfs that we received from neighbors so we're going to forward
# them during our next CA
BFs_TO_SHARE = MANAGER.dict()

MAX_BFs_PER_NODE = 100
# FIB
# forme of entry: 'sourceID': {'nextHope':[ip, port], 'receivedNounces':deque([], MAX_BFs_PER_NODE), 'bfs':deque([], MAX_BFs_PER_NODE)}
FIB = dict()

# load neighbors ip addresses.
ips = list()
#with open("ips.txt", "r") as f:
#    for line in f:
#        adr = line.strip()
#        ip, port, db = adr.split(":")
#        ips.append([ip, int(port), int(db)])
STANDARD_PORT = 8080
STANDARD_BD = 1
for i in range(int(os.environ['NB_NEIGHBORS'])):
    ips.append([os.environ[f'NEIGHBOR{i+1}'], STANDARD_PORT, STANDARD_BD])

# this redis client will be used to check if there is anything new in the redis server
# it is not necessary, we can use the client provided by this proxy, but it is easier
# and just as performant like that
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
#POOL = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT)

# time to sleep between CAIs
SLEEP_TIME = 5 # TODO change to 1 second

# connections
connections = dict()
CAIsConnections = dict()
connectionLocks = dict()
for ip in ips:
    connections[f"{ip[0]}:{ip[1]}"] = None
    CAIsConnections[f"{ip[0]}:{ip[1]}"] = None # will be used by the thread producing CAIs, don't need any locks then
    connectionLocks[f"{ip[0]}:{ip[1]}"] = None


"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                                    Strategies
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""


"""""""""""""""""""""""""""""""""""""""""""""
                Exceptions
"""""""""""""""""""""""""""""""""""""""""""""

class ContentNotFound(Exception):
    def __init__(self):
        super().__init__("Content Not found")

"""""""""""""""""""""""""""""""""""""""""""""
                Proxy
"""""""""""""""""""""""""""""""""""""""""""""

class Proxy:

    def __init__(self):
        """
        Initialize the proxy to not being connected to any other host (not even redis server)
        """
        self.connectedTo = dict()
        self.CAIConnectedTo = dict()
        for ip in ips:
            self.connectedTo[f"{ip[0]}:{ip[1]}"] = False
            self.CAIConnectedTo[f"{ip[0]}:{ip[1]}"] = False
        self.connectedToRedis = False
        self.redisR, self.redisW = None, None
            

    async def connect(self, host=REDIS_HOST, port=REDIS_PORT, task=None, CAI=False):
        """
        Connect to a host (another node or redis server)
        """
        if CAI:
            # TODO check if connection is still open (try: write("test"))
            print(f"Connecting to {host}:{port} (CAI)...")
            if CAIsConnections[f"{host}:{port}"] == None:
                CAIsConnections[f"{host}:{port}"] = await asyncio.open_connection(host=host, port=port)
                print(f"Connection with {host}:{port} (CAI) created!")
                ip = f"I{LOCAL_HOST}:{LOCAL_PORT}\r\n"
                write(CAIsConnections[f"{host}:{port}"][1], ip.encode())
                print("Identification complete (CAI)!")
            self.CAIConnectedTo[f"{host}:{port}"] = True
            print(f"Connecting to {host}:{port} (CAI)...Done!")
        else:
            print(f"Connecting to {host}:{port} ...")
            await connectionLocks[f"{host}:{port}"].acquire()
            print(f"Lock acquired for {host}:{port}!")
            # TODO check if connection is still open (try: write("test"))
            if connections[f"{host}:{port}"] == None:
                connections[f"{host}:{port}"] = await asyncio.open_connection(host=host, port=port)
                print(f"Connection with {host}:{port} created!")
                ip = f"I{LOCAL_HOST}:{LOCAL_PORT}\r\n"
                write(connections[f"{host}:{port}"][1], ip.encode())
                print("Identification complete!")
            self.connectedTo[f"{host}:{port}"] = True
            print(f"Connecting to {host}:{port} ...Done!")
        if task is not None:
            task.cancel()

    async def disconnect(self, host=REDIS_HOST, port=REDIS_PORT, CAI=False):
        """
        Disconnect from the current connection designed by self.w
        """
        if CAI:
            print(f"Disconnecting from {host}:{port} (CAI)...")
            if self.CAIConnectedTo[f"{host}:{port}"]:
                self.CAIConnectedTo[f"{host}:{port}"] = False
            print(f"Disconnecting from {host}:{port} (CAI)...Done!")
        else:
            print(f"Disconnecting from {host}:{port}...")
            if self.connectedTo[f"{host}:{port}"]:
                self.connectedTo[f"{host}:{port}"] = False
                connectionLocks[f"{host}:{port}"].release()
                print(f"Lock released for {host}:{port}!")
            print(f"Disconnecting from {host}:{port}...Done!")


    async def connectTO(self, host=REDIS_HOST, port=REDIS_PORT, timeout=1, CAI=False):
        """
        Connect to a host and if the connection takes longer then timeout
        skip connection
        """
        print(f"Connecting to {host}:{port} with timeout {timeout} ...")
        #await self.disconnect()
        timing = asyncio.create_task(asyncio.sleep(timeout))
        connection = asyncio.create_task(self.connect(host, port, timing,CAI))
        try:
            await timing
            if not timing.cancelled():
                print("Time's up! Canceling connection...")
                connection.cancel()
                if not CAI:
                    connectionLocks[f"{host}:{port}"].release()
                print("Canceling connection...Done!")
        except asyncio.CancelledError:
            print(f"Connecting to {host}:{port} with timeout {timeout} ...Done!")

    async def connectToRedis(self):
        """
        Preseve a single connection to redis for the totality of the communication
        """
        print(f"Connecting to redis ...")
        self.redisR, self.redisW = await asyncio.open_connection(host=REDIS_HOST, port=REDIS_PORT)
        self.connectedToRedis = True
        print(f"Connecting to redis...Done!")

    async def disconnectRedis(self):
        """
        Close connection with redis
        """
        print("Disconnecting from redis...")
        if self.redisW is not None:
            self.redisW.close()
            await self.redisW.wait_closed()
            self.connected = False
            self.redisW, self.redisR = None, None
        print("Disconnecting from redis...Done!")
        
        

    """""""""""""""""""""""""""""""""""""""""""""
                redis read operations
    """""""""""""""""""""""""""""""""""""""""""""

    async def _read_redis_answer(self, reader):
        """
        This function allow us to read answers from redis, respecting the
        RESP protocole
        """
        print("Reading reply...")
        ch = await reader.read(1)
        bruteAnswer = ch
        if ch == b'$':
            tmp = await self._read_bluk(reader)
            response = tmp[0]
            bruteAnswer += tmp[1]
        elif ch == b'+':
            tmp = await self._read_simple_string(reader)
            response = tmp[0]
            bruteAnswer += tmp[1]
        elif ch == b'-':
            tmp = await self._read_simple_string(reader)
            response = tmp[0].split(" ", 1)
            response = {"error":response[0], "msg":response[1] if len(response) > 1 else ""}
            bruteAnswer += tmp[1]
        elif ch == b':':
            tmp = await self._read_int(reader)
            response = tmp[0]
            bruteAnswer += tmp[1]
        elif ch == b'*':
            tmp = await self._read_array(reader)
            response = tmp[0]
            bruteAnswer += tmp[1]
        else:
            # we get here if the received message has nothing to do with
            # the RESP protocol  
            print("Reading error...")
            msg = await reader.read(100)
            print("Reading error...Done!")
            raise Exception(f"Unknown tag: {ch}, msg: {msg}")
        print("Reading reply...Done!")
        return response, bruteAnswer
            
    async def _read_int(self, reader):
        print("Reading integer...")
        length = b''
        bruteAnswer = b''
        ch = b''
        while ch != b'\n':
            ch = await reader.read(1)
            length += ch
            bruteAnswer += ch
        print("Reading integer...Done!")
        return int(length.decode()[:-1]), bruteAnswer

    async def _read_simple_string(self, reader):
        print("Reading simple string...")
        response = b''
        bruteAnswer = b''
        ch = b''
        while ch != b'\n':
            ch = await reader.read(1)
            response += ch
            bruteAnswer += ch
        print("Reading simple string...Done!")
        return response.decode()[:-1], bruteAnswer
    
    async def _read_bluk(self, reader):
        print("Reading bulk...")
        length, bruteAnswer = await self._read_int(reader)
        if length == -1:
            print("Reading bulk...Done! (-1)")
            return None, bruteAnswer
        response = await reader.read(length)
        ctrl = await reader.read(2)
        bruteAnswer += response + ctrl
        print("Reading bulk...Done!")
        return response.decode(), bruteAnswer

    async def _read_array(self, reader):
        print("Reading array...")
        length, bruteAnswer = await self._read_int(reader)
        response = []
        for _ in range(length):
            ch = await reader.read(1)
            if ch == b'$':
                tmp = await self._read_bluk(reader)
            elif ch == b':':
                tmp = await self._read_int(reader)
            response.append(tmp[0])
            bruteAnswer += tmp[1]
        print("Reading array...Done!")
        return response, bruteAnswer


    """""""""""""""""""""""""""""""""""""""""""""
                query treatement
    """""""""""""""""""""""""""""""""""""""""""""

    async def _forward_query(self, query, host=REDIS_HOST, port=REDIS_PORT):
        """
        Sends the query as it is to redis server designed by 'host' and  'port'
        The return value is a list of 2 elements:
            1) the uniforme string core response or a list of strings if answer 
               is an array
            2) the brute response recieved

        :param query: the query to forward to redis 
        :param host: redis host to address
        :param port: redis port to address
        """
        if host == REDIS_HOST:
            print(f"Forwarding query to redis...")
            print(f"Writing '{query}' to redis...")
            self.redisW.write(query)
            await self.redisW.drain()
            print(f"Writing '{query}' to redis...Done!")
            response = await self._read_redis_answer(self.redisR)
            print(f"Forwarding query to redis...Done!")
        else:
            print(f"Forwarding query to {host}:{port} ...")
            await self.connect(host, port)
            print(f"Writing '{query}' to {host}:{port}...")
            reader, writer = connections[f"{host}:{port}"]
            writer.write(query)
            await writer.drain()
            print(f"Writing '{query}' to {host}:{port}...Done!")
            response = await self._read_redis_answer(reader)
            await self.disconnect(host, port)
            print(f"Forwarding query to {host}:{port} ...Done!")
        return response

    async def _treate_get_query(self, params, query, ip=[]):
        """
        Get queries are the ones to treat specially. We need to see
        if we have the information, and if not we will do something
        with the query according to the used strategy
        NB: The current (default) strategy is Push-Based BFR, so we
            will check neighbors, and if we do not find the requested
            content we will respond immediately by redis "Not Found"
            error (that's a bulk "$-1\r\n")

        :param params: a list of parameters that were given to the GET query
                It has to be a list of only one string element
        :param query: the brute GET query
        """
        value, response = await self._forward_query(query)
        if value is None:
            # content is not in cache, so we will check if it is in
            # one of the neighbors (all the other caches are our
            # neighbors for the moment). 
            print("Content not in cache. Checking neighbors...")
            key = params[0]
            value, response = await self._checkFIBForContent(key, query, ip)
            print("Content not in cache. Checking neighbors...Done!")
        else:
            print("Content found in cache...200 OK")
            # content is in cache, return it directly
        return response

    def _parse_query(self, query):
        """
        This retreives the command and arguments from the redis
        query. These infos will be used to treat the query

        :param query: the received query
        """
        print("Parsing query...")
        query = query[:-2].decode().split() # query ends with b'\r\n'
        print("Parsing query...Done!")
        return {"command":query[0], "params":query[1:]} # TODO this does not support commands like
                                                        # 'MODULE LOAD' (but they are not GET-like
                                                        # commands, so no problem

    async def treate_query(self, query, ip=[]):
        """
        This function will treat all redis queries, decide
        whether to treat it specially or forward it directlly
        toward redis

        :param query: the received query
        """
        pquery = self._parse_query(query)
        command, params = pquery["command"], pquery["params"]
        if command == "GET":
            print("GET command detected. Dealing with it...")
            response = await self._treate_get_query(params, query, ip)
            print("GET command detected. Dealing with it...Done!")
        else:
            print("No GET command detected. Forwarding...")
            response = (await self._forward_query(query))[1] # brute answer
            print("No GET command detected. Forwarding...Done!")
        return response

    async def treate_JSON(self, query):
        """
        This function treats all JSON queries and decide on their nature
        (CAI, CAR, ..) and treats them correspondlly. The nature of the
        query is specified by the "code" field of the received query
        NB: For now we only treat querie as CAI querie

        :param query: the received query
        """
        print("Treating JSON query...")
        msg = json.loads(query[1:-2].decode()) # query starts with 'J' and ends with '\r\n'
        source_host, source_port = str(msg["nextHope"]).split(":")
        source_port = int(source_port) # port numbers are always integers
        sourceID = msg["sourceID"] 
        nounce = msg["nounce"]
        bf_string = msg['bf']
        # populate FIB and forward to neighbors
        await self._populateFIB(sourceID, nounce, source_host, source_port, bf_string)
        print("Treating JSON query...Done!")

    
    """""""""""""""""""""""""""""""""""""""""""""
                Redis Local Client
    Provides some useful redis commands, so we wont
    need to use any external bibliotheque (that we don't know
    exactly how does it work)
    TODO Incomplete
    """""""""""""""""""""""""""""""""""""""""""""

    async def _get_query(self, key, host=REDIS_HOST, port=REDIS_PORT):
        """
        Implements a redis get or mget query. An mget query
        will be executed if key is not a string

        :param key: the key to GET
        :param host: redis host to query
        :param port: redis port to query
        """
        if isinstance(key, str):
            # if key is string send GET query
            response = await self._forward_query(f"GET {key}\r\n".encode(),host,port)
            print(f"GET {key} answered {response.decode()}")
            return response
        else:
            # if key is NOT string send MGET query
            key = " ".join(key)
            response = await self._forward_query(f"MGET {key}\r\n".encode(),host,port)
            print(f"MGET {key} answered {response.decode()}")
            return response
        
    async def _bfExists_query(self, bfName, key, host=REDIS_HOST, port=REDIS_PORT):
        """
        Implements a redis get or mget query. An mget query
        will be executed if key is a list
        TODO Incomplete
        """
        response = self._forward_query(f"BF.EXISTS {bfName} {key}\r\n".encode(),host,port)
        print(f"BF.EXISTS {key} answered {response.decode()}")
        return response.decode()[:-2] == "+(integer) 0" # not sure of this


    """""""""""""""""""""""""""""""""""""""""""""
               FIB and PIT management
    """""""""""""""""""""""""""""""""""""""""""""

    async def _checkFIBForContent(self, key, query, requester=[]):
        """
        Checks if the key is in the BF of one of the neighbors.
        If so, it sends a request to the neighbor to ask for the
        value. Otherwise, it returns the equevelent of null value,
        namely: (value=-1, response=b'$-1\r\n')

        :param key: the key to check neighbors for
        :param query: the received query
        """
        # the default return value is the standard redis "Not Found" msg
        value, response = None, b'$-1\r\n'
        # we iterate over the known sources from wich we received
        # an advertisment before
        for sourceID in FIB.keys():
            ip = FIB[sourceID]['nextHope']
            # don't forward the request to the requester
            if ip == requester:
                continue
            # check if the node designated with 'sourceID' might have the information 
            for bf in FIB[sourceID]['bfs']:
                if bf.check(key):
                    # if so, forward the request to the next hope
                    print(f"Content maybe at {ip[0]}:{ip[1]} .. Sending request...")
                    # TODO we would like to remove the await here, we ask for the content
                    # and with the first true positive we receive we cancel all the other requests
                    value, response = await self._forward_query(query, host=ip[0], port=ip[1])
                    if value is not None:
                        # if the bf hit was a true positive return the value and stop there
                        print(f"Content found at {ip[0]}:{ip[1]} !")
                        return value, response
                    else:
                        # if the bf hit was a false positive log that and continue
                        # to check the other nodes
                        print(f"Content not found at {ip[0]}:{ip[1]} !")
                        break
            print(f"negative for {ip[0]}:{ip[1]}")
        print("Content not found...404")
        return value, response

    async def _populateFIB(self, sourceID, nounce, source_host, source_port, bf_string):
        """
        Populates the FIB dictionnary with bloom filter corresponding to
        the given bf_string. We also forward this bf_string to the neighbors
        except the neighbor from which we received this bf (and ourselves ofcourse).
        See function 'sendCAIs' for more details

        :param sourceID: The ID of the node that produced this CAI
        :param nounce: The nounce of this CAI. It makes ot possible to avoid duplication
        :param source_host: The next hope name
        :param source_port: The next hope port
        :bf_string: The string representation of the CAI bf
        """
        # if we are the source of this CAI we will just ignore it
        if sourceID == f"{LOCAL_HOST}:{LOCAL_PORT}":
            print("Received my own advertisement. Droping it !")
            return
        print("Populating FIB...")
        if sourceID not in FIB.keys():
            # This is the first time we receive a CAI from the node identified with sourceID
            # so we need to create a new set of already recieved nounces
            FIB[sourceID] = {'nextHope':[source_host, source_port], 'receivedNounces':deque([nounce], MAX_BFs_PER_NODE), 'bfs':deque([], MAX_BFs_PER_NODE)}
        else:
            # We already have an entry for this sourceID, so we have to check if this
            # is a new advertisement or it is just a cercular forwarding
            if nounce in FIB[sourceID]['receivedNounces']:
                # if it is a cercular forwarding we will just drop it
                print('Duplicated nounce. Droping advertisement!', nounce)
                return
            else:
                # if this is a new advertisement we will replace the old one with
                # this one and we will add the nounce to the set of already received
                # nounces.
                FIB[sourceID]['receivedNounces'].appendleft(nounce)
                FIB[sourceID]['nextHope'] = [source_host, source_port]
        # reconstruct BF from bf_string and store it in the right place
        restoreBF(bf_string, sourceID)
        print("Populating FIB...Done!")
        # forward CAI to neighbors, except the neighbor that sent us this CAI
        # print(f"Forwarding FIB to neighboors...")
        # await sendCAIs(sourceID, FIB[sourceID]['nextHope'], bf_string)
        # print(f"Forwarding FIB to neighbors...Done!")
        # add the bfs we received to the "TO_BE_FORWARDED" set
        addCAIs(sourceID, FIB[sourceID]['nextHope'], bf_string)




"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                            CAIs and CARs producers
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
def addCAIs(sourceID, nextHope, bf_string):
    """
    TODO control concurrent access
    """
    bf = BloomFilter(hex_string=bf_string)
    nb_elements = bf.estimate_elements() 
    if nb_elements < 3000:
        BFs_TO_SHARE[sourceID] = {'nextHope':nextHope, 'nb_elements':nb_elements, 'bf':BloomFilter(hex_string=bf_string)}
    print(BFs_TO_SHARE)

def chooseContent(nb_content=CAPACITY):
    """
    Chooses randomlly 'CAPACITY' content from the set of contents
    we possess and loads them into 'LOCAL_BLOOM' after clearing it.
    If the number of contents we possess is less than 'CAPACITY' then
    all the content is added to the 'LOCAL_BLOOM'. In this case, it's
    very likely that the 'LOCAL_BLOOM' wont change, so we will set
    'BLOOM_UP_TO_DATE' to date to true in order not to republish the
    same content.
    """
    print("Choosing content to advertise...")
    global BLOOM_UP_TO_DATE, LOCAL_BLOOM
    tmp = BloomFilter(est_elements=CAPACITY, false_positive_rate=FALSE_RATE)
    #redis_client = redis.StrictRedis(connection_pool=POOL)
    keys = list(redis_client.keys())
    shuffle(keys)
    for i in range(min(len(keys), nb_content)):
        tmp.add(keys[i].decode())
    # check if there is a change compared to the old local bloom
    if not LOCAL_BLOOM.jaccard_index(tmp) == 1:
        LOCAL_BLOOM = tmp
        BLOOM_UP_TO_DATE = False
    else:
        BLOOM_UP_TO_DATE = True
    print("Choosing content to advertise...Done!")

def restoreBF(bf_string, sourceID):
    """
    restores the bloom filter from the given hexadecimal
    string representation and adds it to sourceID's BFs deque

    :param: bf_string hexa decimal string representation
    :param: sourceID the source of this bf
    """
    FIB[sourceID]['bfs'].appendleft(BloomFilter(hex_string=bf_string))

async def sendCAIs(sourceID=f"{LOCAL_HOST}:{LOCAL_PORT}", exceptions=set(), bf=None):
    """
    Sends CAIs to neighboors with 'sourceID'=sourceID

    :param sourceID: the source of the CAI. By default it is the current node, but if we are
            forwarding a CAI it will be different
    :param exceptions: the neighbors to exclude from this advertissement (for example the 
            neighbor from which we received this CAI)
    :param bf: the CAI's bloom filter
    """
    print("Sending CAI to neighbors...")
    sleep_time = SLEEP_TIME
    # by default the CAI's bf is the bloom filter of our local content
    bf = bf if bf is not None else LOCAL_BLOOM.export_hex()
    # we construct the json msg that will be sent
    json_bloom = {"code":"CAI", "sourceID":sourceID, "nounce":calendar.timegm(time.gmtime()), "nextHope":f"{LOCAL_HOST}:{LOCAL_PORT}", "bf":bf}
    json_bloom = json.dumps(json_bloom)
    json_bloom = b'J' + json_bloom.encode() + b'\r\n'
    # we instanciate a Proxy to use its communication features to communicate
    # with the neighbors
    proxy = Proxy()
    # we define a timeout for connection equal to the 'SLEEP_TIME' devided
    # by the number of neighbors we have, because we entend to reduce this
    # time from the sleep time if the connection fails
    exceptions.add(f"{LOCAL_HOST}:{LOCAL_PORT}")
    timeout = sleep_time / len(ips) if len(ips) != 0 else sleep_time
    for ip in ips:
        if f"{ip[0]}:{ip[1]}" in exceptions:
            # avoid sending the CAI to ourselves or to the neighbor that
            # forwarded this information to us
            continue
        print(f"Sending CAI to {ip[0]}:{ip[1]}...")
        # connect with timeout to the node designated with 'ip'
        await proxy.connectTO(host=ip[0], port=ip[1], timeout=timeout, CAI=True)
        if not proxy.CAIConnectedTo[f"{ip[0]}:{ip[1]}"]:
            print("not connected")
            # reduce the time taken by the failed connection from sleep time
            # if we fail all connections we will start right away, because we already
            # weighted that time during connections
            sleep_time -= timeout
            continue
        print("connected")
        # format the msg so we can identify that it is a JSON msg
        print(f"Writing '{json_bloom}' to proxy {ip[0]}:{ip[1]}...")
        # wait for the write to complete
        await write(CAIsConnections[f"{ip[0]}:{ip[1]}"][1], json_bloom)
        await proxy.disconnect(host=ip[0], port=ip[1], CAI=True)
        print(f"Sending CAI to {ip[0]}:{ip[1]}...Done!")
    print("Sending CAI to neighbors...Done !")
    return sleep_time

async def CAIsProducer():
    """
    Checks every second if there is any new keys in redis,
    and sends update to neighbors if we found any
    """
    while True:
        sleep_time = SLEEP_TIME
        # choose content from our local redis server to advertise
        exceptions = set()
        for source in BFs_TO_SHARE.keys():
            print(f"Sharing {source} content...")
            nextHope = BFs_TO_SHARE[source]['nextHope']
            exceptions.add(f"{nextHope[0]}:{nextHope[1]}")
            chooseContent(min(int(CAPACITY/3), CAPACITY - BFs_TO_SHARE[source]['nb_elements']))
            if BLOOM_UP_TO_DATE:
                bf = BFs_TO_SHARE[source]['bf']
            else:
                bf = LOCAL_BLOOM.union(BFs_TO_SHARE[source]['bf'])
            await sendCAIs(sourceID=source, exceptions={f"{nextHope[0]}:{nextHope[1]}"}, bf=bf.export_hex())
            print(f"Sharing {source} content...Done!")
        
        print("Sharing local content...")
        chooseContent(int(CAPACITY/3))
        #await sendCAIs(exceptions=exceptions)
        if not BLOOM_UP_TO_DATE:
            await sendCAIs()
        print("Sharing local content...Done!")
        print(f"Going to sleep for {sleep_time} seconds")
        await asyncio.sleep(sleep_time)

def run_CAIsProducer():
    asyncio.run(CAIsProducer())


"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                                    SERVER
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""


async def client_connected_cb(reader, writer):
    """
    This is the callback that will treat connections to our
    proxy. It reads the msg from the node that asked for the connection
    and treats it as:
        1) a 'close connection' msg if 'close' is received
        2) an advertisement if a JSON msg is received
        3) a redis query otherwise

    :param reader: the reader of the connection
    :param writer: the writer of the connection
    """
    print("New connection received!")
    proxy = Proxy()
    await proxy.connectToRedis()
    ip = []
    while True:
        print("Reading query...")
        query = await read(reader)
        if query[:-2] == b'close':
            print("Connection closed !")
            print("Reading query...Done!")
            break 
        elif query[0:1] == b'J':
            await proxy.treate_JSON(query)
        elif query[0:1] == b'I':
            ip = query[1,-2].decode().split(":")
        else:
            response = await proxy.treate_query(query, ip)
            await write(writer, response)
        print("Reading query...Done!")
    await proxy.disconnectRedis()
    

async def read(reader):
    """
    Reads from the 'reader' until the first '\n'

    :param reader: a reader from a connection
    """
    ch = b''
    query = b''
    while ch != b'\n':
        ch = await reader.read(1)
        query += ch
    return query

async def write(writer, data):
    """
    Writes 'data' to the given 'writer' and close the
    connection at the end if 'closeAtEnd' was set to 'True'

    :param writer: a writer from a connection
    :param data: the data to write to the 'writer'
    :param closeAtEnd: ask to close connection at the end
    """
    writer.write(data)
    await writer.drain()

def export_loop(coroutine):
    """
    [DEPRICATED]
    lunches the given 'coroutine' on an other thread
    with a new loop

    :param coroutine: the coroutine to lunch
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run_coroutine_threadsafe(coroutine, asyncio.get_event_loop())
    asyncio.run(coroutine)
    loop.run_forever()

async def server():
    """
    A coroutine that lunches the proxy server
    """
    for ip in ips:
        connectionLocks[f"{ip[0]}:{ip[1]}"] = asyncio.Lock()
    server = await asyncio.start_server(client_connected_cb, host=LOCAL_HOST, port=LOCAL_PORT)
    async with server:
        await server.serve_forever()

def run_server():
    asyncio.run(server())

def main():
    """
    The proxy's entry point
    """
    # lunch server
    print("Lunching server....")
    #serverProcess = Process(target=export_loop, args=(server(),))
    serverProcess = Process(target=run_server)
    serverProcess.start()

    # lunch CA producer
    print("Lunching CAIs Producer....")
    #CAIsProducerProcess = Process(target=export_loop, args=(CAIsProducer(),))
    CAIsProducerProcess = Process(target=run_CAIsProducer)
    CAIsProducerProcess.start()

    serverProcess.join()
    CAIsProducerProcess.join()

if __name__ == '__main__':
    #asyncio.run(main())
    main()