#!/usr/bin/env python3

import asyncio
import async_timeout
import sys
import base64
import json
import time
import calendar
import threading
from redisbloom.client import Client
from redis.exceptions import ResponseError

# Local address
LOCAL_HOST = sys.argv[1]
LOCAL_PORT = int(sys.argv[2])

# local bloom standard name
LOCAL_BLOOM = "bf:localBF"

# local bloom old value
BLOOM_DUMP = [] # table of chunks
BLOOM_UP_TO_DATE = True # no new data

# RedisBloom Client, it will be used for results that we do not need to forward
redis_client = Client(host=LOCAL_HOST, port=LOCAL_PORT)
try:
    print("Loading Redis Bloom...")
    redis_client.execute_command('MODULE LOAD', '/etc/redis/redisbloom.so')
except ResponseError:
    print("Redis Bloom is already loaded !")
try:
    print("Initializing local BF...")
    redis_client.execute_command('BF.RESERVE', LOCAL_BLOOM, '0.01', '1000')
except ResponseError as e:
    print("Local BF is already initialized!")

# load neighbors ip addresses.
ips = list()
with open("ips.txt", "r") as f:
    for line in f:
        adr = line.strip()
        ip, port, db = adr.split(":")
        ips.append([ip, int(port), int(db)])
       
# FIB
# forme of entry: 'sourceID': {'currentNounce':nounce, 'nextHope':[ip, port], 'receivedNounces':set()}
FIB = dict()



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
        Push Based - simplified v
"""""""""""""""""""""""""""""""""""""""""""""

class PushBasedProxy:

    def __init__(self):
        self.connected = False

    async def connect(self, host=LOCAL_HOST, port=LOCAL_PORT):
        """
        Connect to a host
        """
        print(f"Connecting to {host}:{port} ...")
        await self.disconnect()
        self.r, self.w = await asyncio.open_connection(host=host, port=port)
        self.connected = True
        print(f"Connecting to {host}:{port} ...Done!")

    async def disconnect(self):
        """
        Disconnect from the current connection designed by self.w
        """
        if not self.connected:
            return
        print("Disconnecting proxy's connections...")
        self.w.close()
        await self.w.wait_closed()
        self.connected = False
        print("Disconnecting proxy's connections...Done!")


    async def connectTO(self, host=LOCAL_HOST, port=LOCAL_PORT, timeout=1):
        """
        Connect to a host and if the connection takes longer then timeout
        skip connection
        """
        print(f"Connecting to {host}:{port} with timeout {timeout} ...")
        await self.disconnect()
        async with async_timeout.timeout(timeout) as cm:
            print("got inside")
            self.r, self.w = await asyncio.open_connection(host=host, port=port)
            print("await responded")
            self.connected = True
        print(f"Connecting to {host}:{port} with timeout {timeout} ...Done!")
        #try:
        #    await asyncio.wait_for(self.connect(host, port), timeout=timeout)
        #except asyncio.TimeoutError:
        #    print(f"Timeout, skipping {host}:{port}")

    """""""""""""""""""""""""""""""""""""""""""""
                read operations
    """""""""""""""""""""""""""""""""""""""""""""

    async def _read_answer(self):
        """
        TODO
        """
        print("Reading reply...")
        ch = await self.r.read(1)
        bruteAnswer = ch
        if ch == b'$':
            tmp = await self._read_bluk()
            response = tmp[0]
            bruteAnswer += tmp[1]
        elif ch == b'+':
            tmp = await self._read_simple_string()
            response = tmp[0]
            bruteAnswer += tmp[1]
        elif ch == b'-':
            tmp = await self._read_simple_string()
            response = tmp[0].split(" ", 1)
            response = {"error":response[0], "msg":response[1] if len(response) > 1 else ""}
            bruteAnswer += tmp[1]
        elif ch == b':':
            tmp = await self._read_int()
            response = tmp[0]
            bruteAnswer += tmp[1]
        elif ch == b'*':
            tmp = await self._read_array()
            response = tmp[0]
            bruteAnswer += tmp[1]
        else:
            print("Reading error...")
            msg = await self.r.read(100)
            print("Reading error...Done!")
            raise Exception(f"Unknown tag: {ch}, msg: {msg}")
        print("Reading reply...Done")
        return response, bruteAnswer
            
    async def _read_int(self):
        print("Reading integer...")
        length = b''
        bruteAnswer = b''
        ch = b''
        while ch != b'\n':
            ch = await self.r.read(1)
            length += ch
            bruteAnswer += ch
        print("Reading integer...Done!")
        return int(length.decode()[:-1]), bruteAnswer

    async def _read_simple_string(self):
        print("Reading simple string...")
        response = b''
        bruteAnswer = b''
        ch = b''
        while ch != b'\n':
            ch = await self.r.read(1)
            response += ch
            bruteAnswer += ch
        print("Reading simple string...Done!")
        return response.decode()[:-1], bruteAnswer
    
    async def _read_bluk(self):
        print("Reading bulk...")
        length, bruteAnswer = await self._read_int()
        if length == -1:
            return None, bruteAnswer
        response = await self.r.read(length)
        bruteAnswer += response + b'\r\n'
        print("Reading bulk...Done!")
        return response.decode()[:-1], bruteAnswer

    async def _read_array(self):
        print("Reading array...")
        length, bruteAnswer = await self._read_int()
        response = []
        for _ in range(length):
            ch = await self.r.read(1)
            if ch == b'$':
                tmp = await self._read_bluk()
            elif ch == b':':
                tmp = await self._read_int()
            response.append(tmp[0])
            bruteAnswer += tmp[1]
        print("Reading array...Done!")
        return response, bruteAnswer


    """""""""""""""""""""""""""""""""""""""""""""
                query treatement
    """""""""""""""""""""""""""""""""""""""""""""

    async def _forward_query(self, query, host=LOCAL_HOST, port=LOCAL_PORT):
        """
        Sends the query as it is to redis server designed by 'host' and  'port'
        The return value is a list of 2 elements:
            1) the uniforme string core response
            2) the brute response recieved
        """
        print(f"Forwarding query to {host}:{port} ...")
        await self.connect(host, port)
        print(f"Writing '{query}' to redis...")
        self.w.write(query)
        await self.w.drain()
        print(f"Writing '{query}' to redis...Done!")
        response = await self._read_answer()
        print(f"Forwarding query to {host}:{port} ...Done!")
        return response

    async def _treate_get_query(self, params, query):
        """
        :param: params a list of parameters that were given to the GET query
                It has to be a list of only one string element
        """
        value, response = await self._forward_query(query)
        if value is None:
            # content is not in cache, so we will check if it is in
            # one of the neighbors (all the other caches are our
            # neighbors for the moment). 
            print("Content not in cache. Checking neighbors...")
            key = params[0]
            value, response = await self._checkFIBForContent(key, query)
            print("Content not in cache. Checking neighbors...Done!")
        else:
            print("Content found in cache...200 OK")
            # content is in cache, return it directly
        return response

    def _parse_query(self, query):
        """
        TODO
        """
        print("Parsing query...")
        query = query[:-2].decode().split()
        print("Parsing query...Done!")
        return {"command":query[0], "params":query[1:]}

    async def treate_query(self, query):
        """
        TODO
        """
        pquery = self._parse_query(query)
        command, params = pquery["command"], pquery["params"]
        if command == "GET":
            print("GET command detected. Dealing with it...")
            response = await self._treate_get_query(params, query)
            print("GET command detected. Dealing with it...Done!")
        else:
            print("No GET command detected. Forwarding...")
            response = (await self._forward_query(query))[1] # brute answer
            print("No GET command detected. Forwarding...Done!")
        return response

    async def treate_JSON(self, query):
        """
        TODO
        """
        print("Treating JSON query...")
        bloom_chunks = json.loads(query[1:-2].decode()) # query starts with 'J' and ends with '\r\n'
        source_host, source_port = str(bloom_chunks["nextHope"]).split(":")
        source_port = int(source_port)
        sourceID = bloom_chunks["sourceID"] 
        nounce = bloom_chunks["nounce"]
        bloom_chunks = bloom_chunks['bf']
        await self._populateFIB(sourceID, nounce, source_host, source_port, bloom_chunks)
        print("Treating JSON query...Done!")

    
    """""""""""""""""""""""""""""""""""""""""""""
                useful redis commands
    """""""""""""""""""""""""""""""""""""""""""""

    async def _get_query(self, key, host=LOCAL_HOST, port=LOCAL_PORT):
        """
        Implements a redis get or mget query. An mget query
        will be executed if key is not a string
        """
        if isinstance(key, str):
            response = await self._forward_query(f"GET {key}\r\n".encode(),host,port)
            print(f"GET {key} answered {response.decode()}")
            return response
        else:
            key = " ".join(key)
            response = await self._forward_query(f"MGET {key}\r\n".encode(),host,port)
            print(f"MGET {key} answered {response.decode()}")
            return response
        
    async def _bfExists_query(self, bfName, key, host=LOCAL_HOST, port=LOCAL_PORT):
        """
        Implements a redis get or mget query. An mget query
        will be executed if key is a list
        TODO Uncomplete
        """
        response = self._forward_query(f"BF.EXISTS {bfName} {key}\r\n".encode(),host,port)
        print(f"BF.EXISTS {key} answered {response.decode()}")
        return response.decode()[:-2] == "+(integer) 0" # not sure of this


    """""""""""""""""""""""""""""""""""""""""""""
               FIB and PIT management
    """""""""""""""""""""""""""""""""""""""""""""

    async def _checkFIBForContent(self, key, query):
        """
        Checks if the key is in the BF of one of the neighbors.
        If so, it sends a request to the neighbor to ask for the
        value. Otherwise, it returns the equevelent of null value,
        namely: (value=-1, response=b'$-1\r\n')
        """
        value, response = -1, b'$-1\r\n'
        for sourceID in FIB.keys():
            if redis_client.bfExists(f"bf:{sourceID}:{FIB[sourceID]['currentNounce']}", key):
                ip = FIB[sourceID]['nextHope']
                print(f"Content maybe at {ip[0]}:{ip[1]} .. Sending request...")
                # send request to redis ip
                value, response = await self._forward_query(query, host=ip[0], port=ip[1])
                if value != -1:
                    print(f"Content found at {ip[0]}:{ip[1]} !")
                    return value, response
                else:
                    print(f"Content not found at {ip[0]}:{ip[1]} !")
        print("Content not found...404")
        return value, response

    async def _populateFIB(self, sourceID, nounce, source_host, source_port, bloom_chunks):
        """
        TODO
        """
        if sourceID == f"{LOCAL_HOST}:{LOCAL_PORT}":
            print("Received my own advertisment. Droping it !")
            return
        print("Populating FIB...")
        if sourceID not in FIB.keys():
            FIB[sourceID] = {'currentNounce': nounce, 'nextHope':[source_host, source_port], 'receivedNounces':{nounce}}
        else:
            if nounce in FIB[sourceID]['receivedNounces']:
                print('duplicated nounce', nounce)
                return
            else:
                print('received nounce', nounce)
                print('old nounces', FIB[sourceID]['receivedNounces'])
                FIB[sourceID]['currentNounce'] = nounce
                FIB[sourceID]['receivedNounces'].add(nounce)
                FIB[sourceID]['nextHope'] = [source_host, source_port]
        restoreBF(bloom_chunks, f"bf:{sourceID}:{nounce}")
        print("Populating FIB...Done!")
        # forward CAI to neighbors, except the neighbor that sent us this CAI
        print(f"Forwarding FIB to neighboors...")
        await sendCAIs(sourceID, FIB[sourceID]['nextHope'], bloom_chunks)
        print(f"Forwarding FIB to neighbors...Done!")




"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                            CAIs and CARs producers
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

def saveBF(bloom=LOCAL_BLOOM):
    """
    dumps the local bloom. Multiple chunks are used in case
    the BF is too large to be SAVEd in one chunk
    """
    chunks = []
    itera = 0
    while True:
        try:
            itera, data = redis_client.bfScandump(bloom, itera)
        except ResponseError:
            print(f"Can not save bf {bloom}. Reserving BF {bloom}...")
        if itera == 0:
            return chunks
        else:
            chunks.append([itera, base64.b64encode(data).decode('ascii')])

def checkForNews():
    """
    checks if there is any new keys in redis. In this case,
    global variables BLOOM_UP_TO_DATE and BLOOM_DUMP are updated
    """
    print("Checking for new content...")
    global BLOOM_DUMP, BLOOM_UP_TO_DATE
    # iterate over keys and BF.ADD them to local BF
    # NB: This step wont be necessary when we'll have
    #     control over writes (BF.ADD after evry write)
    for key in redis_client.keys():
        if not (str(key).startswith("b'bf:") or str(key).startswith("b'ts:")):
            try:
                redis_client.bfAdd(LOCAL_BLOOM, key)
            except ResponseError:
                print("BF {} does not exist. Creating it...")
                redis_client.bfInsert(LOCAL_BLOOM, key, capacity=1000, error=0.01)
                print("Creating it...Done!")
    new_dump = saveBF()

    if (len(new_dump) != len(BLOOM_DUMP)) or any((new_dump[i][1] != BLOOM_DUMP[i][1] for i in range(len(new_dump)))):
            BLOOM_UP_TO_DATE = False
            BLOOM_DUMP = new_dump
    else:
        BLOOM_UP_TO_DATE = True
    print("Checking for new content...Done!")

def restoreBF(chunks, key):
    """
    restores the given chunks in redis under the given key name

    :param: chunks the data to restore in the BF
    :param: key the name to give to the BF
    """
    for chunk in chunks:
        itera, data = chunk
        redis_client.bfLoadChunk(key, itera, base64.b64decode(data))

# time to sleep between CAIs
SLEEP_TIME = 1 # 1 second

async def sendCAIs(sourceID=f"{LOCAL_HOST}:{LOCAL_PORT}", nextHope=[LOCAL_HOST, LOCAL_PORT], bf=[]):
    """
    Sends CAIs to neighboors with 'sourceID'=sourceID
    """
    print("Sending CAI to neighbors...")
    sleep_time = SLEEP_TIME
    bf = bf if len(bf) > 0 else BLOOM_DUMP
    json_bloom = {"code":"CAI", "sourceID":sourceID, "nounce":calendar.timegm(time.gmtime()), "nextHope":f"{LOCAL_HOST}:{LOCAL_PORT}", "bf":bf}
    json_bloom = json.dumps(json_bloom)
    proxy = PushBasedProxy()
    timeout = sleep_time / len(ips) if len(ips) != 0 else sleep_time
    for ip in ips:
        if ip[1] == LOCAL_PORT or ip[1] == nextHope[1]: # TODO add local_host also
            continue
        #asyncio.run(proxy.connectTO(host=ip[0], port=ip[1]+1, timeout=timeout))
        #async with async_timeout.timeout(timeout):
        #    await proxy.connect(host=ip[0], port=ip[1]+1)
        print(f"Sending CAI to {ip[0]}:{ip[1]}...")
        #await proxy.connectTO(host=ip[0], port=ip[1]+1, timeout=timeout)
        await proxy.connect(host=ip[0], port=ip[1]+1)
        if not proxy.connected:
            print("not connected")
            sleep_time -= timeout
            continue
        print("connected")
        json_bloom = b'J' + json_bloom.encode() + b'\r\n'
        print(f"Writing '{json_bloom}' to proxy {ip[0]}:{ip[1]+1}...")
        await write(proxy.w, json_bloom, True)
        print(f"Sending CAI to {ip[0]}:{ip[1]}...Done!")
    print("Sending CAI to neighbors...Done !")
    return sleep_time

async def CAIsProducer():
    """
    Checks every second if there is any new keys in redis,
    and sends update to neighbors if we found any
    """
    while True:
        checkForNews()
        #if not BLOOM_UP_TO_DATE:
        sleep_time = SLEEP_TIME
        if True:
            # send update to neighbors. We used grequests for multithreading
            sleep_time = await sendCAIs()
            print('sleep time', sleep_time)
        asyncio.sleep(sleep_time)



"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                                    SERVER
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

async def client_connected_cb(reader, writer):
    """
    TODO
    """
    proxy = PushBasedProxy()
    while True:
        print("Reading query...")
        query = await read(reader)
        if query[:-2] == b'close':
            print("Connection closed !")
            await proxy.disconnect()
            writer.close()
            await writer.wait_closed()
            return 
        elif query[0:1] == b'J':
            await proxy.treate_JSON(query)
        else:
            response = await proxy.treate_query(query)
            await write(writer, response)
        print("Reading query...Done!")
    

async def read(reader):
    """
    TODO
    """
    ch = b''
    query = b''
    while ch != b'\n':
        ch = await reader.read(1)
        query += ch
    return query

async def write(writer, data, closeAtEnd=False):
    """
    TODO
    """
    writer.write(data)
    await writer.drain()
    if closeAtEnd:
        writer.write(b'close\r\n')
        await writer.drain()
        writer.close()
        await writer.wait_closed()

def export_loop(coroutine):
    """
    TODO
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run_coroutine_threadsafe(coroutine, asyncio.get_event_loop())
    loop.run_forever()

async def server():
    """
    TODO
    """
    server = await asyncio.start_server(client_connected_cb, host=LOCAL_HOST, port=LOCAL_PORT+1)
    async with server:
        await server.serve_forever()

async def main():
    # lunch server
    print("Lunching server....")
    serverThread = threading.Thread(target=export_loop, args=(server(),))
    serverThread.start()

    # lunch CA producer
    print("Lunching CAIs Producer....")
    CAIsProducerThread = threading.Thread(target=export_loop, args=(CAIsProducer(),))
    CAIsProducerThread.start()

if __name__ == '__main__':
    asyncio.run(main())



