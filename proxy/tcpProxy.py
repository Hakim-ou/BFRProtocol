#!/usr/bin/env python3

import asyncio
import sys
from typing import Iterable
from collections.abc import Iterable
from redisbloom.client import Client

# Local address
LOCAL_HOST = sys.argv[1]
LOCAL_PORT = int(sys.argv[2])

# RedisBloom Client, it will be used for results that we do not need to forward
redis_client = Client(host=LOCAL_HOST, port=LOCAL_PORT)

# load neighbors ip addresses.
ips = list()
with open("ips.txt", "r") as f:
    for line in f:
        adr = line.strip()
        ip, port, db = adr.split(":")
        ips.append([ip, int(port), int(db)])



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

    async def connect(self, host=LOCAL_HOST, port=LOCAL_PORT):
        """
        Connect to redis
        """
        self.r, self.w = await asyncio.open_connection(host=host, port=port)

    """""""""""""""""""""""""""""""""""""""""""""
                read operations
    """""""""""""""""""""""""""""""""""""""""""""

    async def _read_answer(self):
        """
        TODO
        """
        ch = await self.r.read(1)
        bruteAnswer = ch
        if ch == b'$':
            tmp = await self._read_bluk()
            response = tmp[0]
            bruteAnswer += tmp[1]
            return response, bruteAnswer
        elif ch == b'+':
            tmp = await self._read_simple_string()
            response = tmp[0]
            bruteAnswer += tmp[1]
            return response, bruteAnswer
        elif ch == b'-':
            tmp = await self._read_simple_string()
            response = tmp[0].split(" ", 1)
            response = {"error":response[0], "msg":response[1] if len(response > 1) else ""}
            bruteAnswer += tmp[1]
            return response, bruteAnswer
        elif ch == b':':
            tmp = self._read_int()
            response = tmp[0]
            bruteAnswer += tmp[1]
            return response, bruteAnswer
        elif ch == b'*':
            tmp = await self._read_array()
            response = tmp[0]
            bruteAnswer += tmp[1]
            return response, bruteAnswer
        else:
            msg = await self.r.read(100)
            raise Exception(f"Unknown tag: {ch}, msg: {msg}")
            
    async def _read_int(self):
        length = b''
        bruteAnswer = b''
        while ch != b'\n':
            ch = await self.r.read(1)
            length += ch
            bruteAnswer += ch
        return int(length.decode()[:-1]), bruteAnswer

    async def _read_simple_string(self):
        response = b''
        bruteAnswer = b''
        while ch != b'\n':
            ch = await self.r.read(1)
            response += ch
            bruteAnswer += ch
        return response.decode()[:-1], bruteAnswer
    
    async def _read_bluk(self):
        length, bruteAnswer = self._read_int()
        if length == -1:
            ch = await self.r.read(2)
            bruteAnswer += ch
            return None, bruteAnswer
        response = await self.r.read(length)
        bruteAnswer += response + b'\r\n'
        return response.decode()[:-1], bruteAnswer

    async def _read_array(self):
        length, bruteAnswer = self._read_int()
        response = []
        for _ in range(length):
            ch = await self.r.read(1)
            if ch == b'$':
                tmp = self._read_bluk()
            elif ch == b':':
                tmp = self._read_int()
            response.append(tmp[0])
            bruteAnswer += tmp[1]
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
        self.connect(host, port)
        self.w.write(query)
        await self.w.drain()
        response = await self._read_answer()
        return response

    async def _treate_get_query(self, params, query):
        """
        :param: params a list of parameters that were given to the GET query
                It has to be a list of only one string element
        """
        value, response = self._forward_query(query)
        if value is None:
            # content is not in cache, so we will check if it is in
            # one of the neighbors (all the other caches are our
            # neighbors for the moment). 
            print("Content not in cache. Checking neighbors...")
            key = params[0]
            value, response = self._checkFIBForContent(key, query)
        else:
            print("Content found in cache...200 OK")
            # content is in cache, return it directly
        return response

    def _parse_query(self, query):
        """
        TODO
        """
        query = query.decode().split()
        return {"command":query[0], "params":query[1:]}

    async def treate_query(self, query):
        """
        TODO
        """
        pquery = self._parse_query(query)
        command, params = pquery["command"], pquery["params"]
        if command == "GET":
            response = await self._treate_get_query(params, query)
        else:
            response = await self._forward_query(query)[1] # brute answer
        return response

    
    """""""""""""""""""""""""""""""""""""""""""""
                useful redis commands
    """""""""""""""""""""""""""""""""""""""""""""

    async def _get_query(self, key, host=LOCAL_HOST, port=LOCAL_PORT):
        """
        Implements a redis get or mget query. An mget query
        will be executed if key is not a string
        """
        if isinstance(key, str):
            response = self._forward_query(f"GET {key}\r\n".encode(),host,port)
            print(f"GET {key} answered {response.decode()}")
            return response
        else:
            key = " ".join(key)
            response = self._forward_query(f"MGET {key}\r\n".encode(),host,port)
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
        found = False
        # timestamps = self._get_query((f"ts:{ip[0]}:{ip[1]}" for ip in ips))
        timestamps = redis_client.mget((f"ts:{ip[0]}:{ip[1]}" for ip in ips))
        value, response = -1, b'$-1\r\n'
        for ip, timestamp in zip(ips, timestamps):
            #timestamp = redis_client.get(f"ts:{ip[0]}:{ip[1]}").decode('utf-8')
            if timestamp is None:
                continue
            timestamp = timestamp.decode('utf-8')
            if redis_client.bfExists(f"bf:{ip[0]}:{ip[1]}:{timestamp}", key):
                found = True
                print(f"Content maybe at {ip[0]}:{ip[1]} .. Sending request...")
                # send request to redis ip (standard port 6379)
                value, response = await self._forward_query(query, host=ip[0], port=ip[1])
                if value != -1:
                    return value, response
                else:
                    print(f"Content not found at {ip[0]}:{ip[1]} !")
        if not found:
            print("Content not found...404")
            return value, response
            #raise ContentNotFound()



"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                                    SERVER
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

async def client_connected_cb(reader, writer):
    """
    TODO
    """
    proxy = PushBasedProxy()
    proxy.connect()
    query = await read(reader)
    response = await proxy.treate_query(query)
    await write(writer, response)
    

async def read(reader):
    """
    TODO
    """
    ch = b''
    query = b''
    while ch != b'\n':
        ch = await reader.read(1)
        query += ch
    return query[:-1]

async def write(writer, data):
    """
    TODO
    """
    writer.write(data)
    await writer.drain()

async def main():
    print("Lunching server....")
    await asyncio.start_server(client_connected_cb, host=LOCAL_HOST, port=LOCAL_PORT+1)

if __name__ == '__main__':
    asyncio.run(main())



