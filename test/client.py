import asyncio

class Client():
	
    def __init__(self, host, port):
        """
        Initialize the client to not being connected to any other proxy
        """
        self.host = host
        self.port = port
        self.connected = False

    async def connect(self, task=None):
        """
        Connect to the proxy
        """
        print(f"Connecting to proxy {self.host}:{self.port} ...")
        self.r, self.w = await asyncio.open_connection(host=self.host, port=self.port)
        self.connected = True
        if task is not None:
            task.cancel()
        print(f"Connecting to {self.host}:{self.port} ...Done!")

    async def disconnect(self):
        """
        Disconnect from the current connection designed by self.w
        """
        if not self.connected:
            return
        print("Disconnecting from proxy...")
        self.w.close()
        await self.w.wait_closed()
        self.connected = False
        print("Disconnecting from proxy")


    async def connectTO(self, timeout=1):
        """
        Connect to a host and if the connection takes longer then timeout
        skip connection
        """
        host, port = self.host, self.port
        print(f"Connecting to proxy {host}:{port} with timeout {timeout} ...")
        timing = asyncio.create_task(asyncio.sleep(timeout))
        connection = asyncio.create_task(self.connect(host, port, timing))
        try:
            await timing
            if not timing.cancelled():
                print("Time's up! Canceling connection...")
                connection.cancel()
                print("Canceling connection...Done!")
        except asyncio.CancelledError:
            print(f"Connecting to proxy {host}:{port} with timeout {timeout} ...Done!")

    """""""""""""""""""""""""""""""""""""""""""""
                RESP protocol
    """""""""""""""""""""""""""""""""""""""""""""

    async def _read_redis_answer(self):
        """
        This function allow us to read answers from redis, respecting the
        RESP protocole
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
            # we get here if the received message has nothing to do with
            # the RESP protocol  
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
        ctrl = await self.r.read(2)
        bruteAnswer += response + ctrl
        print("Reading bulk...Done!")
        return response.decode(), bruteAnswer

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
                redis queries
    """""""""""""""""""""""""""""""""""""""""""""

    async def _send_query(self, query):
        print(f"Writing '{query}' to redis...")
        self.w.write(query)
        await self.w.drain()
        print(f"Writing '{query}' to redis...Done!")
        value, response = await self._read_redis_answer()
        return value, response

    async def get_query(self, key):
        """
        Implements a redis get query

        :param key: the key to GET
        """
        query = f"GET {key}\r\n".encode()
        print(f"query: {query}")
        value, response = await self._send_query(query)
        print(f"GET {key} answered {response.decode()}")
        return value, response
     
    async def set_query(self, key, value):
        """
        Implements a redis set query

        :param key: the key to SET
        :param value: the key to SET for 'key'
        """
        query = f"SET {key} {value}\r\n".encode()
        print(f"query: {query}")
        rvalue, response = await self._send_query(query)
        print(f"SET {{{key}:{value}}} answered {response.decode()}")
        return rvalue, response

    async def send_close(self):
        """
        Asks server to close connection
        """
        query = "close\r\n".encode()
        print(f"query: {query}")
        self.w.write(query)
        await self.w.drain()
        await self.disconnect()
        print("Closed connection")