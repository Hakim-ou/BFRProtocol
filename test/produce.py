import os
import asyncio
from client import Client

ID = os.environ['ID']
PROXY_HOST = os.environ['PROXY_HOST']
PROXY_PORT = int(os.environ['PROXY_PORT'])

async def main():
	producer = Client(PROXY_HOST, PROXY_PORT)
	# generate content
	NB_CONTENT = 1000 #TODO make it 1M
	for i in range(NB_CONTENT):
		await producer.set_query(f"{ID}:content{i}", f"content{i}")

asyncio.run(main())
