import os
import asyncio
import time
from client import Client

PRODUCERS_IDS = os.environ['PRODUCERS_IDS'].split()
PROXY_HOST = os.environ['PROXY_HOST']
PROXY_PORT = int(os.environ['PROXY_PORT'])

async def main():
	consumer = Client(PROXY_HOST, PROXY_PORT)
	# generate content
	NB_CONTENT = 10000 #TODO make it 1M
	exectimes = dict()
	for id in PRODUCERS_IDS:
		start_time = time.time()
		for i in range(NB_CONTENT):
			value, response = await consumer.get_query(f"{id}:content{i}")
			if value != f"content{i}":
				print(f"Unexpected value: {value}")
		exectimes[id] = time.time() - start_time
	print("exection time by id:")
	total = 0
	for id in PRODUCERS_IDS:
		print(f"from {id}: {exectimes[id]} seconds")
		total += exectimes[id]
	print("--------------------------")
	print(f"total: {total} seconds")

asyncio.run(main())