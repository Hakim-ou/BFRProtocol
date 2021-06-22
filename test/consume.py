import os
import asyncio
import time
from client import Client

PRODUCERS_IDS = os.environ['PRODUCERS_IDS'].split()
PROXY_HOST = os.environ['PROXY_HOST']
PROXY_PORT = int(os.environ['PROXY_PORT'])

async def main():
	consumer = Client(PROXY_HOST, PROXY_PORT)
	await consumer.connect()
	# generate content
	NB_CONTENT = 10000 #TODO make it 1M
	exectimes = dict()
	falsNegatives = 0
	for id in PRODUCERS_IDS:
		start_time = time.time()
		for i in range(NB_CONTENT):
			value, response = await consumer.get_query(f"{id}:content{i}")
			if value != f"content{i}":
				print(f"Unexpected value: {value}")
				falsNegatives += 1
		exectimes[id] = time.time() - start_time
	await consumer.send_close()
	print("exection time by id:")
	total = 0
	for id in PRODUCERS_IDS:
		print(f"from {id}: {exectimes[id]} seconds")
		total += exectimes[id]
	print("--------------------------")
	print(f"total: {total} seconds")
	print(f"false negatives rate: {falsNegatives*100/10000}")

asyncio.run(main())