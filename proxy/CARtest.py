import asyncio
import json, time, calendar
from probables import BloomFilter

bf = BloomFilter(est_elements=1000, false_positive_rate=0.01)
bf.add('hakim')
bf.add('adil')
bf.add('said')
bf.add('encore_hakim')
print(bf)

code = 'CAR'
bf_string = bf.export_hex()
sourceID = "localhost:6579"
nextHope = 'localhost:' + str(6479)
nounce = calendar.timegm(time.gmtime())

json_string = json.dumps({'code':code, 'sourceID':sourceID, 'nextHope':nextHope, 'bf':bf_string, 'nounce':nounce})
msg = b'J' + json_string.encode() + b'\r\n'

async def main():
    reader, writer = await asyncio.open_connection(host='localhost', port='6380')
    writer.write(msg)
    await writer.drain()
    writer.write(b'close')
    await writer.drain()

asyncio.run(main())
