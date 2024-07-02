import asyncio
import aiohttp
import attr
import json
import pickle
from datetime import datetime


final_list = []


@attr.s
class Fetch:

    limit = attr.ib()  # batch size
    rate = attr.ib(default=2, converter=int)  # speed

    async def make_request(self, url, key):
        async with self.limit:
            async with aiohttp.ClientSession() as session:
                async with session.request(method='GET', url=url) as response:
                    json_resp = await response.json()
                    status = response.status
                    print(
                        f'{datetime.now()} - Made request: {url}, Status: {status}\n')

                    await asyncio.sleep(self.rate)

                    return {key: json_resp}


async def main(items, rate, max):
    # this is the number of requests maximum that will go through per round.
    limit = asyncio.Semaphore(max)
    fetch = Fetch(
        rate=rate,
        limit=limit
    )

    tasks = []
    for item in items:
        key = item[0]
        url = item[-1]
        tasks.append(fetch.make_request(
            url=url, key=key, limit=limit)
        )
    results = await asyncio.gather(*tasks)
    final_list.append(results)


if __name__ == '__main__':
    # This is just a pickled file of the URLs from the Mapbox requests.
    with open("urls.pickle", 'rb') as file:
        # Pickling is an unsafe practice. Never open pickled files where you don't already know what's in it.
        items = pickle.load(file, encoding='utf-8')
        print(len(items))
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main(items=items, rate=1, max=4))

    final_list = final_list[0]
    final_list_json = json.dumps(final_list)

    with open('final_list.json', 'w') as file:
        file.write(final_list_json)
