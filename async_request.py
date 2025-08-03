import asyncio
import aiohttp
from aiohttp_retry import RetryClient, ExponentialRetry
import attr
import json
import pickle
from datetime import datetime


@attr.s
class Fetch:

    limit = attr.ib()  # batch size
    rate = attr.ib(default=4, converter=int)  # speed
    retry_options = ExponentialRetry(attempts=3)
    

    async def make_request(self, url, key):
        json_resp = None
        retry_client = RetryClient(raise_for_status=False, retry_options=self.retry_options)
        async with self.limit:
            try:
                async with retry_client.get(url) as response:
                    json_resp = await response.json()
                    status = response.status
                    print(
                        f'{datetime.now()} - Made request: {url}, Status: {status}\n')

                    await asyncio.sleep(self.rate)
                    return {key: json_resp}

            except Exception as e:
                print("There was an error: ", e)
                print(dir(e))
                print(e.value.code)
                self.rate = self.rate ** 2
                await asyncio.sleep(self.rate)

            finally:
                await retry_client.close()



async def main(items, rate, max):
    # this is the number of requests maximum that will go through per round.
    limit = asyncio.Semaphore(max)
    fetch = Fetch(
        rate=rate,
        limit=limit
    )

    tasks = []
    for key, value in items.items():
        url = value
        tasks.append(fetch.make_request(
            url=url, key=key)
        )
    results = await asyncio.gather(*tasks)
    final_list.append(results)


if __name__ == '__main__':
    final_list = []
    # This is just a pickled file of the URLs from the Mapbox requests.
    with open("/home/stephen-tanksley/Desktop/Data/station_pairs_1.pickle", 'rb') as file:
        # Pickling is an unsafe practice. Never open pickled files where you don't already know what's in it.
        items = pickle.load(file, encoding='utf-8')

    limited_keys = list(items.keys())
    limited_keys_len = len(limited_keys) // 3
    limited_2 = limited_keys_len * 2

    limited_items = {key:value for key, value in items.items() if key in limited_keys[limited_keys_len:limited_2]}

    try:
        asyncio.run(main(items=limited_items, rate=4, max=4))
    except Exception as e:
        print(datetime.now(), f"{datetime.now()} -- ", "There was an error:", e)
        print(e.type)
        print(e.value)
        print(e.value.code)
    finally:

        final_list = final_list[0]
        final_list_json = json.dumps(final_list)

        with open('./final_list_test_c.json', 'w') as file:
            file.write(final_list_json)