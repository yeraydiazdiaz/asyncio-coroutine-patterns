"""
An example of periodically scheduling coroutines using an infinite loop of
scheduling a task using ensure_future and sleeping.

Artificially produce an error on URLFetcher.

"""

import asyncio
import argparse
import logging
from datetime import datetime

import aiohttp
import async_timeout


LOGGER_FORMAT = '%(asctime)s %(message)s'
URL_TEMPLATE = "https://hacker-news.firebaseio.com/v0/item/{}.json"
TOP_STORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
FETCH_TIMEOUT = 10
MAXIMUM_FETCHES = 5

parser = argparse.ArgumentParser(
    description='Calculate the number of comments of the top stories in HN.')
parser.add_argument(
    '--period', type=int, default=5, help='Number of seconds between poll')
parser.add_argument(
    '--limit', type=int, default=5,
    help='Number of new stories to calculate comments for')
parser.add_argument('--verbose', action='store_true', help='Detailed output')


logging.basicConfig(format=LOGGER_FORMAT, datefmt='[%H:%M:%S]')
log = logging.getLogger()
log.setLevel(logging.INFO)


class BoomException(Exception):
    pass


class URLFetcher():
    """Provides counting of URL fetches for a particular task.

    """

    def __init__(self):
        self.fetch_counter = 0

    async def fetch(self, session, url):
        """Fetch a URL using aiohttp returning parsed JSON response.

        As suggested by the aiohttp docs we reuse the session.

        """
        with async_timeout.timeout(FETCH_TIMEOUT):
            self.fetch_counter += 1
            if self.fetch_counter > MAXIMUM_FETCHES:
                raise BoomException('BOOM!')

            async with session.get(url) as response:
                return await response.json()


async def post_number_of_comments(loop, session, fetcher, post_id):
    """Retrieve data for current post and recursively for all comments.

    """
    url = URL_TEMPLATE.format(post_id)
    response = await fetcher.fetch(session, url)

    # base case, there are no comments
    if response is None or 'kids' not in response:
        return 0

    # calculate this post's comments as number of comments
    number_of_comments = len(response['kids'])

    # create recursive tasks for all comments
    tasks = [post_number_of_comments(
        loop, session, fetcher, kid_id) for kid_id in response['kids']]

    # schedule the tasks and retrieve results
    results = await asyncio.gather(*tasks)

    # reduce the descendents comments and add it to this post's
    number_of_comments += sum(results)
    log.debug('{:^6} > {} comments'.format(post_id, number_of_comments))

    return number_of_comments


async def get_comments_of_top_stories(loop, session, limit, iteration):
    """Retrieve top stories in HN.

    """
    fetcher = URLFetcher()  # create a new fetcher for this task
    response = await fetcher.fetch(session, TOP_STORIES_URL)

    tasks = [post_number_of_comments(
        loop, session, fetcher, post_id) for post_id in response[:limit]]

    results = await asyncio.gather(*tasks)

    for post_id, num_comments in zip(response[:limit], results):
        log.info("Post {} has {} comments ({})".format(
            post_id, num_comments, iteration))
    return fetcher.fetch_counter  # return the fetch count


async def poll_top_stories_for_comments(loop, session, period, limit):
    """Periodically poll for new stories and retrieve number of comments.

    """
    iteration = 1
    while True:
        log.info("Calculating comments for top {} stories. ({})".format(
            limit, iteration))

        future = asyncio.ensure_future(
            get_comments_of_top_stories(loop, session, limit, iteration))

        now = datetime.now()

        def callback(fut):
            try:
                fetch_count = fut.result()
            except BoomException as e:
                log.exception("Something went BOOM")
                return

            log.info(
                '> Calculating comments took {:.2f} seconds and {} fetches'.format(
                    (datetime.now() - now).total_seconds(), fetch_count))

        future.add_done_callback(callback)

        log.info("Waiting for {} seconds...".format(period))
        iteration += 1
        await asyncio.sleep(period)


async def main(loop, period, limit):
    """Async entry point coroutine.

    """
    async with aiohttp.ClientSession(loop=loop) as session:
        comments = await poll_top_stories_for_comments(loop, session, period, limit)

    return comments


if __name__ == '__main__':
    args = parser.parse_args()
    if args.verbose:
        log.setLevel(logging.DEBUG)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, args.period, args.limit))

    loop.close()
