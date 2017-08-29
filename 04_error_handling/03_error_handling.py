"""
An example of periodically scheduling coroutines using an infinite loop of
scheduling a task using ensure_future and sleeping.

Artificially produce an error and use try..except clauses to catch them.

Set return_exceptions to True to retrieve potentially complete tasks after
an exception is raised.

"""

import asyncio
import argparse
import logging
from functools import partial
from datetime import datetime

import aiohttp
import async_timeout


LOGGER_FORMAT = '%(asctime)s %(message)s'
URL_TEMPLATE = "https://hacker-news.firebaseio.com/v0/item/{}.json"
TOP_STORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
FETCH_TIMEOUT = 10
MAXIMUM_FETCHES = 500

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
    try:
        results = await asyncio.gather(*tasks)
    except BoomException as e:
        # log.error("Error retrieving comments for top stories: {}".format(e))
        raise

    # reduce the descendents comments and add it to this post's
    number_of_comments += sum(results)
    log.debug('{:^6} > {} comments'.format(post_id, number_of_comments))

    return number_of_comments


async def get_comments_of_top_stories(loop, session, limit, iteration):
    """Retrieve top stories in HN.

    """
    fetcher = URLFetcher()  # create a new fetcher for this task
    try:
        response = await fetcher.fetch(session, TOP_STORIES_URL)
    except BoomException as e:
        log.error("Error retrieving top stories: {}".format(e))
        # return instead of re-raising as it will go unnoticed
        return
    except Exception as e:  # catch generic exceptions
        log.error("Unexpected exception: {}".format(e))
        return

    tasks = [post_number_of_comments(
        loop, session, fetcher, post_id) for post_id in response[:limit]]

    # we're not using a try..except anymore as passing `return_exceptions`
    # as True will simply return the Exception object produced
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # we can safely iterate the results
    for post_id, result in zip(response[:limit], results):
        # and manually check the types
        if isinstance(result, BoomException):
            log.error("Error retrieving comments for top stories: {}".format(
                result))
        elif isinstance(result, Exception):
            log.error("Unexpected exception: {}".format(result))
        else:
            log.info("Post {} has {} comments ({})".format(
                post_id, result, iteration))

    return fetcher.fetch_counter


async def poll_top_stories_for_comments(loop, session, period, limit):
    """Periodically poll for new stories and retrieve number of comments.

    """
    iteration = 1
    errors = []
    while True:
        if errors:
            log.info('Error detected, quitting')
            return

        log.info("Calculating comments for top {} stories. ({})".format(
            limit, iteration))

        future = asyncio.ensure_future(
            get_comments_of_top_stories(loop, session, limit, iteration))

        now = datetime.now()

        def callback(fut, errors):
            try:
                fetch_count = fut.result()
            except BoomException as e:
                log.debug('Adding {} to errors'.format(e))
                errors.append(e)
            except Exception as e:
                log.exception('Unexpected error')
                errors.append(e)
            else:
                log.info(
                    '> Calculating comments took {:.2f} seconds and {} fetches'.format(
                        (datetime.now() - now).total_seconds(), fetch_count))

        future.add_done_callback(partial(callback, errors=errors))

        log.info("Waiting for {} seconds...".format(period))
        iteration += 1
        await asyncio.sleep(period)


if __name__ == '__main__':
    args = parser.parse_args()
    if args.verbose:
        log.setLevel(logging.DEBUG)

    loop = asyncio.get_event_loop()
    with aiohttp.ClientSession(loop=loop) as session:
        loop.run_until_complete(
            poll_top_stories_for_comments(
                loop, session, args.period, args.limit))

    loop.close()
