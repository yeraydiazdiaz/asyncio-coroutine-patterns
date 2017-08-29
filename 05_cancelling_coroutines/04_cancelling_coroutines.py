"""
An example of periodically scheduling coroutines using an infinite loop of
scheduling a task using ensure_future and sleeping.

Artificially produce an error and use try..except clauses to catch them.

Use `wait` to cancel pending coroutines in case if an exception.

"""

import asyncio
import argparse
import logging
from functools import partial
from datetime import datetime
from concurrent.futures import FIRST_EXCEPTION

import aiohttp
import async_timeout


LOGGER_FORMAT = '%(asctime)s %(message)s'
URL_TEMPLATE = "https://hacker-news.firebaseio.com/v0/item/{}.json"
TOP_STORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
FETCH_TIMEOUT = 10
MAXIMUM_FETCHES = 250

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
    try:
        response = await fetcher.fetch(session, url)
    except BoomException as e:
        log.debug("Error retrieving post {}: {}".format(post_id, e))
        raise e

    # base case, there are no comments
    if response is None or 'kids' not in response:
        return 0

    # calculate this post's comments as number of comments
    number_of_comments = len(response['kids'])

    try:
        # create recursive tasks for all comments
        tasks = [asyncio.ensure_future(post_number_of_comments(
            loop, session, fetcher, kid_id)) for kid_id in response['kids']]

        # schedule the tasks and retrieve results
        try:
            results = await asyncio.gather(*tasks)
        except BoomException as e:
            log.debug("Error retrieving comments for top stories: {}".format(e))
            raise

        # reduce the descendents comments and add it to this post's
        number_of_comments += sum(results)
        log.debug('{:^6} > {} comments'.format(post_id, number_of_comments))

        return number_of_comments
    except asyncio.CancelledError:
        if tasks:
            log.info("Comments for post {} cancelled, cancelling {} child tasks".format(
                post_id, len(tasks)))
            for task in tasks:
                task.cancel()
        else:
            log.info("Comments for post {} cancelled".format(post_id))
        raise


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

    tasks = {
        asyncio.ensure_future(
            post_number_of_comments(loop, session, fetcher, post_id)
        ): post_id for post_id in response[:limit]}

    # return on first exception to cancel any pending tasks
    done, pending = await asyncio.shield(asyncio.wait(
        tasks.keys(), return_when=FIRST_EXCEPTION))

    # if there are pending tasks is because there was an exception
    # cancel any pending tasks
    for pending_task in pending:
        pending_task.cancel()

    # process the done tasks
    for done_task in done:
        # if an exception is raised one of the Tasks will raise
        try:
            print("Post {} has {} comments ({})".format(
                tasks[done_task], done_task.result(), iteration))
        except BoomException as e:
            print("Error retrieving comments for top stories: {}".format(e))

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
