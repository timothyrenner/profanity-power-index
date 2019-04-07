import click
import os
import twitter
import elasticsearch
import sys

from dotenv import load_dotenv, find_dotenv
from elasticsearch.helpers import bulk as es_bulk
from loguru import logger
from toolz import curry, get_in, thread_last, partition_all

load_dotenv(find_dotenv())

ELASTICSEARCH_HOST = os.getenv(
    "ELASTICSEARCH_HOST", default="http://localhost:9200"
)
TWITTER_CONSUMER_KEY = os.getenv("TWITTER_CONSUMER_KEY")
TWITTER_CONSUMER_SECRET = os.getenv("TWITTER_CONSUMER_SECRET")
TWITTER_ACCESS_TOKEN_KEY = os.getenv("TWITTER_ACCESS_TOKEN_KEY")
TWITTER_ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")

if (
    (not TWITTER_CONSUMER_KEY)
    or (not TWITTER_CONSUMER_SECRET)
    or (not TWITTER_ACCESS_TOKEN_KEY)
    or (not TWITTER_ACCESS_TOKEN_SECRET)
):
    logger.warn("Missing Twitter API keys - collection will not function.")


TWEET_MAPPING = {
    "mappings": {
        "tweet": {
            "properties": {
                "coordinates": {"type": "geo_point"},
                "created_at": {
                    "type": "date",
                    "format": "EEE MMM dd HH:mm:ss Z yyyy",
                },
            }
        }
    }
}


PROFANITY = [
    "fuck",
    "shit",
    "bitch",
    "dick",
    "douche",
    " ass ",
    "asshole",
    "asshat",
    "jackass",
    "dumbass",
]


def _extract_text(tweet):
    if "retweeted_status" in tweet:
        if get_in(["retweeted_status", "truncated"], tweet, False):
            return tweet["retweeted_status"]["extended_tweet"]["full_text"]
        else:
            return tweet["retweeted_status"]["text"]
    else:
        return tweet["text"]


def _contains_profanity(tweet):
    tweet_text = _extract_text(tweet).lower()
    for profanity in PROFANITY:
        if profanity in tweet_text:
            return True

    return False


def _tweet_to_bulk(index, tweet):
    return {
        "_index": index,
        "_type": "tweet",
        "_id": tweet["id_str"],
        "_source": {
            "id": tweet["id_str"],
            "coordinates": get_in(["coordinates", "coordinates"], tweet, None),
            "text": _extract_text(tweet),
            "created_at": tweet["created_at"],
        },
    }


@click.group()
def main():
    pass


@main.command()
@click.option("--track", "-t", multiple=True)
@click.option(
    "--elasticsearch-index", "-e", type=str, default="profanity-power-index"
)
@click.option("--drop-index", "-d", is_flag=True)
@click.option("--batch-size", "-b", type=int, default=10)
def collect(track, elasticsearch_index, drop_index, batch_size):

    if not track:
        logger.error("Must track at least one term.")
        sys.exit(1)

    es = elasticsearch.Elasticsearch(hosts=[ELASTICSEARCH_HOST])

    if es.indices.exists(elasticsearch_index):
        logger.warning(f"Index {elasticsearch_index} exists.")
        if drop_index:
            logger.warning(f"Dropping {elasticsearch_index}.")
            es.indices.delete(elasticsearch_index)
            logger.info(f"Creating {elasticsearch_index}.")
            es.indices.create(index=elasticsearch_index, body=TWEET_MAPPING)
    else:
        logger.info(f"Creating {elasticsearch_index}.")
        es.indices.create(index=elasticsearch_index, body=TWEET_MAPPING)
        logger.info(f"{elasticsearch_index} successfully created.")

    api = twitter.Api(
        consumer_key=TWITTER_CONSUMER_KEY,
        consumer_secret=TWITTER_CONSUMER_SECRET,
        access_token_key=TWITTER_ACCESS_TOKEN_KEY,
        access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
    )

    logger.info(f"Connecting to twitter stream. Tracking {', '.join(track)}.")
    tweet_stream = api.GetStreamFilter(track=track)

    tweet_to_bulk = curry(_tweet_to_bulk)(elasticsearch_index)
    tweet_doc_stream = thread_last(
        tweet_stream,
        # Filter out tweets that don't contain profanity.
        (filter, _contains_profanity),
        # Convert the tweets to a bulk-indexable document.
        (map, tweet_to_bulk),
        # Partition for bulk writes.
        (partition_all, batch_size)
    )

    logger.info(f"Sending tweets to {elasticsearch_index}.")
    failed = 0
    succeeded = 0
    # Since the doc stream is partitioned we get the tweets in batches.
    for tweet_batch in tweet_doc_stream:
        if (failed + succeeded) % 100 == 0:
            logger.info(
                f"{failed + succeeded} tweets processed: "
                f"{succeeded} succeeded, {failed} failed."
            )
        ok, fail = es_bulk(es, tweet_batch, stats_only=True)
        succeeded += ok
        failed += fail


if __name__ == "__main__":
    main()
