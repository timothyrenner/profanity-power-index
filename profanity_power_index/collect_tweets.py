import twitter

from toolz import get_in, curry, thread_last, partition_all
from elasticsearch.helpers import bulk as es_bulk
from loguru import logger

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
    try:
        tweet_text = _extract_text(tweet).lower()
    except Exception:
        return False
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


def collect_tweets(
    es_client,
    track,
    twitter_consumer_key,
    twitter_consumer_secret,
    twitter_access_token_key,
    twitter_access_token_secret,
    elasticsearch_index="profanity-power-index",
    drop_index=False,
    batch_size=10,
):

    if es_client.indices.exists(elasticsearch_index):
        logger.warning(f"Index {elasticsearch_index} exists.")
        if drop_index:
            logger.warning(f"Dropping {elasticsearch_index}.")
            es_client.indices.delete(elasticsearch_index)
            logger.info(f"Creating {elasticsearch_index}.")
            es_client.indices.create(
                index=elasticsearch_index, body=TWEET_MAPPING
            )
    else:
        logger.info(f"Creating {elasticsearch_index}.")
        es_client.indices.create(index=elasticsearch_index, body=TWEET_MAPPING)
        logger.info(f"{elasticsearch_index} successfully created.")

    api = twitter.Api(
        consumer_key=twitter_consumer_key,
        consumer_secret=twitter_consumer_secret,
        access_token_key=twitter_access_token_key,
        access_token_secret=twitter_access_token_secret,
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
        (partition_all, batch_size),
    )

    logger.info(f"Sending tweets to {elasticsearch_index}.")
    failed = 0
    succeeded = 0
    logger.info(
        f"{failed + succeeded} tweets processed: "
        f"{succeeded} succeeded, {failed} failed."
    )
    # Since the doc stream is partitioned we get the tweets in batches.
    for tweet_batch in tweet_doc_stream:
        ok, fail = es_bulk(es_client, tweet_batch, stats_only=True)
        succeeded += ok
        failed += fail
        if (failed + succeeded) % 100 == 0:
            logger.info(
                f"{failed + succeeded} tweets processed: "
                f"{succeeded} succeeded, {failed} failed."
            )
