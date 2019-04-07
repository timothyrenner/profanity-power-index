import click
import os
import elasticsearch
import sys

from dotenv import load_dotenv, find_dotenv
from loguru import logger

from collect_tweets import collect_tweets

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

    collect_tweets(
        es,
        track,
        twitter_consumer_key=TWITTER_CONSUMER_KEY,
        twitter_consumer_secret=TWITTER_CONSUMER_SECRET,
        twitter_access_token_key=TWITTER_ACCESS_TOKEN_KEY,
        twitter_access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
        elasticsearch_index=elasticsearch_index,
        drop_index=drop_index,
        batch_size=batch_size
    )


if __name__ == "__main__":
    main()
