import click
import os
import elasticsearch
import sys
import csv
import re

from dotenv import load_dotenv, find_dotenv
from loguru import logger
from datetime import datetime
from dateutil import tz

from collect_tweets import collect_tweets
from extract_profanity import extract_profanity

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
    logger.warn("Missing Twitter API keys - collect will not function.")


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
        logger.error("❌ Must track at least one term. ❌")
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
        batch_size=batch_size,
    )


@main.command()
@click.argument("start", type=str)
@click.argument("end", type=str)
@click.option("--track", "-t", multiple=True)
@click.option(
    "--elasticsearch-index", "-e", type=str, default="profanity-power-index"
)
@click.option("--output", "-o", type=click.File("w"), default="-")
def extract(start, end, track, elasticsearch_index, output):
    if not track:
        logger.error("❌ Must track at least one term. ❌")
        sys.exit(1)

    if not re.match(r".*\d{4}$", start):
        time_zone = tz.gettz()
        logger.info(f"Adding local time zone to start.")
        start_date = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S").replace(
            tzinfo=time_zone
        )
        start = start_date.strftime("%Y-%m-%dT%H:%M:%S%z")

    if not re.match(r".*\d{4}$", end):
        time_zone = tz.gettz()
        logger.info(f"Adding local time zone to end.")
        end_date = datetime.strptime(end, "%Y-%m-%dT%H:%M:%S").replace(
            tzinfo=time_zone
        )
        end = end_date.strftime("%Y-%m-%dT%H:%M:%S%z")

    es = elasticsearch.Elasticsearch(hosts=[ELASTICSEARCH_HOST])
    logger.info(
        f"🖕 Extracting profanity between {start} and {end} "
        f"for {', '.join(track)} in {elasticsearch_index}. 🖕"
    )
    results = extract_profanity(es, start, end, track)
    logger.info(f"Writing to {output.name}.")

    writer = csv.DictWriter(
        output, fieldnames=["time", "word", "subject", "count"]
    )
    writer.writeheader()
    for result in results:
        writer.writerow(result)
    logger.info(f"🖕 Wrote {len(results)} rows to {output.name}. 🖕")


if __name__ == "__main__":
    main()
