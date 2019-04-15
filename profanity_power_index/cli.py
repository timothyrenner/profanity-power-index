import click
import os
import elasticsearch
import sys
import csv
import re
import sh
import json
import importlib_resources

from dotenv import load_dotenv, find_dotenv
from loguru import logger
from datetime import datetime
from dateutil import tz

from profanity_power_index.collect_tweets import collect_tweets
from profanity_power_index.extract_profanity import extract_profanity
from profanity_power_index.build_site import build_site

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
@click.option(
    "--track",
    "-t",
    multiple=True,
    help="A target to track. This option can be repeated. "
    "At least one is required.",
)
@click.option(
    "--elasticsearch-index",
    "-e",
    type=str,
    default="profanity-power-index",
    help="The name of the elasticsearch index to save the results to. "
    "Default: profanity-power-index.",
)
@click.option(
    "--drop-index",
    "-d",
    is_flag=True,
    help="Whether to drop the elasticsearch index prior to collecting. "
    "Default: False.",
)
@click.option(
    "--batch-size",
    "-b",
    type=int,
    default=10,
    help="The batch size for bulk writing to Elasticsearch. Default: 10.",
)
def collect(track, elasticsearch_index, drop_index, batch_size):
    """
    Collects tweets from the Twitter public timeline for the specified
    tracking terms that contain profanity and saves them to Elasticsearch.

    Requires the following four environment variables (which can be loaded from
    a .env file):

    TWITTER_CONSUMER_KEY
    TWITTER_CONSUMER_SECRET
    TWITTER_ACCESS_TOKEN_KEY
    TWITTER_ACCESS_TOKEN_SECRET

    The elasticsearch URL can be controlled through ELASTICSEARCH_HOST, which
    defaults to "http://localhost:9200".
    """
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
@click.option(
    "--track",
    "-t",
    multiple=True,
    help="A target to track. This option can be repeated. "
    "At least one is required.",
)
@click.option(
    "--elasticsearch-index",
    "-e",
    type=str,
    default="profanity-power-index",
    help="The Elasticsearch index to pull the data from. "
    "Default: profanity-power-index.",
)
@click.option(
    "--output",
    "-o",
    type=click.File("w"),
    default="-",
    help="The name of the output file to save the data to. Default: stdout",
)
def extract(start, end, track, elasticsearch_index, output):
    """
    Extracts data from Elasticsearch into a CSV file.

    Arguments:\n
        START - The start date as YYYY-mm-ddTHH:MM:SS. Time zone offset is
            optional by adding +/-ZZZZ. Defaults to local system timezone.\n
        END - The end date as YYYY-mm-ddTHH:MM:SS. Time zone offset is
            optional by adding +/-ZZZZ. Defaults to local system timezone.
    """
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


@main.command()
@click.argument("data_file", type=str)
@click.argument("config_file", type=click.File("r"))
@click.option(
    "--output-dir",
    type=str,
    help="The output directory to render the site to.",
)
def build(data_file, config_file, output_dir):
    """
    Builds a site with a fancy interactive visualization.

    Arguments:\n
        DATA_FILE - The CSV file with the profanity. See README for schema.
        CONFIG_FILE - The JSON file with the site configuration.
    See README for schema.
    """
    if not os.path.exists(output_dir):
        logger.info(f"{output_dir} does not exist. Creating.")
        sh.mkdir(output_dir)
    if not os.path.exists(f"{output_dir}/js"):
        sh.mkdir(f"{output_dir}/js")
    sh.cp(data_file, output_dir)
    with importlib_resources.path(
        "profanity_power_index.resources", "profanity_power_index.js"
    ) as js_path:
        sh.cp(js_path, f"{output_dir}/js")

    template = build_site(
        json.load(config_file), f"{os.path.basename(data_file)}"
    )
    with open(f"{output_dir}/index.html", "w") as index_out:
        index_out.write(template)
