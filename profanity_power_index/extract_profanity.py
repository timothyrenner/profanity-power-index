from toolz import assoc, assoc_in, thread_first, get_in
from itertools import product
from loguru import logger


PROFANITY_MAPPING = {
    "fuck": "text:*fuck*",
    "shit": "text:*shit*",
    "bitch": "text:bitch*",
    "ass": "text:ass*",
    "dick": "text:*dick*",
}


tweets_per_minute = {
    "tweets_per_minute": {
        "date_histogram": {
            "field": "created_at",
            "interval": "minute",
            "format": "date_time_no_millis",
        }
    }
}


def profanity_filter(profanity_mapping):
    return {
        "profanity": {
            "filters": {
                "filters": {
                    word: {"query_string": {"query": query}}
                    for word, query in profanity_mapping.items()
                }
            }
        }
    }


def time_range(start, end):
    return {
        "range": {
            "created_at": {
                "gte": start,
                "lte": end,
                "format": "date_time_no_millis",
            }
        }
    }


def target_match(target_query):
    return {"query_string": {"query": target_query, "fields": ["text"]}}


def target_filter(targets):
    return {
        "target": {
            "filters": {
                "filters": {
                    target: target_match(f"{target}*") for target in targets
                }
            }
        }
    }


def elasticsearch_query(start, end, targets, profanity_mapping):
    return thread_first(
        {"size": 0},
        (assoc, "query", time_range(start, end)),
        (assoc, "aggregations", tweets_per_minute),
        (
            assoc_in,
            ["aggregations", "tweets_per_minute", "aggregations"],
            profanity_filter(profanity_mapping),
        ),
        (
            assoc_in,
            [
                "aggregations",
                "tweets_per_minute",
                "aggregations",
                "profanity",
                "aggregations",
            ],
            target_filter(targets),
        ),
    )


def extract_profanity(
    es_connection,
    start,
    end,
    targets,
    elasticsearch_index="profanity-power-index",
):

    es_query = elasticsearch_query(start, end, targets, PROFANITY_MAPPING)

    logger.info("Calling elasticsearch.")
    results = es_connection.search(index=elasticsearch_index, body=es_query)
    logger.info(f"Done. Hit count: {results['hits']['total']}")

    logger.info("Marshalling elasticsearch results.")
    marshalled_results = []
    time_profanity_target = product(
        results["aggregations"]["tweets_per_minute"]["buckets"],
        PROFANITY_MAPPING.keys(),
        targets,
    )
    for time_bucket, profanity, target in time_profanity_target:
        marshalled_results.append(
            {
                "time": time_bucket["key_as_string"],
                "word": profanity,
                "subject": target,
                "count": get_in(
                    [
                        "profanity",
                        "buckets",
                        profanity,
                        "target",
                        "buckets",
                        target,
                        "doc_count",
                    ],
                    time_bucket,
                    0,
                ),
            }
        )
    return marshalled_results
