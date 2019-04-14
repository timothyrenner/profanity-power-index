# Profanity Power Index

The Profanity Power Index tracks profanity associated with certain subjects on Twitter.
This is a Python port of the Clojure version [located here](https://github.com/timothyrenner/ProfanityPowerIndex).
I was having trouble with the dependencies for ES and Twitter for the Clojure version so I threw this implementation together.

## Setup

Clone this repo, then

```
pip install -e .
```

This installs the `profanity-power-index` command line tool.
There are three subcommands: `collect`, `extract` and `build`.
Details for each are below. 

## `collect`

This subcommand pulls data from the Twitter public timeline based on tracking targets and saves the tweets that contain profanity to an Elasticsearch index.
You'll need an installation of Elasticsearch 6.X running (`brew install elasticsearch` works fine on MacOS for local instances).

Suppose you wanted to collect tweets containing profanity for Donald Trump.
On my machine (MacOS) I need to start Elasticsearch manually.

```
elasticsearch
```

does the trick.
In _another terminal_ I start the profanity power index collector.

```
profanity-power-index collect -t trump
```

... and that's it.
The collector will run until it's killed.
It batches tweets and sends them in bulk to ES, so it's not always going to be super realtime, but for high enough volume (not a problem for the example above) it should be pretty close to real time.

Full usage:

```
Usage: profanity-power-index collect [OPTIONS]

  Collects tweets from the Twitter public timeline for the specified
  tracking terms that contain profanity and saves them to Elasticsearch.

  Requires the following four environment variables (which can be loaded
  from a .env file):

  TWITTER_CONSUMER_KEY TWITTER_CONSUMER_SECRET TWITTER_ACCESS_TOKEN_KEY
  TWITTER_ACCESS_TOKEN_SECRET

  The elasticsearch URL can be controlled through ELASTICSEARCH_HOST, which
  defaults to "http://localhost:9200".

Options:
  -t, --track TEXT                A target to track. This option can be
                                  repeated. At least one is required.
  -e, --elasticsearch-index TEXT  The name of the elasticsearch index to save
                                  the results to. Default: profanity-power-
                                  index.
  -d, --drop-index                Whether to drop the elasticsearch index
                                  prior to collecting. Default: False.
  -b, --batch-size INTEGER        The batch size for bulk writing to
                                  Elasticsearch. Default: 10.
  --help                          Show this message and exit.

```

## `extract`

Once you've collected your glorious dataset it needs to be seen!
This involves querying Elasticsearch and extracting the data in a structured CSV form.
That's what `extract` is for.

Say you've collected an hour's worth of tweets about Donald Trump that contain profanity.
To extract them you'd run.

```
profanity-power-index extract 2019-04-11T14:00:00 2019-04-11T15:00:00 -t trump -o trump_profanity.csv
```

This pulls and aggregates by minute the profanity containing tweets between 2 and 3 PM on 4/11/2019 associated with Donald Trump.
The CSV has the following schema:

| Column Name | Description                                                                                   | Example              |
| ----------- | --------------------------------------------------------------------------------------------- | -------------------- |
| time        | The time by minute.                                                                           | 2019-04-11T13:00:00Z |
| word        | The profanity being aggregated.                                                               | fuck                 |
| subject     | The target.                                                                                   | trump                |
| count       | The number of tweets containing both the word and subject during the specified minute period. | 12423                |

From here you can use pretty much anything for visualization or analysis.

Complete usage:

```
Usage: profanity-power-index extract [OPTIONS] START END

  Extracts data from Elasticsearch into a CSV file.

  Arguments:

      START - The start date as YYYY-mm-ddTHH:MM:SS. Time zone offset is
      optional by adding +/-ZZZZ. Defaults to local system timezone.

      END - The end date as YYYY-mm-ddTHH:MM:SS. Time zone offset is
      optional by adding +/-ZZZZ. Defaults to local system timezone.

Options:
  -t, --track TEXT                A target to track. This option can be
                                  repeated. At least one is required.
  -e, --elasticsearch-index TEXT  The Elasticsearch index to pull the data
                                  from. Default: profanity-power-index.
  -o, --output FILENAME           The name of the output file to save the data
                                  to. Default: stdout
  --help                          Show this message and exit.

```

## `build`

I designed an interactive visualization for this a while back (examples [here](https://timothyrenner.github.io/projects/profanitypowerindex/)).
If you want this for your data there's an additional configuration required.

```javascript
{
    "subjects": [
        {
            "name": "trump",
            "display_name": "President Trump",
            "image": "https://pbs.twimg.com/profile_images/874276197357596672/kUuht00m_400x400.jpg",
            "id": "trump",
            "colors": "Reds"
        },
        // other subjects here.   
    ]
}
```

The "name" needs to match the values of the "subject" column in the data CSV.
"display_name" is the name you want to see on the site itself.
"image" is a URL to the image you want to see next to everyone's feelings 😄.
Aspect ratio 1 works best.
"id" needs to be a valid CSS identifier.
It's used to tie the interactions together.
"colors" needs to be a [ColorBrewer](http://colorbrewer2.org) scale.

Suppose your config file is in `config.json` and the data's in `profanity.csv`.
Then to build the fancypants visualization you'll call:

```
profanity-power-index build profanity.csv config.json --output-dir test-site
```

This will build a fully functioning site in `test-site` with the following directory structure:

```
test-site/
├── index.html
├── js
│   └── profanity_power_index.js
└── profanity.csv
```

This is a fully functioning site.

```
cd test-site && python -m http.server
```

will launch it on `localhost:8000`.

The `profanity_power_index.js` file and the [Jinja](http://jinja.pocoo.org/) template ship with the package.

Full usage for build is pretty simple:

```
Usage: profanity-power-index build [OPTIONS] DATA_FILE CONFIG_FILE

  Builds a site with a fancy interactive visualization.

  Arguments:

      DATA_FILE - The CSV file with the profanity. See README for schema.
      CONFIG_FILE - The JSON file with the site configuration. See README
      for schema.

Options:
  --output-dir TEXT  The output directory to render the site to.
  --help             Show this message and exit.

```