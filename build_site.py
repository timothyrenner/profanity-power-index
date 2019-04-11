import importlib

from toolz import assoc
from jinja2 import Environment, FileSystemLoader, select_autoescape

env = Environment(
    loader=FileSystemLoader("."), autoescape=select_autoescape(["html"])
)


def _make_colors(subject_config):
    color_scheme = importlib.import_module(
        f"palettable.colorbrewer.sequential.{subject_config['colors']}_5"
    )
    return {
        "sparkline": [
            {"offset": offset, "color": color}
            for offset, color in zip(
                ["0%", "25%", "50%", "75%", "100%"], color_scheme.hex_colors
            )
        ],
        "barchart": {
            "base": color_scheme.hex_colors[3],
            "hover": color_scheme.hex_colors[4]
        }
    }


def build_site(site_config, data_file):
    subjects = [
        assoc(s, "colors", _make_colors(s)) for s in site_config["subjects"]
    ]

    return env.get_template("profanity_power_index.html.jinja").render(
        subjects=subjects, file_location=data_file
    )
