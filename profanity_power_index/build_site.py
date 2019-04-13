from toolz import assoc
from jinja2 import Environment, PackageLoader, select_autoescape
from palettable.colorbrewer import sequential as brewer_sequential
from palettable.colorbrewer import diverging as brewer_diverging
from palettable.colorbrewer import qualitative as brewer_qualitative

env = Environment(
    loader=PackageLoader("profanity_power_index", "resources"),
    autoescape=select_autoescape(["html"]),
)


def _make_colors(subject_config):
    scheme_name = f"{subject_config['colors']}_5"
    if hasattr(brewer_sequential, scheme_name):
        color_scheme = getattr(brewer_sequential, scheme_name)
    elif hasattr(brewer_diverging, scheme_name):
        color_scheme = getattr(brewer_diverging, scheme_name)
    elif hasattr(brewer_qualitative, scheme_name):
        color_scheme = getattr(brewer_qualitative, scheme_name)
    else:
        raise ValueError(f"{scheme_name} is not a supported color scheme.")

    return {
        "sparkline": [
            {"offset": offset, "color": color}
            for offset, color in zip(
                ["0%", "25%", "50%", "75%", "100%"], color_scheme.hex_colors
            )
        ],
        "barchart": {
            "base": color_scheme.hex_colors[3],
            "hover": color_scheme.hex_colors[4],
        },
    }


def build_site(site_config, data_file):
    subjects = [
        assoc(s, "colors", _make_colors(s)) for s in site_config["subjects"]
    ]

    return env.get_template("profanity_power_index.html.jinja").render(
        subjects=subjects, file_location=data_file
    )
