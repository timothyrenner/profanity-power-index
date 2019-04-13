from setuptools import find_packages, setup


setup(
    name="profanity-power-index",
    url="https://github.com/timothyrenner/profanity-power-index",
    packages=find_packages(exclude=["site_configs", "data"]),
    license="MIT",
    install_requires=[
        "click",
        "toolz",
        "elasticsearch>=6.0.0,<7.0.0",
        "loguru",
        "python-dotenv",
        "python-twitter",
        "jinja2",
        "sh",
        "importlib_resources",
    ],
    entry_points={
        "console_scripts": [
            "profanity-power-index=profanity_power_index.cli:main"
        ]
    },
)
