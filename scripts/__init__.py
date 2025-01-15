import yaml

import logging

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def load_config():
    with open("./scripts/config.yaml", "r") as file:
        return yaml.safe_load(file)


config = load_config()
