import yaml


def load_config():
    with open("./scripts/config.yaml", "r") as file:
        return yaml.safe_load(file)


config = load_config()
