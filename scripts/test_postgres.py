#!/usr/bin/env python3
import os
import time
import xiaochen_py
import psycopg2
import yaml
from scripts import config

DATA_DIR = os.path.join(config["DATA_ROOT"], "postgres")
DB_NAME = "db"
DB_USER = "postgres"
CONTAINER_NAME = "postgres"


def setup():
    xiaochen_py.run_command(f"docker rm -f {CONTAINER_NAME}")
    # xiaochen_py.run_command(f"sudo rm -rf {DATA_DIR}")
    xiaochen_py.run_command(
        f"""
        docker run --detach \
            --name {CONTAINER_NAME} \
            -e POSTGRES_DB={DB_NAME} \
            -e POSTGRES_HOST_AUTH_METHOD=trust \
            -p 127.0.0.1:5432:5432 \
            -v {DATA_DIR}:/var/lib/postgresql/data \
            postgres:17 \
            -c max_connections=1000
        """
    )
    wait_for_service()


def wait_for_service():
    conn = None
    while conn is None:
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password="", host="localhost", port=5432
            )
        except psycopg2.OperationalError:
            time.sleep(1)
    conn.close()


def test():
    anomalies_dir = "./anomalies"
    for filename in os.listdir(anomalies_dir):
        if filename.endswith(".yaml"):
            with open(os.path.join(anomalies_dir, filename), "r") as file:
                data = yaml.safe_load(file)
                print(os.path.splitext(filename)[0])
                print(data)


if __name__ == "__main__":
    setup()
    test()
