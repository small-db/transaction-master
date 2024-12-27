#!/usr/bin/env python3
import logging
import os
import time
import xiaochen_py
import psycopg2
import yaml
from scripts import config
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

DATA_DIR = os.path.join(config["DATA_ROOT"], "postgres")
DB_NAME = "db"
DB_USER = "postgres"
CONTAINER_NAME = "postgres"


def setup():
    xiaochen_py.run_command(f"docker rm -f {CONTAINER_NAME}", slient=True)
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
        """,
        slient=True,
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
                cases = data["cases"]
                for case in cases:
                    run_case(case)


def run_case(case):
    db_config = {
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": "",
        "host": "localhost",
        "port": 5432,
    }

    # clear previous data
    case_teardown(db_config, case["teardown"])

    case_setup(db_config, case["setup"])
    case_run(db_config, case["events"])
    case_teardown(db_config, case["teardown"])


def case_setup(db_config, setup_statements):
    conn = psycopg2.connect(**db_config)

    for stat in setup_statements:
        with conn:
            with conn.cursor() as curs:
                curs.execute(stat)

    conn.close()


def case_run(db_config, events):
    # collect all sessions
    session_names = []
    for event in events:
        name = event["session"]
        if name not in session_names:
            session_names.append(name)

    # collect all timestamps
    timestamps = []
    for event in events:
        t = event["timestamp"]
        if t not in timestamps:
            timestamps.append(t)

    # group events by timestamp
    grouped_events = defaultdict(list)
    for event in events:
        grouped_events[event["timestamp"]].append(event)

    # init all sessions
    sessions = dict()
    for name in session_names:
        sessions[name] = Session()

    # start all sessions
    for name, session in sessions.items():
        session.start()

    # execute all events
    for ts in timestamps:
        logging.info(f"executing events at timestamp {ts}")
        for event in grouped_events[ts]:
            logging.info(f"executing event: {event}")
            session = sessions[event["session"]]


class Session:
    def start(self):
        pass

    def execute(self, statement):
        pass

    def exit(self):
        pass


def case_teardown(db_config, teardown_statements):
    conn = psycopg2.connect(**db_config)

    for stat in teardown_statements:
        with conn:
            with conn.cursor() as curs:
                curs.execute(stat)

    conn.close()


if __name__ == "__main__":
    setup()
    test()
