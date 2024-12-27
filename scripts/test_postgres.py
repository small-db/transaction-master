#!/usr/bin/env python3
import logging
import os
import time
import xiaochen_py
import psycopg2
import yaml
import threading
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

DB_CONFIG = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": "",
    "host": "localhost",
    "port": 5432,
}


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
    # clear previous data
    case_teardown(DB_CONFIG, case["teardown"])

    case_setup(DB_CONFIG, case["setup"])
    case_run(DB_CONFIG, case["events"])
    case_teardown(DB_CONFIG, case["teardown"])


def case_setup(setup_statements):
    conn = psycopg2.connect(**DB_CONFIG)

    for stat in setup_statements:
        with conn:
            with conn.cursor() as curs:
                curs.execute(stat)

    conn.close()


def case_run(events):
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
        logging.info(f"==== executing events at timestamp {ts}")
        for event in grouped_events[ts]:
            session = sessions[event["session"]]
            session.push_task(event["statements"])

        for name, session in sessions.items():
            session.wait_for_completion()

    time.sleep(3)

    # exit all sessions
    for name, session in sessions.items():
        session.exit()


class Session:
    def __init__(self):
        self.thread = None
        self.queue = []
        self.running = False

    def start(self):
        # the running flag must be set before the thread starts to meet the
        # requirement of the "run" method.
        self.running = True

        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def run(self):
        conn = psycopg2.connect(**DB_CONFIG)
        curser = conn.cursor()

        while self.running:
            if len(self.queue) == 0:
                time.sleep(0.1)
                continue

            statement = self.queue.pop(0)
            logging.info(f"executing statement: {statement}")
            # curser.execute(stat)

        curser.close()
        conn.close()

    def push_task(self, statement):
        self.queue.append(statement)

    def wait_for_completion(self):
        while len(self.queue) > 0:
            time.sleep(0.1)

    def exit(self):
        self.running = False
        if self.thread:
            self.thread.join()


def case_teardown(teardown_statements):
    conn = psycopg2.connect(**DB_CONFIG)

    for stat in teardown_statements:
        with conn:
            with conn.cursor() as curs:
                curs.execute(stat)

    conn.close()


if __name__ == "__main__":
    setup()
    test()
