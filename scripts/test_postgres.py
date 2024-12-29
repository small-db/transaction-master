#!/usr/bin/env python3
import logging
import os
import time
import xiaochen_py
import psycopg2
import yaml
import threading
import csv
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


SUPPORTED_ISOLATION_LEVELS = [
    "read uncommitted",
    "read committed",
    "repeatable read",
    "serializable",
]


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


def read_csv_to_2d_array(filepath):
    with open(filepath, mode="r") as file:
        reader = csv.reader(file)
        data = [row for row in reader]
    return data


def test():
    # load expected behavior
    expected_behavior_csv = "./spec/expected/postgres.csv"
    expected_behavior = read_csv_to_2d_array(expected_behavior_csv)

    # load anomalies
    anomalies_dir = "./spec/anomalies"
    anomalies = dict()
    for filename in os.listdir(anomalies_dir):
        if filename.endswith(".yaml"):
            with open(os.path.join(anomalies_dir, filename), "r") as file:
                data = yaml.safe_load(file)
                name = data["name"]
                cases = data["cases"]
                anomalies[name] = cases

    # run test cases and compare with expected behavior
    pg_isolation_levels = [row[0] for row in expected_behavior[1:]]
    pg_anomalies = expected_behavior[0][1:]

    # init possible anomalies
    # key: isolation level
    # value: list of possible anomalies
    possible_anomalies = defaultdict(list)
    for row in expected_behavior[1:]:
        isolation_level = row[0]
        for i, possibility in enumerate(row[1:]):
            anomaly_name = pg_anomalies[i]
            if possibility == "Possible":
                possible_anomalies[isolation_level].append(anomaly_name)

    for isolation_level in pg_isolation_levels:
        set_isolation_level(isolation_level)
        for anomaly_name in pg_anomalies:
            logging.info("========================================")
            logging.info(
                f'test anomaly "{anomaly_name}" with isolation level "{isolation_level}"'
            )

            possible = False
            if anomaly_name in possible_anomalies[isolation_level]:
                possible = True

            if anomaly_name in anomalies:
                cases = anomalies[anomaly_name]
                for case in cases:
                    run_case(case, possible)
            else:
                logging.error(f"test cases for anomaly {anomaly_name} not found")


def run_case(case, possible: bool):
    # clear previous data
    case_teardown(case["teardown"])

    set_isolation_level("read uncommitted")

    case_setup(case["setup"])
    case_run(case["events"], possible)
    case_teardown(case["teardown"])


def set_isolation_level(level):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(
        f"ALTER DATABASE {DB_NAME} SET default_transaction_isolation TO '{level}'"
    )
    conn.close()


def case_setup(setup_statements):
    conn = psycopg2.connect(**DB_CONFIG)

    for stat in setup_statements:
        with conn:
            with conn.cursor() as curs:
                curs.execute(stat)

    conn.close()


def case_run(events, possible: bool):
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
            session.push_task(event["statements"], possible)

        for name, session in sessions.items():
            error = session.wait_for_completion()
            if error:
                logging.error(f"error in session {name}: {error}")
                exit_all_sessions(sessions)
                raise error

    time.sleep(3)

    exit_all_sessions(sessions)


def exit_all_sessions(sessions):
    for name, session in sessions.items():
        session.exit()


class Session:
    def __init__(self):
        self.thread = None
        self.queue = []
        self.running = False
        self.error = None

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
            sql = statement["sql"]
            curser.execute(sql)
            if curser.statusmessage.startswith("SELECT"):
                for record in curser.fetchall():
                    try:
                        check_result(statement, record)
                    except Exception as e:
                        self.error = e
                        break
                    logging.info(f"output: {record}")

        curser.close()
        conn.close()

    def push_task(self, statements, possible: bool):
        for stmt in statements:
            stmt["possible"] = possible
            self.queue.append(stmt)

    def wait_for_completion(self) -> Exception:
        while len(self.queue) > 0:
            if self.error:
                return self.error
            time.sleep(0.1)
        return self.error

    def exit(self):
        self.running = False
        if self.thread:
            self.thread.join()


def check_result(statement, record):
    if statement["possible"]:
        # expect "abomalous_results"
        expected = statement["anomalous_results"]
        got = record
        assert got == expected, f"expected {expected}, got {got}"
    else:
        # expect "legal_results"
        expected = statement["legal_results"]
        got = record
        assert got == expected, f"expected {expected}, got {got}"


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
