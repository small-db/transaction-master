import logging

import psycopg2
from scripts import test_postgres


def check_postgres():
    # conn = psycopg2.connect(**test_postgres.DB_CONFIG)
    # cursor = conn.cursor()
    # cursor.execute("SELECT current_setting('transaction_isolation')")
    # for record in cursor.fetchall():
    #     logging.info(f"transaction_isolation: {record}")
    # conn.close()

    test_postgres.set_isolation_level("read uncommitted")
    test_postgres.set_isolation_level("read committed")
    test_postgres.set_isolation_level("repeatable read")
    test_postgres.set_isolation_level("serializable")


if __name__ == "__main__":
    check_postgres()
