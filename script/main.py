import sys
import time

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster

import xacts.payment_xact


def write_summary(file_dir, transactions_count, elapsed_time):
    with open(file_dir, "a") as myfile:
        myfile.write("{0} {1}\n".format(transactions_count, elapsed_time))


def run_xacts(session, xact_file_dir, xact_id, client_summary_file_dir):
    transactions_count = 0
    with open(xact_file_dir) as xact_file:
        xacts = xact_file.readlines()

        start_time = time.time()

        for (i, xact) in enumerate(xacts):
            values = xact.replace('\n', '').split(',')

            result = None

            if values[0] == 'N':

            elif values[0] == 'P':
                result = payment_xact.payment_xact(
                    session, values[1], values[2], values[3], values[4])
            elif values[0] == 'D':
            elif values[0] == 'O':
            elif values[0] == 'S':
            elif values[0] == 'I':
            elif values[0] == 'T':
            else:
                transactions_count -= 1

            sys.stderr.write(
                "\n Transaction: {0}\n {1}\n".format(xact, result))

    end_time = time.time()
    elapsed_time = end_time - start_time

    sys.stderr.write("Client: {0}: Done with {1} Transactions in {2}s\n".format(
        xact_id, transactions_count, end_time - start_time))

    write_summary(client_summary_file_dir, transactions_count, elapsed_time)


def main():
    cluster = Cluster(control_connection_timeout=None)
    session = cluster.connect()
    xact_file_dir = sys.argv[1]
    xact_id = sys.argv[2]
    client_summary_file_dir = sys.argv[3]
    run_xacts(session, xact_file_dir, xact_id)


if __name__ == "__main__":
    main()
