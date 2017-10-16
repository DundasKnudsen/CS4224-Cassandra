import sys
import time

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.cluster import ExecutionProfile

from xacts.payment_xact import payment_xact
from xacts.top_balance import top_balance
from xacts.order_status_xact import order_status_xact
from xacts.new_order_xact import new_order_xact
from xacts.delivery_xact import delivery_xact
from xacts.popular_item_xact import popular_item_xact
from xacts.stock_level_xact import stock_level_xact


def write_summary(file_dir, transactions_count, elapsed_time, throughput):
    with open(file_dir, "a") as myfile:
        myfile.write("{0} {1} {2}\n".format(
            transactions_count, elapsed_time, throughput))


def run_xacts(session, xact_file_dir, xact_id, client_summary_file_dir):
    transactions_count = 0
    with open(xact_file_dir) as xact_file:
        lines = xact_file.readlines()

        start_time = time.time()

        for (i, line) in enumerate(lines):
            values = line.replace('\n', '').split(',')
            transactions_count += 1
            result = None

            if values[0] == 'N':
                item_number = []
                supplier_w = []
                quantity = []
                num_of_items = int(values[4])
                for j in range(i + 1, i + num_of_items + 1):
                    item_data = lines[j].replace('\n', '').split(',')
                    item_number.append(item_data[0])
                    supplier_w.append(item_data[1])
                    quantity.append(item_data[2])
                result = new_order_xact(session, int(values[1]), int(values[2]),
                                        int(values[3]), int(values[4]), item_number, supplier_w, quantity)

            elif values[0] == 'P':
                result = payment_xact(session, int(values[1]),
                                      int(values[2]), int(values[3]), values[4])
            elif values[0] == 'D':
                result = delivery_xact(session, int(values[1]), int(values[2]))
            elif values[0] == 'O':
                result = order_status_xact(
                    session, int(values[1]), int(values[2]), int(values[3]))
            elif values[0] == 'S':
                result = stock_level_xact(
                    session, int(values[1]), int(values[2]), int(values[3]), int(values[4]))
            elif values[0] == 'I':
                result = popular_item_xact(session, int(
                    values[1]), int(values[2]), int(values[3]))
            elif values[0] == 'T':
                result = top_balance(session)
            else:
                transactions_count -= 1
                continue

            sys.stderr.write(
                "\n Transaction: {0}\n {1}\n".format(line, str(result)))

    end_time = time.time()
    elapsed_time = end_time - start_time
    throughput = float(elapsed_time) / transactions_count

    sys.stderr.write("Client: {0}: Done with {1} Transactions in {2}s with throughput {3}\n".format(
        xact_id, transactions_count, elapsed_time, throughput))

    write_summary(client_summary_file_dir, transactions_count,
                  elapsed_time, throughput)


def main():
    xact_file_dir = sys.argv[1]
    xact_id = sys.argv[2]
    consistency_level_string = sys.argv[3]
    client_summary_file_dir = sys.argv[4]

    consistency_level = ConsistencyLevel.LOCAL_ONE
    if consistency_level_string == "ONE":
        consistency_level = ConsistencyLevel.ONE
    elif consistency_level_string == "QUORUM":
        consistency_level = ConsistencyLevel.QUORUM

    cluster = Cluster(control_connection_timeout=None)
    profile = ExecutionProfile(consistency_level=consistency_level)
    cluster.add_execution_profile("client", profile)
    session = cluster.connect("wholesaler")
    run_xacts(session, xact_file_dir, xact_id, client_summary_file_dir)


if __name__ == "__main__":
    main()
