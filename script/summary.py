import sys

from cassandra.cluster import Cluster


def get_db_state():
    # cluster = Cluster(control_connection_timeout=None)
    # session = cluster.connect("wholesaler")

    # result = {}
    # rows = session.execute("SELECT sum(w_ytd) FROM warehouse")
    # result['warehouse'] = rows[0][0] if rows else 0

    # rows = session.execute("SELECT sum(d_ytd), sum(d_next_o_id) FROM district")
    # result['district'] = None
    # if rows:
    #     result['warehouse'] = {
    #         'sum_ytd': rows[0][0],
    #         'sum_next_o_id': rows[0][1],
    #     }

    # rows = session.execute(
    #     "SELECT sum(c_balance), sum(c_ytd_payment), sum(c_payment_cnt), sum(c_delivery_cnt) FROM customer")
    # result['customer'] = None
    # if rows:
    #     result['customer'] = {
    #         'sum_balance': rows[0][0],
    #         'sum_ytd_payment': rows[0][1],
    #         'sum_payment_cnt': rows[0][2],
    #         'sum_delivery_cnt': rows[0][3],
    #     }

    # rows = session.execute("SELECT sum(o_id), sum(o_ol_cnt) FROM order_table")
    # result['order'] = None
    # if rows:
    #     result['order'] = {
    #         'sum_id': rows[0][0],
    #         'sum_ol_cnt': rows[0][1],
    #     }

    # rows = session.execute(
    #     "SELECT sum(ol_amount), sum(ol_quantity) FROM order_line")
    # result['order-line'] = None
    # if rows:
    #     result['order-line'] = {
    #         'sum_ol_amount': rows[0][0],
    #         'sum_ol_quantity': rows[0][1],
    #     }

    # rows = session.execute(
    #     "SELECT sum(s_quantity), sum(s_ytd), sum(s_order_cnt), sum(s_remote_cnt) FROM stock")
    # result['stock'] = None
    # if rows:
    #     result['stock'] = {
    #         'sum_quantity': rows[0][0],
    #         'sum_ytd': rows[0][1],
    #         'sum_order_cnt': rows[0][2],
    #         'sum_remote_cnt': rows[0][3],
    #     }
    # return result
    return {
        'db_state': None
    }


def main():
    stat_file_dir = sys.argv[1]
    min_throughput = 1000000000
    max_throughput = -1
    total_throughput = 0
    total_client = 0

    with open(stat_file_dir) as stat_file:
        lines = stat_file.readlines()
        for line in lines:
            total_client += 1
            values = line.replace('\n', '').split(' ')
            transactions_count = int(values[0])
            elapsed_time = float(values[1])
            throughput = elapsed_time / transactions_count
            min_throughput = min(min_throughput, throughput)
            max_throughput = max(max_throughput, throughput)
            total_throughput += throughput

    print "Min throughput ", min_throughput
    print "Max throughput ", max_throughput
    print "Average throughpput ", total_throughput / total_client
    db_state = get_db_state()
    print str(db_state)


if __name__ == "__main__":
    main()
