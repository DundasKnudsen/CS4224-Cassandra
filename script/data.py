#!/usr/bin/env python


import sys
import csv

from cassandra.cluster import Cluster


def load_order_data(data_dir):
    data = {}
    with open("{0}/order.csv") as csv_file:
        reader = csv.reader(csv_file, delimiter=",")
        for line in reader:
            _id = line[0] + "-" + line[1]
            if line[4] == "":
                data[_id] += 1
    return data


def fix_district(next_o_id_map, session):
    rows = session.execute("SELCT * FROM district")
    for row in rows:
        _id = row[0] + "-" + row[1]
        session.execute(
            "UPDATE warehouse \
            SET d_next_o_id_to_deliver={0} \
            WHERE d_w_id={1} AND d_id={2} \
        ".format(next_o_id_map[_id], row[0], row[1]))


def main():
    data_dir = sys.argv[1]
    cluster = Cluster()
    session = cluster.connect()

    session.execute("USE wholesaler")

    print "Importing warehouse..."
    session.execute(
        "COPY wholesaler.warehouse FROM '{0}/warehouse.csv' WITH DELIMITER=','".format(data_dir))

    print "Importing district..."
    session.execute(
        "COPY wholesaler.district FROM '{0}/district.csv' WITH DELIMITER=','".format(data_dir))

    print "Importing customer..."
    session.execute(
        "COPY wholesaler.customer FROM '{0}/customer.csv' WITH DELIMITER=','".format(data_dir))

    print "Importing order..."
    session.execute(
        "COPY wholesaler.order_table FROM '{0}/order.csv' WITH DELIMITER=','".format(data_dir))

    print "Importing order-line..."
    session.execute(
        "COPY wholesaler.order_line FROM '{0}/tmp-order-line.csv' WITH DELIMITER=','".format(data_dir))

    print "Importing item..."
    session.execute(
        "COPY wholesaler.item FROM '{0}/item.csv' WITH DELIMITER=','".format(data_dir))

    print "Importing stock..."
    session.execute(
        "COPY wholesaler.stock FROM '{0}/tmp-stock.csv' WITH DELIMITER=','".format(data_dir))

    print "Loading.. data of order to prepare for d_next_o_id_to_deliver value of district..."
    next_o_id_map = load_order_data(data_dir)

    print "Fixing district with value of d_next_o_id_to_deliver..."
    fix_district(next_o_id_map, session)

    print "Done."


if __name__ == "__main__":
    main()
