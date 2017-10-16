def get_top_balance_customers(session, d_id, limit):
    prepared = session.prepare(
        "SELECT c_first, c_middle, c_last, c_balance, c_w_id, c_d_id \
        FROM customer_top_balance \
        WHERE c_d_id = ? LIMIT ?"
    )
    rows = session.execute(prepared.bind((int(d_id), int(limit))))
    return list(rows)


def get_district_name(session, d_id):
    prepared = session.prepare("SELECT d_name FROM district WHERE d_id = ?")
    rows = session.execute(prepared.bind([int(d_id)]))
    return None if not rows else rows[0].d_name


def get_warehouse_name(session, w_id):
    prepared = session.prepare("SELECT w_name FROM warehouse WHERE w_id = ?")
    rows = session.execute(prepared.bind([int(w_id)]))
    return None if not rows else rows[0].w_name


def customer_compare(c1, c2):
    return int(c2.c_balance - c1.c_balance)


def top_balance(session):
    limit = 10
    customers = []
    for d_id in range(1, 11):
        customers = customers + get_top_balance_customers(session, d_id, limit)

    customers.sort(cmp=customer_compare)
    result = []
    for customer in customers[0:limit]:
        data = {
            'c_first': customer.c_first,
            'c_middle': customer.c_middle,
            'c_last': customer.c_last,
            'c_balance': customer.c_balance,
            'w_name': get_warehouse_name(session, customer.c_w_id),
            'd_name': get_district_name(session, customer.c_d_id)
        }
        result.append(data)
    return result
