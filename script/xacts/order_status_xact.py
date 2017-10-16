def get_customer(session, w_id, d_id, c_id):
    prepared = session.prepare(
        "SELECT c_first, c_middle, c_last FROM customer \
        WHERE c_w_id = ? AND c_d_id = ? and c_id = ?"
    )
    rows = session.execute(prepared.bind((w_id, d_id, c_id)))
    return None if not rows else rows[0]


def get_last_order(session, w_id, d_id, c_id):
    prepared = session.prepare(
        "SELECT o_id, o_entry_d, o_carrier_id FROM order_by_customer \
        WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? LIMIT 1"
    )
    rows = session.execute(prepared.bind((w_id, d_id, c_id)))
    return None if not rows else rows[0]


def get_order_items(session, w_id, d_id, o_id):
    prepared = session.prepare(
        "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d \
        FROM order_line \
        WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?"
    )
    rows = session.execute(prepared.bind((w_id, d_id, int(o_id))))
    return None if not rows else rows


def order_status_xact(session, w_id, d_id, c_id):
    payload = {}

    payload['customer'] = get_customer(session, w_id, d_id, c_id)
    order = get_last_order(session, w_id, d_id, c_id)
    payload['order'] = order
    payload['items'] = None
    if order:
        payload['items'] = get_order_items(session, w_id, d_id, order.o_id)

    return payload
