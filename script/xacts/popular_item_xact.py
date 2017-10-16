def get_orders(session, w_id, d_id, order_limit):
    prepared = session.prepare(
        "SELECT o_id, o_entry_d, o_c_id FROM order_table \
        WHERE o_w_id = ? AND o_d_id = ? \
        LIMIT ?"
    )
    return list(session.execute(prepared.bind([w_id, d_id, int(order_limit)])))


def get_most_popular_items(session, w_id, d_id, o_id):
    item_highest_quantity = get_item_highest_quantity(
        session, w_id, d_id, o_id)
    prepared = session.prepare(
        "SELECT ol_i_id, ol_quantity, ol_i_name \
        FROM order_line \
        WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_quantity = ?"
    )
    rows = session.execute(prepared.bind(
        (w_id, d_id, int(o_id), item_highest_quantity)))

    return rows


def get_item_highest_quantity(session, w_id, d_id, o_id):
    prepared = session.prepare(
        "SELECT ol_quantity \
        FROM order_line \
        WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? LIMIT 1"
    )
    rows = session.execute(prepared.bind((w_id, d_id, int(o_id))))
    return rows[0].ol_quantity if rows else 0


def get_customer(session, w_id, d_id, c_id):
    prepared = session.prepare(
        "SELECT c_first, c_middle, c_last \
        FROM customer \
        WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
    )
    rows = session.execute(prepared.bind((w_id, d_id, int(c_id))))
    return rows[0] if rows else None


def popular_item_xact(session, w_id, d_id, last_order_count):

    last_orders = get_orders(session, w_id, d_id, last_order_count)
    order_count = len(last_orders)

    orders_data = []
    set_item = set()
    map_item = {}

    for order in last_orders:
        popular_items = get_most_popular_items(session, w_id, d_id, order.o_id)
        items_data = []
        for item in popular_items:
            set_item.add(item.ol_i_id)
            if item.ol_i_id in map_item:
                map_item[item.ol_i_id]['count'] += 1
            else:
                map_item[item.ol_i_id] = {
                    'name': item.ol_i_name,
                    'count': 1
                }

            items_data.append({
                'quantity': item.ol_quantity,
                'item_name': item.ol_i_name,
            })

        orders_data.append({
            'order': order,
            'customer': get_customer(session, w_id, d_id, order.o_c_id),
            'items': items_data
        })

    items_percentage = []
    for item in set_item:
        items_percentage.append({
            'name': item['name'],
            'percentage': float(item['count']) / order_count
        })

    result = {
        'w_id': w_id,
        'd_id': d_id,
        'last_order_count': last_order_count,
        'order': orders_data,
        'item_percentage': items_percentage
    }
    return result
