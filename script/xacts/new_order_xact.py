from datetime import datetime


def get_and_update_district(session, w_id, d_id):
    prepared = session.prepare(
        "SELECT d_next_o_id, d_tax FROM district WHERE d_w_id = ? AND d_id = ?"
    )
    rows = session.execute(prepared.bind((w_id, d_id)))
    if not rows:
        return None
    district = rows[0]
    prepared = session.prepare(
        "UPDATE district SET d_next_o_id = ? WHERE d_w_id = ? AND d_id = ?"
    )
    session.execute(prepared.bind((int(district.d_next_o_id + 1), w_id, d_id)))
    return district


def get_customer(session, w_id, d_id, c_id):
    prepared = session.prepare(
        "SELECT c_w_id, c_d_id, c_id, c_last, c_credit, c_discount\
        FROM customer \
        WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
    )
    rows = session.execute(prepared.bind((w_id, d_id, c_id)))
    return rows[0] if rows else None


def get_w_tax(session, w_id):
    prepared = session.prepare(
        "SELECT w_tax FROM warehouse WHERE w_id = ?"
    )
    rows = session.execute(prepared.bind([w_id]))
    return rows[0].w_tax if rows else 0


def get_all_local_value(w_id, order_lines):
    for ol in order_lines:
        if w_id != ol['ol_supply_w_id']:
            return 0
    return 1


def create_order(session, w_id, d_id, c_id, o_id, num_of_items, order_lines):
    all_local = get_all_local_value(w_id, order_lines)
    o_entry_d = datetime.now()

    prepared = session.prepare(
        "INSERT INTO order_table \
        (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) \
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
    session.execute(prepared.bind((int(o_id), d_id, w_id,
                                   int(c_id), str(o_entry_d), int(num_of_items), all_local)))

    order = {
        'o_id': o_id,
        'o_entry_d': o_entry_d
    }
    return order


def update_order_line_and_stock(session, w_id, d_id, o_id, order_lines):
    total_amount = 0
    added_items = []

    item_index = 0
    for ol in order_lines:
        item_index += 1
        i_id = ol['ol_i_id']
        supply_w_id = ol['ol_supply_w_id']
        quantity = ol['ol_quantity']
        prepared = session.prepare(
            "SELECT s_i_name, s_i_price, s_quantity, s_ytd, s_order_cnt, s_remote_cnt \
            FROM stock \
            WHERE s_w_id = ? AND s_i_id = ?"
        )
        rows = session.execute(prepared.bind((int(supply_w_id), int(i_id))))
        if rows:
            stock = rows[0]
            updated_s_order_cnt = stock.s_order_cnt + 1
            updated_s_ytd = stock.s_ytd + int(quantity)
            updated_s_remote_cnt = stock.s_remote_cnt + \
                int(supply_w_id != w_id)
            adjusted_quantity = int(stock.s_quantity) - int(quantity)
            if adjusted_quantity < 10:
                adjusted_quantity += 100

            prepared = session.prepare(
                "UPDATE stock SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? \
                WHERE s_w_id = ? AND s_i_id = ?"
            )
            session.execute(prepared.bind((adjusted_quantity, updated_s_ytd,
                                           updated_s_order_cnt, updated_s_remote_cnt, int(supply_w_id), int(i_id))))

            item_amount = float(quantity) * float(stock.s_i_price)
            total_amount += item_amount

            prepared = session.prepare(
                "INSERT INTO order_line \
                (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) \
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            session.execute(prepared.bind((int(o_id), d_id, w_id, item_index,
                                           int(i_id), int(supply_w_id),
                                           int(quantity), item_amount, "S_DIST_{0}".format(d_id))))

            added_items.append({
                'i_id': i_id,
                'i_name': stock.s_i_name,
                'supplier_warehouse': supply_w_id,
                'quantity': quantity,
                'ol_amount': item_amount,
                's_quantity': stock.s_quantity,
            })

    return total_amount, added_items


def compact_order_lines(num_items, item_number, supplier_w, quantity):
    order_lines = []
    for i in range(0, num_items):
        order_lines.append({
            'ol_i_id': item_number[i],
            'ol_supply_w_id': supplier_w[i],
            'ol_quantity': quantity[i]
        })
    return order_lines


def new_order_xact(session, c_id, w_id, d_id, num_items, item_number, supplier_w, quantity):

    order_lines = compact_order_lines(
        num_items, item_number, supplier_w, quantity)
    district = get_and_update_district(session, w_id, d_id)
    customer = get_customer(session, w_id, d_id, c_id)
    if not district or not customer:
        return None

    order = create_order(session, w_id, d_id, c_id,
                         district.d_next_o_id, num_items, order_lines)

    total_amount, added_items = update_order_line_and_stock(
        session, w_id, d_id, district.d_next_o_id, order_lines)

    d_tax = district.d_tax
    w_tax = get_w_tax(session, w_id)
    total_amount *= (1 + float(d_tax) + float(w_tax)) * \
        (1 - float(customer.c_discount))

    result = {
        'customer': customer,
        'w_tax': w_tax,
        'd_tax': d_tax,
        'order': order,
        'num_items': num_items,
        'total_amount': total_amount,
        'items': added_items
    }

    return result
