def get_d_next_o_id(session, w_id, d_id):
    prepared = session.prepare(
        "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?")
    rows = session.execute(prepared.bind((int(w_id), int(d_id))))

    return None if not rows else rows[0].d_next_o_id


def get_item_ids(session, w_id, d_id, min_o_id):
    prepared = session.prepare(
        "SELECT ol_i_id FROM order_line \
        WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id >= ?"
    )
    rows = session.execute(prepared.bind(
        (int(w_id), int(d_id), int(min_o_id))))
    return list(set([row.ol_i_id for row in rows]))


def get_stock_bellow_threshold(session, w_id, item_ids, threshold):
    prepared = session.prepare(
        "SELECT s_quantity FROM stock \
        WHERE s_w_id = ? AND s_i_id = ?"
    )
    count = 0
    for i_id in item_ids:
        rows = session.execute(prepared.bind((int(w_id), int(i_id))))
        if rows and rows[0].s_quantity < threshold:
            count += 1

    return count


def stock_level_xact(session, w_id, d_id, threshold, last_order_count):
    d_next_o_id = get_d_next_o_id(session, w_id, d_id)
    if not d_next_o_id:
        return 0

    item_ids = get_item_ids(session, w_id, d_id,
                            d_next_o_id - last_order_count)

    return get_stock_bellow_threshold(session, w_id, item_ids, threshold)
