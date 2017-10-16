from decimal import *


def update_warehouse(session, w_id, payment):
    prepared = session.prepare(
        "SELECT w_ytd, w_street_1, w_street_2, w_city, w_state, w_zip FROM warehouse WHERE w_id = ?")
    rows = session.execute(prepared.bind([w_id]))

    if not rows:
        return None

    data = rows[0]
    updated_w_ytd = data.w_ytd + Decimal(payment)
    prepared = session.prepare("UPDATE warehouse SET w_ytd = ? WHERE w_id = ?")
    session.execute(prepared.bind((updated_w_ytd, w_id)))

    return data


def update_district(session, w_id, d_id, payment):
    prepared = session.prepare(
        "SELECT d_ytd, d_street_1, d_street_2, d_city, d_state, d_zip FROM district WHERE d_w_id = ? AND d_id = ?")
    rows = session.execute(prepared.bind((w_id, d_id)))

    if not rows:
        return None

    data = rows[0]
    updated_d_ytd = data.d_ytd + Decimal(payment)
    prepared = session.prepare(
        "UPDATE district SET d_ytd = ? WHERE d_w_id = ? AND d_id = ?")
    session.execute(prepared.bind((updated_d_ytd, w_id, d_id)))

    return data


def update_customer(session, w_id, d_id, c_id, payment):
    prepared = session.prepare(
        "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, \
        c_phone, c_since, c_credit, c_credit_limit, c_discount, c_balance, c_ytd_payment, \
        c_payment_cnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
    )
    rows = session.execute(prepared.bind((w_id, d_id, c_id)))

    if not rows:
        return None

    data = rows[0]
    updated_c_balance = data.c_balance - Decimal(payment)
    updated_c_ytd_payment = data.c_ytd_payment + Decimal(payment)
    updated_c_payment_cnt = data.c_payment_cnt + 1

    prepared = session.prepare(
        """UPDATE customer SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? WHERE 
                c_w_id = ? AND c_d_id = ? AND c_id = ?
        """)
    session.execute(prepared.bind((updated_c_balance, updated_c_ytd_payment,
                                   updated_c_payment_cnt, w_id, d_id, c_id)))

    data.c_balance = updated_c_balance
    return data


def payment_xact(session, w_id, d_id, c_id, payment):
    payload = {}

    payload['warehouse'] = update_warehouse(session, w_id, payment)
    payload['district'] = update_district(session, w_id, d_id, payment)
    payload['customer'] = update_customer(session, w_id, d_id, c_id, payment)
    payload['payment'] = payment

    return payload
