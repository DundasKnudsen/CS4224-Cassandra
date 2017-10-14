DROP KEYSPACE IF EXISTS wholesaler;

CREATE KEYSPACE wholesaler WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '3'
};

DESC KEYSPACE wholesaler;

USE wholesaler;

CREATE TABLE warehouse (
    w_id int,
    w_name text,
    w_street_1 text,
    w_street_2 text,
    w_city text,
    w_state text,
    w_zip text,
    w_tax decimal,
    w_ytd decimal,
    PRIMARY KEY (w_id)
);

CREATE TABLE district (
    d_w_id int,
    d_id int,
    d_name text,
    d_street_1 text,
    d_street_2 text,
    d_city text,
    d_state text,
    d_zip text,
    d_tax decimal,
    d_ytd decimal,
    d_next_o_id int,
    d_next_o_id_to_deliver int,
    PRIMARY KEY ((d_w_id, d_id))
);

CREATE TABLE customer (
    c_w_id int,
    c_d_id int,
    c_id int,
    c_first text,
    c_middle text,
    c_last text,
    c_street_1 text,
    c_street_2 text,
    c_city text,
    c_state text,
    c_zip text,
    c_phone text,
    c_since timestamp,
    c_credit text,
    c_credit_limit decimal,
    c_discount decimal,
    c_balance decimal,
    c_ytd_payment float,
    c_payment_cnt int,
    c_delivery_cnt int,
    c_data text,
    PRIMARY KEY ((c_w_id, c_d_id), c_id)
);

CREATE MATERIALIZED VIEW customer_top_balance AS
    SELECT c_d_id, c_w_id, c_id, c_balance, c_first, c_middle, c_last
    FROM customer
    WHERE c_d_id IS NOT NULL AND c_w_id IS NOT NULL AND c_id IS NOT NULL AND c_balance IS NOT NULL
    PRIMARY KEY (c_w_id, c_balance, c_d_id, c_id)
    WITH CLUSTERING ORDER BY (c_balance DESC);


CREATE TABLE order (
    o_w_id int,
    o_d_id int,
    o_id int,
    o_c_id int,
    o_carrier_id int,
    o_ol_cnt decimal,
    o_all_local decimal,
    o_entry_d timestamp,
    PRIMARY KEY ((o_w_id, o_d_id), o_id)
);

CREATE TABLE item (
    i_id int,
    i_name text,
    i_price decimal,
    i_im_id int,
    i_data text,
    PRIMARY KEY (i_id)
);

CREATE MATERIALIZED VIEW order_by_customer AS
    SELECT o_w_id, o_d_id, o_id, o_c_id, o_entry_d, o_carrier_id
    FROM order
    WHERE o_w_id IS NOT NULL AND o_d_id IS NOT NULL AND o_id IS NOT NULL AND o_c_id IS NOT NULL
    PRIMARY KEY ((o_w_id, o_d_id), o_c_id, o_id)
    WITH CLUSTERING ORDER BY (o_c_id ASC, o_id DESC);

CREATE TABLE order_line (
    ol_w_id int,
    ol_d_id int,
    ol_o_id int,
    ol_number int,
    ol_i_id int,
    ol_delivery_d timestamp,
    ol_amount decimal,
    ol_supply_w_id int,
    ol_quantity decimal,
    ol_dist_info text,
    ol_i_name text,
    PRIMARY KEY ((ol_w_id, ol_d_id), ol_o_id, ol_quantity, ol_number)
) WITH CLUSTERING ORDER BY (ol_o_id DESC, ol_quantity DESC);

CREATE TABLE stock (
    s_w_id int,
    s_i_id int,
    s_quantity decimal,
    s_ytd decimal,
    s_order_cnt int,
    s_remote_cnt int,
    s_dist_01 text,
    s_dist_02 text,
    s_dist_03 text,
    s_dist_04 text,
    s_dist_05 text,
    s_dist_06 text,
    s_dist_07 text,
    s_dist_08 text,
    s_dist_09 text,
    s_dist_10 text,
    s_data text,
    s_i_name text,
    s_i_price decimal,
    PRIMARY KEY (s_w_id, s_i_id),
);