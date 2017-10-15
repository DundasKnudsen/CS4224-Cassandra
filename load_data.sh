#!/usr/bin/env bash

# Pass the csv files folder as the first args. Note that the data path must be absolute path, which can easily archive by using `pwd`
# Replace all null value with empty

echo "---------------------Start loading"

echo "---------------------Import schema into db"
/temp/apache-cassandra/bin/cqlsh -f schema.sql

echo "---------------------Adding ol-i-name into order-line..."

# Create tmp_order_line.csv with new value ol_i_name on the row 11
join -a 1 -j 1 -t ',' -o 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 2.2 -e "null" <(paste -d',' <(cut -d',' --output-delimiter=- -f5 $1/order-line.csv) $1/order-line.csv | sort -t',' -k1,1) <(cat $1/item.csv | sort -t',' -k1,1) > $1/tmp-order-line.csv

echo "---------------------Adding s-i-name and s-i-price into stock..."

# Create tmp_stock.csv with new value s_i_name, s_i_price on the row 18, 19
join -a 1 -j 1 -t ',' -o 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 1.12 1.13 1.14 1.15 1.16 1.17 1.18 2.2 2.3 -e "null" <(paste -d',' <(cut -d',' --output-delimiter=- -f2 $1/stock.csv) $1/stock.csv | sort -t',' -k1,1) <(cat $1/item.csv | sort -t',' -k1,1) > $1/tmp-stock.csv


echo "---------------------Replacing null by empty in all csv files..."

for f in $1/*.csv
do
	sed -i -e 's/,null,/,,/g' -e 's/^null,/,/' -e 's/,null$/,/' $f
done


echo "---------------------Start import csv files into db"
echo "--importing warehouse--"
echo "COPY wholesaler.warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) FROM '$1/warehouse.csv' WITH DELIMITER=',';" | /temp/apache-cassandra/bin/cqlsh

echo "--importing district--"
echo "COPY wholesaler.district (d_w_id, d_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) FROM '$1/district.csv' WITH DELIMITER=',';" | /temp/apache-cassandra/bin/cqlsh

echo "--importing customer--"
echo "COPY wholesaler.customer (c_w_id, c_d_id, c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_limit, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) FROM '$1/customer.csv' WITH DELIMITER=',';" | /temp/apache-cassandra/bin/cqlsh

echo "--importing order--"
echo "COPY wholesaler.order_table (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) FROM '$1/order.csv' WITH DELIMITER=',';" | /temp/apache-cassandra/bin/cqlsh


echo "--importing item--"
echo "COPY wholesaler.item (i_id, i_name, i_price, i_im_id, i_data) FROM '$1/item.csv' WITH DELIMITER=',';" | /temp/apache-cassandra/bin/cqlsh

echo "--importing order line--"
echo "COPY wholesaler.order_line (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info, ol_i_name) FROM '$1/tmp-order-line.csv' WITH DELIMITER=',';" | /temp/apache-cassandra/bin/cqlsh

echo "--importing stock--"
echo "COPY wholesaler.stock (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data, s_i_name, s_i_price) FROM '$1/tmp-stock.csv' WITH DELIMITER=',';" | /temp/apache-cassandra/bin/cqlsh


# load data
echo "---------------------Fix db"
python ./script/data.py $1

echo "---------------------Done."
