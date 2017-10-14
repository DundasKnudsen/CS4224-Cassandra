#!/usr/bin/env bash

# Pass the csv files folder as the first args. Note that the data path must be absolute path, which can easily archive by using `pwd`
# Replace all null value with empty

echo "---------------------Start loading"

echo "---------------------Import schema into db"
/temp/apache-cassandra/bin/cqlsh -f schema.sql

echo "---------------------Replacing null by empty in all csv files..."

for f in $1/*.csv
do
	sed -i -e 's/,null,/,,/g' -e 's/^null,/,/' -e 's/,null$/,/' $f
done

echo "---------------------Adding ol-i-name into order-line..."

# Create tmp_order_line.csv with new value ol_i_name on the row 11
join -a 1 -j 1 -t ',' -o 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 2.2 -e "null" <(paste -d',' <(cut -d',' --output-delimiter=- -f5 $1/order-line.csv) $1/order-line.csv | sort -t',' -k1,1) <(cat $1/item.csv | sort -t',' -k1,1) > $1/tmp-order-line.csv

echo "---------------------Adding s-i-name and s-i-price into stock..."

# Create tmp_stock.csv with new value s_i_name, s_i_price on the row 18, 19
join -a 1 -j 1 -t ',' -o 1.2 1.3 1.4 1.5 1.6 1.7 1.8 1.9 1.10 1.11 1.12 1.13 1.14 1.15 1.16 1.17 2.2 2.3 -e "null" <(paste -d',' <(cut -d',' --output-delimiter=- -f2 $1/stock.csv) $1/stock.csv | sort -t',' -k1,1) <(cat $1/item.csv | sort -t',' -k1,1) > $1/tmp-stock.csv

echo "---------------------Start import csv files into db"
# load data
python ./script/data.py $1 > log/load_data.txt

echo "---------------------Done."
