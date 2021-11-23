#!/bin/bash

# add user to mysql for vtdriver.
# Run this script in the vtdriver config director

echo "Add remote user in MySQL for vtdriver..."
for i in 100 300 400
do
    mysql -hlocalhost -P $[17000 + $i] -uroot \
        --socket ../vtdataroot/vt_0000000$i/mysql.sock \
        -e "source mysql_user.sql"
done


for i in 100 101 102 300 301 302 400 401 402
do
    mysql -hlocalhost -P $[17000 + $i] -uroot \
        --socket ../vtdataroot/vt_0000000$i/mysql.sock \
        -e "set global sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';"
done
