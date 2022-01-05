#!/bin/bash

# Copyright 2019 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# this script brings up zookeeper and all the vitess components
# required for a single shard deployment.

# create a unshard keyspace "commerce" and a sharded keyspace "customer"

set -e

# initialize keyspace commerce
unsharded_start() {
    echo "init unsharded keyspace 'commerce'..."

    # start vttablets
    for i in 100 101 102; do
        CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
        CELL=zone1 KEYSPACE=commerce TABLET_UID=$i ./vtdriver/vttablet-up.sh
    done

    # set one of the replicas to primary
    vtctldclient InitShardPrimary --server 127.0.0.1:15999 --force commerce/0 zone1-100

    # Add remote user in MySQL
    for i in 100
    do
        mysql -hlocalhost -P $[17000 + $i] -uroot \
            --socket ../vtdataroot/vt_0000000$i/mysql.sock \
            -e "source ./vtdriver/mysql_user.sql"
    done

    # set sql_mode
    for i in 100 101 102
    do
        mysql -hlocalhost -P $[17000 + $i] -uroot \
            --socket ../vtdataroot/vt_0000000$i/mysql.sock \
            -e "set global sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';"
    done

    # Create test tables
    vtctlclient -server localhost:15999 ApplySchema \
        -sql-file ./vtdriver/commerce_unsharded.sql commerce

    # change the vschema
    vtctlclient -server localhost:15999 ApplyVSchema \
        -vschema_file ./vtdriver/vschema_commerce_unsharded.json commerce

    echo "unsharded keyspace 'commerce' initialized."
}

# initialize keyspace customer
sharded_start() {
    echo "init sharded keyspace 'customer'..."

    # start vttablets
    for i in 300 301 302; do
        CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
        SHARD=-80 CELL=zone1 KEYSPACE=customer TABLET_UID=$i ./vtdriver/vttablet-up.sh
    done

    for i in 400 401 402; do
        CELL=zone1 TABLET_UID=$i ./scripts/mysqlctl-up.sh
        SHARD=80- CELL=zone1 KEYSPACE=customer TABLET_UID=$i ./vtdriver/vttablet-up.sh
    done

    # set one of the replicas to primary
    vtctldclient --server 127.0.0.1:15999 InitShardPrimary --force customer/-80 zone1-300
    vtctldclient --server 127.0.0.1:15999 InitShardPrimary --force customer/80- zone1-400

    # Add remote user in MySQL
    for i in 300 400
    do
        mysql -hlocalhost -P $[17000 + $i] -uroot \
            --socket ../vtdataroot/vt_0000000$i/mysql.sock \
            -e "source ./vtdriver/mysql_user.sql"
    done

    # set sql_mode
    for i in 300 301 302 400 401 402
    do
        mysql -hlocalhost -P $[17000 + $i] -uroot \
            --socket ../vtdataroot/vt_0000000$i/mysql.sock \
            -e "set global sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';"
    done

    # Create test tables
    vtctlclient --server 127.0.0.1:15999 ApplySchema \
        -sql-file ./vtdriver/customer_sharded.sql customer

    # change the vschema
    vtctlclient --server 127.0.0.1:15999 ApplyVSchema \
        -vschema_file ./vtdriver/vschema_customer_sharded.json customer

    echo "two-shard keyspace 'customer' initialized."
}

#####################################################

if [ -z "${HOST_IP}" ]; then
    echo "HOST_IP not provided, will use `cat /etc/hosts | awk 'END {print}' | awk '{print $1}'`"
    export HOST_IP=`cat /etc/hosts | awk 'END {print}' | awk '{print $1}'`
fi

echo "HOST_IP=${HOST_IP}"

source ./vtdriver/env.sh

# start topo server
CELL=zone1 ./vtdriver/etcd-up.sh

# start vtctld
CELL=zone1 ./vtdriver/vtctld-up.sh

if [ -n "${ONE_SHARD_ONLY}" ]; then
    unsharded_start
elif [ -n "${TWO_SHARD_ONLY}" ]; then
    sharded_start
else
    unsharded_start
    sharded_start
fi

# start vtgate if NO_VTGATE not defined.
if [ -n "$NO_VTGATE" ]; then
    echo "NO_VTGATE defined, will not to run vtgate."
else
    CELL=zone1 ./vtdriver/vtgate-up.sh
fi
