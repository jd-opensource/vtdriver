#!/bin/bash

# this script create test tables and change the vschema for vtdriver testcase.
# Run this script in the vtdriver config directory
# Before executing the script, please make sure that ‘vtctlclient’ already exists in the environment variable.

echo "Create test tables and change the vschema for vtdriver testcase..."

vtctlclient -server localhost:15999 ApplySchema \
    -sql-file `pwd`/customer_sharded.sql customer

vtctlclient -server localhost:15999 ApplyVSchema \
    -vschema_file `pwd`/vschema_customer_sharded.json customer

vtctlclient -server localhost:15999 ApplySchema \
    -sql-file `pwd`/commerce_unsharded.sql commerce

vtctlclient -server localhost:15999 ApplyVSchema \
    -vschema_file `pwd`/vschema_commerce_unsharded.json commerce
