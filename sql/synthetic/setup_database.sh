#!/bin/sh

DBNAME=synthetic_db
USERNAME=$1
PASSWORD=$2
export PGPASSWORD=$PASSWORD

# create symlink to tmp folder (load_tpch.sql needs absolute paths)
unlink /tmp/synthetic-data
ln -s "$PWD/data" /tmp/synthetic-data

# create tables, populate with data 
echo "  drop tables if exist"
psql -h localhost -U $USERNAME $DBNAME < ./drop.sql

echo "  setup schema"
psql -h localhost -U $USERNAME $DBNAME < ./schema.sql

# comment this out if you don't want to create new data
echo "  create data"
python3 create_csv.py

echo "  load data"
psql -h localhost -U $USERNAME $DBNAME < ./load.sql
echo "finished database setup"
