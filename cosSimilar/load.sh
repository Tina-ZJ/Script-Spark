#!/bin/sh

sql_file='creat.sql'
data_file='cid3.top200'
table_name='app.app_algo_recallrelation_cid_top200_score'

#create table
hive -f ${sql_file}

hive -e """
use app;
load data local inpath '${data_file}'
overwrite into table ${table_name}
partition(dt='active');
"""
