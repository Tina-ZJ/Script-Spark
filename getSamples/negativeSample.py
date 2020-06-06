# -*-encoding: utf-8 -*-

import argparse
import collections
import datetime
import math
import os
import random
import sys
import time

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import Row
from pyspark.sql import HiveContext
from pyspark.sql.types import *

DBPATH = 'hdfs://ns1013/user/recsys/suggest/app.db'
table_name = 'app_algo_query_negative_similar_sample'
#table_name = 'app_algo_query_negative'

def create_table():
    create_table_sql = """
    CREATE EXTERNAL TABLE IF NOT EXISTS {table_name}
    (
        query_1 string                    comment 'query 1',
        query_2 string                    comment 'query 2',
        label int                        comment '类别'
    )
    PARTITIONED BY (
        dt string,
        version string
    )
    STORED AS ORC
    LOCATION '{SUGGEST_DBPATH}/{table_name}'
    TBLPROPERTIES ("orc.compress"="SNAPPY")
    """.format(SUGGEST_DBPATH=DBPATH, table_name=table_name)
    print("create_table_sql:" + create_table_sql)
    hiveCtx.sql(create_table_sql)


def negative_sampling(dt, version):
    select_sql = """
        select distinct name
        from app.app_algo_graph_path
        where dt='{dt}' and version='{version}'
    """.format(dt=dt, version=version)
    name_rdd = hiveCtx.sql(select_sql).rdd
    name_rdd.cache()
    id_name_rdd = name_rdd.zipWithIndex().map(lambda x: (x[1], x[0][0]))

    id_sql = """
        select count(distinct name) as count
        from app.app_algo_graph_path
        where dt='{dt}' and version='{version}'
    """.format(dt=dt, version=version)
    nums = hiveCtx.sql(id_sql).rdd.collect()

    select_sql = """
        select distinct name
        from app.app_algo_graph_path
        where dt='{dt}' and version='{version}'
    """.format(dt=dt, version=version)
    distinct_name_rdd = hiveCtx.sql(select_sql).rdd

    name_numberlist_rdd = distinct_name_rdd.map(lambda x: (x.name, [random.randint(1, nums[0][0]) for _ in range(5)])).\
        flatMap(lambda x: ((x[1][0], x[0]), (x[1][1], x[0]), (x[1][2], x[0]), (x[1][3], x[0]), (x[1][4], x[0])))
    
    negative_sample_rdd = name_numberlist_rdd.join(id_name_rdd).map(lambda x: (x[1][0], x[1][1], 0))
    negative_sample_rdd.toDF(["query_1","query_2","label"]).registerTempTable("temp")
    insert_sql = """
        insert overwrite table {table_name} partition(dt='{dt}', version='{version}')
        select * from temp
        """.format(table_name=table_name, dt=dt, version=version)
    print(insert_sql)
    hiveCtx.sql(insert_sql) 

def main_func(rows):
    for row in rows:
        query_list = row.querys.split('&&')
        if len(query_list) > 1:
            for i in range(len(query_list)-1):
                yield query_list[i], query_list[i+1], 1

def postive_sampling(dt, version):
    select_sql = """
        select pathid, concat_ws('&&',collect_set(name)) as querys 
        from app.app_algo_graph_path
        where dt='{dt}' and version='{version}' and name!='' and name is not null
        group by pathid
    """.format(dt=dt, version=version)
    print(select_sql)
    #hiveCtx.sql(select_sql).registerTempTable("tmp_table")
    data = hiveCtx.sql(select_sql).rdd.cache()
    data.repartition(200).mapPartitions(lambda rows: main_func(rows)) \
        .toDF(["query_1","query_2","label"]).registerTempTable("tmp_table")

    insert_sql = """
        insert into table {table_name} partition(dt='{dt}', version='{version}')
        select * from tmp_table
        """.format(table_name=table_name, dt=dt, version=version)
    print(insert_sql)
    hiveCtx.sql(insert_sql)
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--date", help="work date", default="1")
    parser.add_argument("-v", "--version", help="version", default="1")

    args = parser.parse_args()
    print("%s parameters:%s" % (sys.argv[0], args))

    begin_time = time.time()
    print("%s begin at %s" % (sys.argv[0], str(datetime.datetime.now())))

    conf = SparkConf()
    sc = SparkContext(conf=conf, appName="sp_ind")
    sc.setLogLevel("WARN")
    hiveCtx = HiveContext(sc)
    hiveCtx.setConf('spark.shuffle.consolidateFiles', 'true')
    hiveCtx.setConf('spark.sql.shuffle.partitions', '1000')
    hiveCtx.sql('use app')
    dt='2020-05-25'
    version='query-similar-month' 
    # Create table.
    create_table()
    #negative_sampling(dt, version)
    postive_sampling(dt, version)

