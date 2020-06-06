# coding=utf-8
import sys

import datetime
from pyspark.sql.types import Row
from pyspark import SparkConf, SparkContext, HiveContext
import os

table_name="sz_algo_similar_query"
def create_table(hiveCtx):
    create_tbl = """
        CREATE EXTERNAL TABLE IF NOT EXISTS app.{table_name} (
            sku string,
            query string
            )
        PARTITIONED BY(dt string)
        STORED AS ORC
        LOCATION 'hdfs://ns1013/user/recsys/suggest/app.db/{table_name}'
        TBLPROPERTIES('orc.compress'='SNAPPY')
    """.format(table_name=table_name)
    hiveCtx.sql(create_tbl)

def main_func(rows):
    for row in rows:
        sku, querys = row
        query_list = querys.split('&&')
        if len(query_list)>2:
            for i in range(len(query_list)-2):
                yield sku, ' '.join(query_list[i:i+3])
        else:
            yield sku, ' '.join(query_list)
 
def getQuery(hiveCtx):
    sql="""
        select sku, concat_ws('&&', collect_set(key_word)) as key_word from 
        (
        select sku, key_word, pv, row_number() over(partition by sku order by pv desc) as rank
        from
        ( 
            select sku, key_word, count(1) as pv
            from app.app_sdl_yinliu_search_click_log where dt>'{dt}' and dim_type='app' and trim(key_word)!='' and key_word is not null group by sku, key_word
        )a
        )b
        where rank<=50 group by sku 
    """.format(dt=yest_str)
    print(sql)
    #hiveCtx.sql(sql).registerTempTable("tmp_table")
    data = hiveCtx.sql(sql).rdd.cache()
    data.repartition(200).mapPartitions(lambda rows: main_func(rows))\
        .toDF(["sku","query"]).registerTempTable("tmp_table")

    insert_sql = """
        insert overwrite table {table_name} partition(dt='{dt}')
        select  * from tmp_table
        """.format(table_name=table_name, dt=dt_str)
    print("insert_sql:\n" + insert_sql)
    hiveCtx.sql(insert_sql)



if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(conf=conf, appName="sp_ind")
    sc.setLogLevel("WARN")
    hiveCtx = HiveContext(sc)
    hiveCtx.setConf('spark.shuffle.consolidateFiles', 'true')
    hiveCtx.setConf('spark.shuffle.memoryFraction', '0.4')
    hiveCtx.setConf('spark.sql.shuffle.partitions', '1000')
    if len(sys.argv) == 1:
        dt = datetime.datetime.now() + datetime.timedelta(-1)
    else:
        dt = datetime.datetime.strptime(sys.argv[1], "%Y%m%d").date()

    dt_str = dt.strftime("%Y-%m-%d")
    yest_dt=dt + datetime.timedelta(-30)
    yest_str=yest_dt.strftime("%Y-%m-%d")

    hiveCtx.sql("use app")
    create_table(hiveCtx)
    getQuery(hiveCtx)

