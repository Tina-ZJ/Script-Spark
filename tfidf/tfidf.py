# -*-coding=utf-8
import sys

import datetime
from pyspark.sql.types import Row
from pyspark import SparkConf, SparkContext, HiveContext
import os

table_name='sz_algo_cid3_word_tfidf'
def create_table(hiveCtx):
    create_tbl = """
        CREATE EXTERNAL TABLE IF NOT EXISTS app.{table_name} (
            cid3    string,
            cid3_name    string,
            term    string,
            tfidf   float,
            rank    int 
            )
        PARTITIONED BY(dt string)
        STORED AS ORC
        LOCATION 'hdfs://ns1013/user/recsys/suggest/app.db/{table_name}'
        TBLPROPERTIES('orc.compress'='SNAPPY')
    """.format(table_name=table_name)
    hiveCtx.sql(create_tbl)


def getQuery(hiveCtx):
    sql="""
        select cid3, cid3_name, term
        from app.app_sz_algo_cid3_sku_segs 
        LATERAL VIEW explode(split(sku_term, ',')) t as term
        where dt='active'
    """
    print(sql)
    hiveCtx.sql(sql).registerTempTable("cid_seg")

    sql="""
        select b.cid3 as cid3, cid3_name, b.term as term, b.nums/a.allnums as tf
        from
        (select cid3, count(1) as allnums
        from cid_seg
        group by cid3
        )a
        right join
        (select cid3, cid3_name, term, count(1) as nums
        from cid_seg
        group by cid3, cid3_name, term
        )b
        on a.cid3=b.cid3
        """
    print(sql)
    hiveCtx.sql(sql).registerTempTable("term_tf")
    
    sql="""
        select count(distinct cid3) as num
        from term_tf
        """
    print(sql)
    nums = hiveCtx.sql(sql).rdd.collect()

    sql="""
        select b.cid3 as cid3, cid3_name, b.term as term, b.tf * LOG({nums}/a.nums) as tfidf
        from
        (select term, size(collect_set(cid3))  as nums
        from cid_seg
        group by term
        )a
        right join
        (select cid3, cid3_name, term, tf
        from term_tf
        )b
        on a.term=b.term
        """.format(nums=nums[0][0])
    print(sql)
    hiveCtx.sql(sql).registerTempTable("term_tfidf")
         
    sql="""
        select cid3, cid3_name, term, tfidf, row_number() over(partition by cid3 order by tfidf desc) as rank
        from  term_tfidf
        """ 
    print(sql)
    hiveCtx.sql(sql).registerTempTable("temp_table")
     

    insert_sql = """
        insert overwrite table {table_name} partition(dt='{dt}')
        select  * from temp_table
        """.format(table_name=table_name, dt=dt_str)
    print("insert_sql:\n" + insert_sql)
    hiveCtx.sql(insert_sql)



if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(conf=conf, appName="sp-tfidf")
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
