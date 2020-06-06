# -*-coding=utf-8
import sys

import datetime
from pyspark.sql.types import Row
from pyspark import SparkConf, SparkContext, HiveContext
import os
import numpy as np

def create_table(hiveCtx):
    create_tbl = """
        CREATE EXTERNAL TABLE IF NOT EXISTS app.sz_algo_query_feature (
            query string,
            num   int,
            sku_score_normal map<string,float>,
            quarter_n float,
            quarter float,
            half_n float,
            half float,
            three_quarters_n float,
            three_quarters float
            )
        PARTITIONED BY(dt string, dp string)
        STORED AS ORC
        LOCATION 'hdfs://ns1013/user/recsys/suggest/app.db/sz_algo_query_feature'
        TBLPROPERTIES('orc.compress'='SNAPPY')
    """
    hiveCtx.sql(create_tbl)

def main_func(rows):
    for row in rows:
        query, sku, score, num = row
        if num<20 or num>12000:
            continue
        score_list = [float(x) for x in score.split(',')]
        sku_list = sku.split(',')
        #均值
        mean = np.mean(score_list)
        #方差
        #var = np.var(score_list)
        #标准差
        std = np.std(score_list)
        #归一化，减均值除以方差
        score_list_new = (score_list - mean) / std
        # sort
        #score_sorted_new= sorted(score_list_new, reverse=True)
        index_sorted = np.argsort(-score_list_new)
        sku_score = [(sku_list[i],score_list_new[i]) for i in index_sorted]
        # get fractile
        quarter = index_sorted[int(len(score_list_new)/4)]
        half = index_sorted[int(len(score_list_new)/2)] 
        three_quarters = index_sorted[int(len(score_list_new)*0.75)] 
        yield query, num, dict(sku_score), float(score_list_new[quarter]), float(score_list[quarter]), float(score_list_new[half]), float(score_list[half]), float(score_list_new[three_quarters]), float(score_list[three_quarters])   
 
def getQuery(hiveCtx):
    sql="""
        select query, concat_ws(',', collect_list(query_cate_weight)) as sku, concat_ws(',',collect_list(query_sku_score)) as score, count(1) as num
        from app.app_algo_recallrelation_query_sku_cate_score
        where dt='2020-05-14_v2'
        group by query
    """
    print(sql)
    #hiveCtx.sql(sql).registerTempTable("tmp_table")
    data = hiveCtx.sql(sql).rdd
    data.repartition(500).mapPartitions(lambda rows: main_func(rows))\
        .toDF(["query","num","sku_score_normal","quarter_n","quarter","half_n","half","three_quarters_n","three_quarters"]).registerTempTable("tmp_table")

    insert_sql = """
        insert overwrite table app.sz_algo_query_feature partition(dt='{dt}', dp='{dp}')
        select  * from tmp_table
        """.format(dt=dt_str, dp=dp)
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
    
    dp='v1'

    dt_str = dt.strftime("%Y-%m-%d")
    yest_dt=dt + datetime.timedelta(-30)
    yest_str=yest_dt.strftime("%Y-%m-%d")

    hiveCtx.sql("use app")
    #create_table(hiveCtx)
    getQuery(hiveCtx)

