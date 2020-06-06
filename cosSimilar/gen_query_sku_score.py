# -*-coding=utf-8
import sys

import datetime
from pyspark.sql.types import Row
from pyspark import SparkConf, SparkContext, HiveContext
import os

import parse_args
args = parse_args.parse_args()

to_table_name = 'app_algo_recallrelation_query_sku_score'
def create_table(hiveCtx):
    create_tbl = """
        CREATE EXTERNAL TABLE IF NOT EXISTS app.{table_name} (
            query string,
            sku string,
            sku_name string,
            query_sku_score float,
            query_cate array<string>,
            query_cate_weigth array<float>,
            sku_cate array<string>,
            sku_cate_weigth array<float>,
            sku_org_cate string,
            search_count int
            )
        PARTITIONED BY(dt string, dp string)
        STORED AS ORC
        LOCATION 'hdfs://ns1013/user/recsys/suggest/app.db/{table_name}'
        TBLPROPERTIES('orc.compress'='SNAPPY')
    """.format(table_name=to_table_name)
    hiveCtx.sql(create_tbl)

def get_score(q_c, sku_c, cid3_dict):
    return cid3_dict.get(q_c,{}).get(sku_c,0.0)
    
def main_func(rows, br_cid3_dict):
    cid3_dict = br_cid3_dict.value
    for row in rows:
        query, sku, sku_name, query_cate, query_cate_weigth, sku_cate, sku_cate_weigth, sku_org_cate, search_count= row
        score = 0.0
        if query_cate=='' or query_cate==None or sku_cate=='' or sku_cate==None:
            continue

        q_cid = query_cate
        q_cid_weigth = query_cate_weigth
        sku_cid = sku_cate
        sku_cid_weigth = sku_cate_weigth

        # normlize weigth
        q_sum_weigth = sum(q_cid_weigth)
        sku_sum_weigth = sum(sku_cid_weigth)
        # comput score
        for q_w, q_c in zip(q_cid_weigth, q_cid):
            for sku_w, sku_c in zip(sku_cid_weigth, sku_cid):
                c2c_score = get_score(q_c, sku_c, cid3_dict)
                if c2c_score==0.0:continue
                score+=(q_w/q_sum_weigth)*(sku_w/sku_sum_weigth)*c2c_score
        yield Row(query=query, sku=sku, sku_name=sku_name,
                  query_sku_score=score, query_cate=query_cate,
                  query_cate_weigth=query_cate_weigth,
                  sku_cate = sku_cate,
                  sku_cate_weigth=sku_cate_weigth,
                  sku_org_cate=sku_org_cate, search_count=search_count)

def getQuery(sc,hiveCtx):
    sql="""
        select query, sku, sku_name, 
        query_cate, 
        query_cate_weigth, sku_cate, sku_cate_weigth, sku_org_cate, search_count
        from app.app_algo_recallrelation_query_sku_cate where dt='{dt}'  and dp='{dp}'  
    """.format(dt=args.fdt, dp=args.fdp)
    print(sql)

    sql_score="""
        select cid3, score_top200
        from app.app_algo_recallrelation_cid_top200_score
        where dt='active'
    """
    print(sql_score)
    ###  cids score data ######
    cid3_data = hiveCtx.sql(sql_score)

    cid3_data_rdd = cid3_data.rdd.map(lambda x : (x[0],x[1]))
    cid3_collect_dict = cid3_data_rdd.collectAsMap()
    cid3_dict = {}

    for item in cid3_collect_dict:
        score_list = cid3_collect_dict[item].split(',')
        for score in score_list:
            c, s = score.split(':')
            cid3_dict.setdefault(item,{}).setdefault(c,float(s))


    br_cid3_dict = sc.broadcast(cid3_dict)
    data = hiveCtx.sql(sql).rdd
    data.repartition(1000).mapPartitions(lambda rows: main_func(rows, br_cid3_dict))\
        .toDF().registerTempTable("tmp_table")

    insert_sql = """
        insert overwrite table app.app_algo_recallrelation_query_sku_score partition(dt='{dt}', dp='{dp}')
        select  query, sku, sku_name, query_sku_score, query_cate, query_cate_weigth, sku_cate, sku_cate_weigth, sku_org_cate, search_count from tmp_table
        """.format(dt=args.tdt, dp=args.tdp)
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

    hiveCtx.sql("use app")
    create_table(hiveCtx)
    getQuery(sc,hiveCtx)

