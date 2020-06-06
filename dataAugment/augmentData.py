#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import datetime
import os
import random
import sys
import time
import re
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SparkFiles
from pyspark.sql import HiveContext
import numpy as np

process_result_table_name = "app_sz_algo_multi_task_han_augment2"

def load():
    f = open('./augment.cid3')
    cid3 = list()
    for line in f:
        terms = line.strip().split('\t')
        if terms[0] not in cid3:
            cid3.append("'"+str(terms[0])+"'")
    return cid3


def create_table(hive_context):
    create_table_sql = """
        CREATE EXTERNAL TABLE IF NOT EXISTS app.{table_name} (
            keyword        string, 
            cid3           string, 
            cid3_name      string)
        PARTITIONED BY (dt string)
        STORED AS ORC
        LOCATION '/user/recsys/suggest/app.db/{table_name}'
        TBLPROPERTIES('orc.compress'='SNAPPY')
    """.format(table_name=process_result_table_name)
    print("create_table_sql:%s" % create_table_sql)
    hive_context.sql(create_table_sql)



def main_func(rows):
    def cut(begin,end,term, tfidf):
        maxs = len(term)
        idx = min(begin, maxs)
        return term[idx:end], tfidf[idx:end]

    def prob(tfidf):
        sums = sum(tfidf)
        tfidfs_p = [x/sums for x in tfidf]
        return tfidfs_p
       
    def choice(terms,probs):
        p = np.array(probs)
        term = np.random.choice(terms, p=p.ravel())
        return term 
    def choice_simple(terms):
        return random.choice(terms)
    def combine(terms):
        mid = int(len(terms)/2)
        return ''.join(terms), terms[0]+''.join(terms[mid:-1]), ''.join(terms[:mid+1]) 
    for row in rows:
        cid3, cid3_name, terms, ranks, tfidfs = row
        #cut
        result = []
        for i in range(500):
            result = []
            for begin,end in zip([0,5,20,50,100],[5,20,50,100,200]):
                top_term, top_tfidf = cut(begin,end, terms, tfidfs)
                # normalize
                tfidf_p = prob(top_tfidf) 
                #choice term according to prob
                if len(top_term)==0:
                    continue 
                term = choice_simple(top_term)
                #term = choice(top_term, tfidf_p) 
                result.append(term)
            for query in combine(result):
                yield query, cid3, cid3_name
 
def do_keyword_text_process(hive_context, keyword_source_sql):
    data = hive_context.sql(keyword_source_sql).rdd.cache()
    data.repartition(200).mapPartitions(lambda rows: main_func(rows))\
        .toDF(['keyword','cid3', 'cid3_name']).registerTempTable("tmp_table")
    #hive_context.sql(keyword_source_sql).registerTempTable("tmp_table")
    insert_result_into_table_sql = """
        insert overwrite table {table_name} partition(dt='{partition_dt}')
        select * 
        from tmp_table
    """.format(table_name=process_result_table_name, partition_dt='active')
    print("insert_result_into_table_sql:%s" % insert_result_into_table_sql)
    hive_context.sql(insert_result_into_table_sql)
    return

if __name__ == '__main__':
    begin_time = time.time()
    dt = datetime.datetime.now() + datetime.timedelta(-3)
    dt = dt.strftime("%Y-%m-%d")
    name = load()
    name = '('+','.join(name)+')' 
    print("%s begin at %s" % (sys.argv[0], str(datetime.datetime.now())))
    # order by length(sku_name) has no data richness
    input_keyword_source="""
                        select cid3, cid3_name, collect_list(term) as terms, collect_list(rank) as ranks, collect_list(tfidf) as tfidfs from app.sz_algo_cid3_word_tfidf
                        where rank<=200 and dt='{dt}' and cid3 in {name}
                        group by cid3, cid3_name 
                """.format(dt='active',name=name)
    conf = SparkConf()
    spark_sc = SparkContext(conf=conf, appName=(os.path.basename(sys.argv[0])))
    hive_hc = HiveContext(spark_sc)
    hive_hc.setConf('spark.shuffle.consolidateFiles', 'true')
    hive_hc.setConf('spark.storage.memoryFraction', '0.8')  # default 0.6
    hive_hc.setConf('spark.shuffle.memoryFraction', '0.4')  # default 0.2

    hive_hc.sql('use app')
    create_table(hive_hc)
    do_keyword_text_process(hive_hc, input_keyword_source)

    end_time = time.time()
    print("%s end at %s" % (sys.argv[0], str(datetime.datetime.now())))
    print("%s total cost time:%s" % (sys.argv[0], str(end_time - begin_time)))

