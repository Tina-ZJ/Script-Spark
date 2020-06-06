# -*-encoding: utf-8 -*-
#Kmeans.py

import sys
import math
import random
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import HiveContext

import parse_args
args = parse_args.parse_args()

DBPATH = 'hdfs://ns1013/user/recsys/suggest/app.db'
table_name = 'app_algo_kmeans_query'

def create_table():
    create_sql = """
    CREATE EXTERNAL TABLE IF NOT EXISTS {table_name}
    (
        category int                   comment  '聚类类别',
        query    string                comment  'query',
        feature  array<float>          comment  'query的各分位点特征',
        cate_feature  array<float>     comment  '每个聚类特征值'
    )
    PARTITIONED BY (
        dt string,
        dp string
    )
    STORED AS ORC
    LOCATION '{DBPATH}/{table_name}'
    TBLPROPERTIES  ("orc.compress"="SNAPPY")
    """.format(DBPATH=DBPATH, table_name=table_name)
    print(create_sql)
    hiveCtx.sql(create_sql)
    

def closestPoint(point, centerpoints):
    shortestDistanceIndex = 0
    currentIndex = 0
    shortestDistance = float("inf")

    for cp in centerpoints:
        distance = [math.pow((x-y),2) for x,y in zip(point,cp)]
        distance = math.sqrt(sum(distance))
        if (distance < shortestDistance):
            shortestDistance = distance
            shortestDistanceIndex = currentIndex
        currentIndex = currentIndex + 1

    return shortestDistanceIndex

def addPoints(p1, p2):
    newPoint = [x+y for x,y in zip(p1,p2)]

    return newPoint

def euclideanDistance(p1, p2):
    distance = [math.pow((x-y),2) for x,y in zip(p1,p2)]
    distance = math.sqrt(sum(distance))
    return distance


def funcAverage(point, totalSize):
    t = [x/totalSize for x in point[1] ]
    return t
 

if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(conf=conf, appName="sp_kmeans")
    sc.setLogLevel("WARN")
    hiveCtx = HiveContext(sc)
    hiveCtx.setConf('spark.shuffle.consolidateFiles', 'true')
    hiveCtx.setConf('spark.sql.shuffle.partitions', '1000')
    hiveCtx.sql('use app')
    k = args.kmean
    
    # create table
    create_table()
    
    # get data
    sql = """
        select query, query_feature_20
        from app.sz_algo_recallrelation_query_score_fractile
        where dt='{dt}' and dp='{dp}'
        """.format(dt=args.fdt, dp=args.fdp)
    print(sql)
    data = hiveCtx.sql(sql).rdd.map(lambda x: (x[0], x[1])) 
    data.persist() 


    # intialize convergeDist, and the centroids list

    convergeDist = 0.01
    centroids = []

    for i in range(k):
        feature = [random.uniform(0, 1) for _ in range(20)]
        #print(feature)
        centroids.append(feature)

    meanChange = 1

    # beginning of K-means algorithm
    while meanChange >= convergeDist:		
        oldCentroids = centroids[:]
        
        pointCentroidPair = data.map(lambda x: (closestPoint(x[1], centroids), x[1])).cache()

        # for each centroid:
        for i in range(k):
            # create an rdd of only points that belong to the kth cluster
            clusterPoints = pointCentroidPair.filter(lambda x: x[0] == i).cache()
            #clusterPoints.persist()
            totalPoints = float(clusterPoints.count())
            # sum all the points in the cluster
            clusterSum = clusterPoints.reduceByKey(lambda c1, c2: addPoints(c1, c2))

            if clusterSum.count() != 0:
            # create a new centroid by taking the average of all the points in the cluster
                newCentroid = clusterSum.map(lambda pointSum: (funcAverage(pointSum, totalPoints)))	
                # update the centroids array with the new centroid
                centroids[i] = newCentroid.take(1)[0]	

        # calculate the distance between all the old cluster centroids and new cluster centroids
        centerDistances = []
        for i in range(k):
            centerDistances.append(euclideanDistance(oldCentroids[i], centroids[i]))
        # calculate the average change in centroid
        cDlength = len(centerDistances)
        sumcDs = sum(centerDistances)
        meanChange = sumcDs / cDlength
    centroids = [(x,y) for x,y in zip([x for x in range(len(centroids))],centroids) ] 
    centroidsRDD = sc.parallelize(centroids) 
    centroidsRDD.toDF(["category","category_feature"]).registerTempTable("tmp_table")

    pointCentroidPair.toDF(["category","query_feature"]).registerTempTable("tmp_table2")

    insert_sql = """
        insert overwrite table {table_name} partition(dt='{tdt}', dp='{tdp}')
        select  c.category, d.query, c.query_feature, c.category_feature   
        from
        (
        select a.category as category, query_feature , category_feature
        from
        (
            select category, category_feature from tmp_table
        )a
        right join
        (
            select category, query_feature from tmp_table2
        )b
        on a.category=b.category
        )c
        right join
        (
            select query, query_feature_20 from app.sz_algo_recallrelation_query_score_fractile
            where dt='{fdt}' and dp='{fdp}'
        )d
        on c.query_feature=d.query_feature_20
        """.format(fdt=args.fdt, fdp=args.fdp, table_name=table_name,tdt=args.tdt, tdp=args.tdp)
    
    print(insert_sql)
    hiveCtx.sql(insert_sql) 

