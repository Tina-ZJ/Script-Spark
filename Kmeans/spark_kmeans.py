from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import HiveContext

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler 



#to_table_name = 'app_algo_cid3_similar'
to_table_name = 'app_algo_word_similar'

def create_table_1(hiveCtx):
    create_tbl = """
        CREATE EXTERNAL TABLE IF NOT EXISTS app.{table_name} (
            word string,
            cluster_id int,
            emb array<float>
            )
        PARTITIONED BY(dt string, dp string)
        STORED AS ORC
        LOCATION 'hdfs://ns1013/user/recsys/suggest/app.db/{table_name}'
        TBLPROPERTIES('orc.compress'='SNAPPY')
    """.format(table_name=to_table_name)
    hiveCtx.sql(create_tbl)

#center_table_name = 'app_algo_kmean_cid3_cluster'
center_table_name = 'app_algo_kmean_word_cluster'
def create_table_2(hiveCtx):
    create_tbl = """
        CREATE EXTERNAL TABLE IF NOT EXISTS app.{table_name} (
            cluster_id int,
            axis array<float>
            )
        PARTITIONED BY(dt string, dp string)
        STORED AS ORC
        LOCATION 'hdfs://ns1013/user/recsys/suggest/app.db/{table_name}'
        TBLPROPERTIES('orc.compress'='SNAPPY')
    """.format(table_name=center_table_name)
    hiveCtx.sql(create_tbl)


def create_table(hiveCtx):
    create_table_1(hiveCtx)
    create_table_2(hiveCtx)


def split_feature(q, ss, feature_count):
    l = list()
    feature = [float(x) for x in ss.split()] 
    l.append(q)
    l.append(feature)
    l.extend(feature)
    return l

def main():
    conf = SparkConf()
    sc = SparkContext(conf=conf, appName="sp_kmeans_cid3")
    sc.setLogLevel("WARN")
    hiveCtx = HiveContext(sc)
    hiveCtx.setConf('spark.shuffle.consolidateFiles', 'true')
    hiveCtx.setConf('spark.sql.shuffle.partitions', '1000')
    hiveCtx.sql('use app')

    create_table(hiveCtx)
    feature_count = 100
    k = 3000

    sql = """
        select word, emb
        from app.app_algo_recallrelation_word_emb
        where dt='{dt}'
        """.format(dt='sku_product')
    print(sql)

    all_name = ['word', 'emb']
    name_feature = [ 'f_' + str(i) for i in range(0, feature_count)]
    all_name.extend(name_feature)
    #.filter(lambda x: len(x[1])>0)
    df = hiveCtx.sql(sql).rdd.map(lambda x: split_feature(x[0],x[1], feature_count)).toDF(all_name) 
    #df= hiveCtx.sql(sql).rdd.map(lambda x: (x[1])).toDF(['features']) 
    df.persist() 

   # Loads data.
    #dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")

    #input_cols=['_1', '_2', '_3']
    vec_assembler = VectorAssembler(inputCols = name_feature, outputCol='features')
    dataset = vec_assembler.transform(df).cache()
    print('dataset count %d'%(dataset.count()))
    #dataset.show(100, False)
    
    # Trains a k-means model.
    kmeans = KMeans(featuresCol='features').setK(k).setSeed(1)
    model = kmeans.fit(dataset)
    
    # Make predictions
    predictions = model.transform(dataset)
    
    # Evaluate clustering by computing Silhouette score
    evaluator = ClusteringEvaluator()
    
    #silhouette = evaluator.evaluate(predictions)
    #print("Silhouette with squared euclidean distance = " + str(silhouette))
    
    # Shows the result.
    centers = model.clusterCenters()
    print("Cluster Centers %s: "%(type(centers)))
    #for center in centers:
    #    print(center)

    tmp_list = [[i,[float(f) for f in v]] for i,v in enumerate(centers)]
    sc.parallelize(tmp_list).toDF(['cluster_id', 'axis']).registerTempTable('tmp_center_table')
    insert_sql = """
            insert overwrite table app.{table_name} partition(dt='{dt}', dp='{dp}')
            select * from tmp_center_table 
            """.format(table_name=center_table_name,dt=dt, dp=dp)
    print('start insert ' + insert_sql)
    hiveCtx.sql(insert_sql)

    #print('predictions %s'% str(type(predictions)))
    #for p in predictions:
    #    print('predictions %s'% str(p))

    #predictions.show(10, False)
    predictions.registerTempTable('tmp_table')
    insert_sql = """
            insert overwrite table app.{table_name} partition(dt='{dt}', dp='{dp}')
            select word, prediction as cluster_id, emb
            from tmp_table 
            """.format(table_name=to_table_name,dt=dt, dp=dp)
    print('start insert ' + insert_sql)
    hiveCtx.sql(insert_sql)


if __name__ ==  '__main__':
    dt = '2020-06-19'
    dp = 'product_emb'
    main()
