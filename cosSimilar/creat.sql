use app;
set mapred.job.priority=VERY_HIGH;
set mapreduce.job.queuename=bdp_jmart_recsys.recsys_suggest;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=True;
CREATE EXTERNAL TABLE IF NOT EXISTS `app.app_algo_recallrelation_cid_top200_score`(
    `cid3` string COMMENT 'cid3',
    `score_top200` string COMMENT 'score_top200')
PARTITIONED BY (
    `dt` string)
row format delimited fields terminated by '\t'
lines terminated by '\n'
STORED AS textfile
LOCATION 'hdfs://ns1013/user/recsys/suggest/app.db/app_algo_recallrelation_cid_top200_score'
TBLPROPERTIES (
    'orc.compress'='SNAPPY',
    'SENSITIVE_TABLE'='FALSE');
