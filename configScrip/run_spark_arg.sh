#!/usr/bin/env bash
#set -e

workspace=`pwd`

$SPARK_HOME/bin/spark-submit \
--driver-memory 10G \
--executor-memory 20G \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=50 \
--conf spark.dynamicAllocation.maxExecutors=300 \
--executor-cores 8 \
--conf spark.yarn.executor.memoryOverhead=6G \
--conf spark.broadcast.compress=true \
--conf spark.rdd.compress=true \
--conf spark.network.timeout=1000s \
--conf spark.sql.hive.mergeFiles=true \
--conf spark.speculation=false \
--conf spark.driver.extraLibraryPath=/software/servers/hadoop-2.7.1/lib/native \
--conf spark.executor.extraLibraryPath=/software/servers/hadoop-2.7.1/lib/native:/software/servers/hadoop-2.7.1/share/hadoop/common/lib/hadoop-lzo-0.4.20.jar \
--conf spark.pyspark.python=python2.7 \
--files $HIVE_CONF_DIR/hive-site.xml \
--conf spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
--conf spark.executorEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
--conf spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_algorithm:latest \
--conf spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_algorithm:latest \
$@
