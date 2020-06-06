#!/usr/bin/env bash


#echo dt:${dt}
$SPARK_HOME/bin/spark-submit \
--deploy-mode cluster \
--driver-memory 10G \
--executor-memory 20G \
--executor-cores 8 \
--conf spark.yarn.maxAppAttempts=2 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.yarn.executor.memoryOverhead=16G \
--conf spark.broadcast.compress=true \
--conf spark.rdd.compress=true \
--conf spark.sql.hive.mergeFiles=true \
--conf spark.speculation=false \
--conf spark.shuffle.file.buffer=128k \
--conf spark.reducer.maxSizeInFlight=96M \
--conf spark.shuffle.io.maxRetries=9 \
--conf spark.shuffle.io.retryWait=60s \
--conf spark.driver.extraLibraryPath=/software/servers/hadoop-2.7.1/lib/native \
--conf spark.executor.extraLibraryPath=/software/servers/hadoop-2.7.1/lib/native:/software/servers/hadoop-2.7.1/share/hadoop/common/lib/hadoop-lzo-0.4.20.jar \
--conf spark.pyspark.python=python2.7 \
--conf spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
--conf spark.executorEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
--conf spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_algorithm:latest \
--conf spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_algorithm:latest \
$@
