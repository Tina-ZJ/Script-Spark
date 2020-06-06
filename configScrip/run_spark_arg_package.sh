#!/usr/bin/env bash
#set -e

workspace=`pwd`/

common_module_dir_name="common_module"
common_module_dir="${workspace}/../../${common_module_dir_name}/"
common_module_zip="${common_module_dir_name}.zip"
rm ${common_module_zip}
cd "${common_module_dir}/../"
zip -r "${workspace}/${common_module_zip}" ${common_module_dir_name}
cd ${workspace}

# sync latest searcher segment dicts.
searcher_segment_dict="dict.zip"
remote_ip="http://opfile.jd.local/"
remote_dir="opfile/word_table_data/word_table_idc/mainsearch_dict/${searcher_segment_dict}"
wget -r -npH -R.html ${remote_ip}/${remote_dir}
if [ $? -ne 0 ]; then
    echo "wget_dicts_from_op failed!"
    exit -1
fi
cp ${workspace}/${remote_dir} ${searcher_segment_dict}

searcher_segment_dir="${common_module_dir}/searcher_segment_tool/"
searcher_segment_lib="${searcher_segment_dir}/lib/libWordSeggerInterface.so"
searcher_segment_data="${searcher_segment_dict}"

# qp_normalize_tool
qp_normalize_dir="${common_module_dir}/qp_normalize_tool/"
qp_normalize_lib="${qp_normalize_dir}/lib/libnormalize_tool_interface.so,${qp_normalize_dir}/normalize.ini"
qp_normalize_data="${qp_normalize_dir}/data/rmap,${qp_normalize_dir}/data/special_prefix,${qp_normalize_dir}/data/tsmap"

# qp_segment_tool
qp_segment_dir="${common_module_dir}/sz_segment_tool/"
qp_segment_lib="${qp_segment_dir}/lib/libsz_segment_tool_interface.so,${qp_segment_dir}/sz_segment.ini"
qp_segment_data="hdfs://ns1013/user/recsys/suggest/qp_workspace/segment/prob_model/latest/mashup.dat"

# crf_tag_tool
crf_model_dir="${common_module_dir}/crf_model_tool/"
crf_model_lib="${crf_model_dir}/_CRFPP.so,${crf_model_dir}/libcrfpp.so.0"
crf_model_data="hdfs://ns1013/user/recsys/suggest/qp_workspace/base_tag/model/latest/crf_binary.model.v9"

files_submit_with_spark="${searcher_segment_lib},${searcher_segment_data},${qp_normalize_lib},${qp_normalize_data},${qp_segment_lib},${qp_segment_data},${crf_model_lib},${crf_model_data}"

export PYTHONPATH=${PYTHONPATH}:${crf_model_dir}:${workspace}
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${crf_model_dir}

$SPARK_HOME/bin/spark-submit \
--driver-memory 10G \
--executor-memory 20G \
--executor-cores 10 \
--num-executors 50 \
--conf spark.network.timeout=1000s \
--conf spark.yarn.executor.memoryOverhead=16G \
--conf spark.rdd.compress=true \
--conf spark.speculation=false \
--conf spark.sql.hive.mergeFiles=true \
--conf spark.shuffle.file.buffer=128k \
--conf spark.reducer.maxSizeInFlight=96M \
--conf spark.shuffle.io.maxRetries=9 \
--conf spark.shuffle.io.retryWait=60s \
--conf spark.driver.extraLibraryPath=/software/servers/hadoop-2.7.1/lib/native \
--conf spark.executor.extraLibraryPath=/software/servers/hadoop-2.7.1/lib/native:/software/servers/hadoop-2.7.1/share/hadoop/common/lib/hadoop-lzo-0.4.20.jar \
--conf spark.pyspark.python=python2.7 \
--files $HIVE_CONF_DIR/hive-site.xml,${files_submit_with_spark} \
--conf spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
--conf spark.executorEnv.yarn.nodemanager.container-executor.class=DockerLinuxContainer \
--conf spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_algorithm:latest \
--conf spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name=bdp-docker.jd.com:5000/wise_algorithm:latest \
--py-files ${common_module_zip} \
$@
