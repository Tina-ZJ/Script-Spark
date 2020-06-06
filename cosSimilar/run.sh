# -*- coding: utf8 -*-
#!/usr/bin/env bash


#step 1: get model.w and cid3_name from hadoop

hadoop fs -get hdfs://ns1013/user/recsys/suggest/qp_workspace/HAN_wegiht/model.w ./
hadoop fs -get hdfs://ns1013/user/recsys/suggest/qp_workspace/HAN_wegiht/cid3_name.txt ./


#step 2: compute cid3 score
python gen_cid_score.py model.w cid3.top200 200

if [ $? -ne 0 ]
then
    echo " cid3 score faild"
    exit -1
else
    echo " cid3 score success"
fi

#step 3: insert data to table
sh load.sh
 
if [ $? -ne 0 ]
then
    echo " insert cid score to table faild"
    exit -1
else
    echo " insert cid score to table success"
fi
