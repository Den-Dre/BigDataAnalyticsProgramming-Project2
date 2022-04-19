#!/bin/bash

echo "Removing possibly existing output directories..."
hdfs dfs -rm -r /user/r0760777/sparkDistances
echo "Running Exercise1..."
/cw/bdap/software/spark-3.2.1-bin-hadoop3.2/bin/spark-submit --driver-memory 1600m Exercise1.jar


