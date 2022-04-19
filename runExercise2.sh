#!/bin/bash

echo "Removing output dirctories that possibly already exist..."
hdfs dfs -rm -r /user/r0760777/output
echo "Running Exercise 2"
hadoop jar Exercise2.jar TripReconstructor -D mapreduce.job.reduces=8 -D mapreduce.input.fileinputformat.split.minsize=536870912 /data/all.segments output
