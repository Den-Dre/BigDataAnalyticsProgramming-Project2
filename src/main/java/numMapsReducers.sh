#!/bin/bash

# SRC=/home/r0760777/bdap/BigDataAnalyticsProgramming-Project2/src/main/java
SRC=.
OUT=../../../experiments/effectNbMappers2
/usr/lib/jvm/java-8-openjdk-amd64/bin/javac -cp "$SRC:$(yarn classpath)" "$SRC"/TripReconstructor.java
/usr/lib/jvm/java-8-openjdk-amd64/bin/jar cf "$SRC"/TripReconstructor.jar "$SRC"/*.class
if [ ! -d "$OUT" ]; then
    echo "Directory $OUT doesn't exist"
    exit 1
fi
splitSize="$((1024*1024*1024))"  # 8 Mb
nbReduces=8
#for splitSize in 134217728 1073741824 8589934592; do # 8, 16, 32, 64, 128Mb
    #for nbReduces in $(seq 8 14); do
for i in $(seq 1 5); do
    hdfs dfs -rm -r /user/r0760777/output
    echo "============================================"
    echo "Starting splitSize:${splitSize} nbReduces:${nbReduces}"
    echo "============================================"
    mapred job -list
    echo "============================================"
    # { time hadoop jar "$SRC"/TripReconstructor.jar TripReconstructor -D mapreduce.job.maps="$nbMaps" -D mapreduce.job.reduces="$nbReduces" /data/all.segments output ; } 2> "$OUT"/"3-maps:${nbMaps}reduces:${nbReduces}"
    # -D mapreduce.input.fileinputformat.split.maxsize="$splitSize" -D dfs.blocksize="$splitSize" mapreduce.input.fileinputformat.split.minsize=$splitSize
    { time hadoop jar "$SRC"/TripReconstructor.jar TripReconstructor -D mapreduce.job.reduces=$nbReduces -D dfs.blocksize="$splitSize" /data/all.segments output ; } 2> "$OUT"/"reduces:${nbReduces}splitSize:${splitSize}"
    splitSize="$((2*splitSize))"
    echo "Done"
done
    #done
#done
