#!/bin/bash

# SRC=/home/r0760777/bdap/BigDataAnalyticsProgramming-Project2/src/main/java
SRC=.
OUT=../../../experiments
/usr/lib/jvm/java-8-openjdk-amd64/bin/javac -cp "$SRC:$(yarn classpath)" "$SRC"/TripReconstructor.java
/usr/lib/jvm/java-8-openjdk-amd64/bin/jar cf "$SRC"/TripReconstructor.jar "$SRC"/*.class
# for splitSize in 8388608 16777216 33554432 67108864 134217728; do # 8, 16, 32, 64, 128Mb
for splitSize in 268435456 536870912 1073741824 2147483648; do # 256, 512, 1024 Mb
  #for nbReduces in $(seq 9 2 17); do
    hdfs dfs -rm -r /user/r0760777/output
    echo "============================================"
    echo "Starting max split size: ${splitSize}, blocksize: ${splitSize}, reduces:10"
    echo "============================================"
    # { time hadoop jar "$SRC"/TripReconstructor.jar TripReconstructor -D mapreduce.job.maps="$nbMaps" -D mapreduce.job.reduces="$nbReduces" /data/all.segments output ; } 2> "$OUT"/"3-maps:${nbMaps}reduces:${nbReduces}"
    { time hadoop jar "$SRC"/TripReconstructor.jar TripReconstructor -D mapreduce.job.reduces=10 -D mapreduce.input.fileinputformat.split.maxsize="$splitSize" -D dfs.blocksize="$splitSize" /data/all.segments output ; } 2> "$OUT"/"splitSize:${splitSize}-blockSize:${splitSize}-reduces:10"
  #done
done
