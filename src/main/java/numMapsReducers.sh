#!/bin/bash

# SRC=/home/r0760777/bdap/BigDataAnalyticsProgramming-Project2/src/main/java
SRC=.
OUT=../../../experiments/effectNbReducers
/usr/lib/jvm/java-8-openjdk-amd64/bin/javac -cp "$SRC:$(yarn classpath)" "$SRC"/TripReconstructor.java
/usr/lib/jvm/java-8-openjdk-amd64/bin/jar cf "$SRC"/TripReconstructor.jar "$SRC"/*.class
# for splitSize in 2147483648 4294967296 8589934592 17179869184; do # 8, 16, 32, 64, 128Mb
for nbReduces in $(seq 28 32); do
    hdfs dfs -rm -r /user/r0760777/output
    if [ ! -d "$OUT" ]; then
	echo "Directory $OUT doesn't exist"
	exit 1
    fi
    echo "============================================"
    echo "Starting nbReduces: ${nbReduces}"
    echo "============================================"
    # { time hadoop jar "$SRC"/TripReconstructor.jar TripReconstructor -D mapreduce.job.maps="$nbMaps" -D mapreduce.job.reduces="$nbReduces" /data/all.segments output ; } 2> "$OUT"/"3-maps:${nbMaps}reduces:${nbReduces}"
    # -D mapreduce.input.fileinputformat.split.maxsize="$splitSize" -D dfs.blocksize="$splitSize" mapreduce.input.fileinputformat.split.minsize=$splitSize
    { time hadoop jar "$SRC"/TripReconstructor.jar TripReconstructor -D mapreduce.job.reduces=$nbReduces /data/all.segments output ; } 2> "$OUT"/"3-maps:1reduces:$nbReduces"
  #done
done
