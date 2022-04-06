#!/bin/bash

# SRC=/home/r0760777/bdap/BigDataAnalyticsProgramming-Project2/src/main/java
SRC=.
OUT=../../../experiments
/usr/lib/jvm/java-8-openjdk-amd64/bin/javac -cp "$SRC:$(yarn classpath)" "$SRC"/TripReconstructor.java
/usr/lib/jvm/java-8-openjdk-amd64/bin/jar cf "$SRC"/TripReconstructor.jar "$SRC"/*.class
for nbMaps in $(seq 1 5); do
  for nbReduces in $(seq 9 2 17); do
    hdfs dfs -rm -r /user/r0760777/output
    echo "============================================"
    echo "Starting maps:${nbMaps} reduces:${nbReduces}"
    echo "============================================"
    { time hadoop jar "$SRC"/TripReconstructor.jar TripReconstructor /data/all.segments output -Dmapreduce.job.maps="$nbMaps" -Dmapreduce.job.reduces="$nbReduces" ; } 2> "$OUT"/"2-maps:${nbMaps}reduces:${nbReduces}"
    # { time wget https://www.google.com ; } 2> "$OUT"/"maps:${nbMaps}reduces:${nbReduces}"
  done
done
