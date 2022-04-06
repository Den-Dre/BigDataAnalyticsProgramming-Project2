#!/bin/bash

SRC=/home/r0760777/bdap/BigDataAnalyticsProgramming-Project2/src/main/java
/usr/lib/jvm/java-8-openjdk-amd64/bin/javac -cp "$SRC:$(yarn classpath)" "$SRC"/TripReconstructor.java
/usr/lib/jvm/java-8-openjdk-amd64/bin/jar cf "$SRC"/TripReconstructor.jar "$SRC"/*.class
for nbMaps in $(seq 1 5); do
  for nbReduces in $(seq 1 5); do
    hdfs dfs -rm -r output
    echo "============================================"
    echo "Starting maps:${nbMaps} reduces:${nbReduces}"
    echo "============================================"
    #time hadoop jar -D mapreduce.job.maps="$nbMaps" -D mapreduce.job.reduces="$nbReduces" "$SRC"/TripReconstructor.jar "$SRC"/TripReconstructor /data/all.segments output > "maps:${nbMaps}reduces:${nbReduces}"
    time hadoop jar  "$SRC"/TripReconstructor.jar "$SRC"/TripReconstructor /data/all.segments output -D mapreduce.job.maps="$nbMaps" -D mapreduce.job.reduces="$nbReduces" > "maps:${nbMaps}reduces:${nbReduces}"
  done
done
