#!/bin/bash

SRC=../src/main/java
/usr/lib/jvm/java-8-openjdk-amd64/bin/javac -cp "$SRC:$(yarn classpath)" "$SRC"/TripReconstructor.java
/usr/lib/jvm/java-8-openjdk-amd64/bin/jar cf "$SRC"/TripReconstructor.jar "$SRC"/*.class
for nbMaps in $(seq 1 5); do
  for nbReduces in $(seq 1 5); do
    hdfs dfs -rm -r output
    echo "============================================"
    echo "Starting maps:${nbMaps} reduces:${nbReduces}:"
    echo "============================================"
    time hadoop jar -D mapred.map.tasks="$nbMaps" -D mapred.reduce.tasks="$nbReduces" "$SRC"/TripReconstructor.jar "$SRC"/TripReconstructor /data/all.segments output > "maps:${nbMaps}reduces:${nbReduces}"
  done
done