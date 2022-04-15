#!/bin/bash

CUR_DIR="$PWD"
SCRIPT_PATH="$(readlink -f "$0")"
SCRIPT_DIR="$(dirname "$SCRIPT_PATH")"
if [ ! "$CUR_DIR" = "$SCRIPT_DIR" ]; then
  echo "Script must be called from its directory: $SCRIPT_DIR"
  echo "Changing directories..."
fi
cd "$SCRIPT_DIR" || exit
/usr/lib/jvm/java-8-openjdk-amd64/bin/javac -cp ".:$(yarn classpath)" TripReconstructor.java
/usr/lib/jvm/java-8-openjdk-amd64/bin/jar cf TripReconstructor.jar ./*.class
hdfs dfs -rm -r output
OUT=../../../experiments
if [ ! -d "$OUT" ]; then
    echo "Directory $OUT doesn't exist"
    exit 1
fi
# hdfs dfs -put TripReconstructor.jar /user/r0760777/src
# hadoop --loglevel DEBUG jar TripReconstructor.jar TripReconstructor /data/all.segments output

# The call below is correct to provide number of reducers I think (it prints 12)
hadoop jar TripReconstructor.jar TripReconstructor -D mapreduce.job.reduces=8  /data/all.segments output
#{ time hadoop jar TripReconstructor.jar TripReconstructor -D mapreduce.job.reduces=15 -D dfs.blocksize=1073741824 -D mapreduce.input.fileinputformat.split.minsize=1073741824 /data/all.segments output ; } 2> optimalParameters:reduces:15splitSize:1073741824
#{ time hadoop jar TripReconstructor.jar TripReconstructor -D mapreduce.job.reduces=8 -D mapreduce.input.fileinputformat.split.minsize=1073741824 /data/all.segments output ; } 2> ./output-optimal-reduces:8splitSize:1073741824

