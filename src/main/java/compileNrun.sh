#!/bin/bash

/usr/lib/jvm/java-8-openjdk-amd64/bin/javac -cp ".:$(yarn classpath)" TripReconstructor.java 
/usr/lib/jvm/java-8-openjdk-amd64/bin/jar cf TripReconstructor.jar *.class
hdfs dfs -rm -r output
# hdfs dfs -put TripReconstructor.jar /user/r0760777/src
# hadoop --loglevel DEBUG jar TripReconstructor.jar TripReconstructor /data/all.segments output

# The call below is correct to provide number of reducers I think (it prints 12)
hadoop jar TripReconstructor.jar TripReconstructor -D mapreduce.job.reduces=10 /data/all.segments output 

