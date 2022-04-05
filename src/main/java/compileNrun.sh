#!/bin/bash

/usr/lib/jvm/java-8-openjdk-amd64/bin/javac -cp ".:$(yarn classpath)" TripReconstructor.java 
/usr/lib/jvm/java-8-openjdk-amd64/bin/jar cf TripReconstructor.jar *.class
hdfs dfs -rm -r output
# hdfs dfs -put TripReconstructor.jar /user/r0760777/src
# hadoop --loglevel DEBUG jar TripReconstructor.jar TripReconstructor /data/all.segments output
hadoop jar TripReconstructor.jar TripReconstructor /data/all.segments output
