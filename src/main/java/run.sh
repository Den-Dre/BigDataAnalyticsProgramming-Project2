#!/bin/bash
clear
rm -r ../../../output
# rm -r ./intermediaryOutput
javac -cp $(yarn classpath) TripReconstructor.java
jar cf TripReconstructor.jar *.class
hadoop jar TripReconstructor.jar TripReconstructor ../../../data/2010_03_mini.segments ../../../output/
