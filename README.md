# BigDataAnalyticsProgramming-Project2
## File structure
```
r0760777 [project root]
│   README.md
│   report.pdf
│   Exercise1.jar
│   Exercise2.jar
└───src
│   |   2010_03.trips
|   └───main
|   |   └───GPSUtil.java
|   |   |   Main.java
|   |   |   SparkDistances.java
|   |   |   TripReconstructor.java
```

## Exercise 1
To execute, navigate to the root of extracted zip file (r0760777), noted as "r0760777 [project root]" above.
Then, run: `java -jar Exercise1.jar` to run the code for exercise 1.

## Exercise 2
To execute, navigate to the root of extracted zip file (r0760777), noted as "r0760777 [project root]" above.
Then, run: 
```bash
hadoop jar Exercise2.jar TripReconstructor -D mapreduce.job.reduces=8 -D mapreduce.input.fileinputformat.split.minsize=1073741824 /data/all.segments output 
```
to run the code for exercise 2.
Note: this will run the program with the chosen number of reducers and mappers 
