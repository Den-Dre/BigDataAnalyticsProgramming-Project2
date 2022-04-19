# BigDataAnalyticsProgramming-Project2
## File structure
```
r0760777 [project root]
│   README.md
│   report.pdf
│   Exercise1.jar
│   Exercise2.jar
└───src
    |   2010_03.trips
    └───main
        └───GPSUtil.java
            Main.java
            SparkDistances.java
            TripReconstructor.java
```

## Exercise 1
To execute, navigate to the root of extracted zip file (r0760777), noted as "r0760777 [project root]" above.
Then, execute `./runExercise1.sh` to run `Exercise1.jar`. This shell file does the following: 
- First, it runs `hdfs dfs -rm -r /user/r0760777/sparkDistances` to ensure that the output directory doesn't already exist.
- Then, it runs: `/cw/bdap/software/spark-3.2.1-bin-hadoop3.2/bin/spark-submit --driver-memory 1600m Exercise1.jar` to run the code for exercise 1.
  The output will be stored on the hdfs in `/user/r0760777/sparkDistances/part-00000`

To compile the code for `Exercise1` yourself, one can navigate to the root of the extracted 
zip file (noted as "r0760777 [project root]" above) and run `mvn clean compile assembly:single`.

## Exercise 2
To execute, navigate to the root of extracted zip file (r0760777), noted as "r0760777 [project root]" above. 
Run `runExercise2.sh` to run `Exercise2.jar`. This shell script does the following:
- First it runs: `hdfs dfs -rm -r /user/r0760777/output` to ensure that the output directory doesn't already exist. 
- Then it runs:
```bash
hadoop jar Exercise2.jar TripReconstructor -D mapreduce.job.reduces=8 -D mapreduce.input.fileinputformat.split.minsize=536870912 /data/all.segments output 
```
to run the code for exercise 2. The output will be stored on the hdfs in the directory `/user/r0760777/output/` which has the following strucutre:
```
output
│   
└───revenuePerMonth
│   └───_SUCCESS
│	part-r-00000
│   
└─── revenuePerTrip
     └───_SUCCESS
	  part-r-00000
	  part-r-00001
	  part-r-00002
	  part-r-00003
	  part-r-00004
	  part-r-00005
	  ...
	  part-r-n
```
where `n` is the number of nodes in the cluster the program is ran on.
Note: this will run the program with the chosen number of reducers and mappers, as discussed in report/report.pdf. 

To compile the code for `Exercise 2` yourself, one can navigate to `r0760777/src/main/java` and 
run the following commands:
- `/usr/lib/jvm/java-8-openjdk-amd64/bin/javac -cp ".:$(yarn classpath)" TripReconstructor.java`
- `/usr/lib/jvm/java-8-openjdk-amd64/bin/jar cf TripReconstructor.jar ./*.class`
