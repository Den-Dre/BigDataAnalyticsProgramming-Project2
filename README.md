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
To execute, navigate to the root of extracted zip file (r0760777), noted as "r0760777 [project root]" above. Run 
`hdfs dfs -rm -r /user/r0760777/sparkDistances` to ensure that the output directory doesn't already exist.
Then, run: `/cw/bdap/software/spark-3.2.1-bin-hadoop3.2/bin/spark-submit --driver-memory 1600m Exercise1.jar` to run the code for exercise 1.

## Exercise 2
To execute, navigate to the root of extracted zip file (r0760777), noted as "r0760777 [project root]" above. run 
Then, run: `hdfs dfs -rm -r /user/r0760777/output` to ensure that the output directory doesn't already exist. Then, run
```bash
hadoop jar Exercise2.jar TripReconstructor -D mapreduce.job.reduces=8 -D mapreduce.input.fileinputformat.split.minsize=536870912 /data/all.segments output 
```
to run the code for exercise 2. The output will be stored on the hdfs in the directory /user/r0760777/output/ which has the following strucutre:
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
