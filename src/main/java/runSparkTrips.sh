/usr/lib/jvm/java-8-openjdk-amd64/bin/javac -cp ".:$(yarn classpath):/cw/bdap/software/spark-3.2.1-bin-hadoop3.2/jars/*" Main.java
#/usr/lib/jvm/java-8-openjdk-amd64/bin/jar cmvf META-INF/MANIFEST.MF Main.jar GPSUtil.class Main.class SparkDistances.class 
java -cp "/cw/bdap/software/spark-3.2.1-bin-hadoop3.2/jars/*:." Main
