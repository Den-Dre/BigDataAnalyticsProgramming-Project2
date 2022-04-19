CLASS_PATH=".:/cw/bdap/software/spark-3.2.1-bin-hadoop3.2/jars/*"

if [ -d "./src/main/java/sparkDistances" ]; then
    rm -r ./src/main/java/sparkDistances
fi
/usr/lib/jvm/java-8-openjdk-amd64/bin/javac -cp $CLASS_PATH Main.java
/usr/lib/jvm/java-8-openjdk-amd64/bin/jar cmf META-INF/MANIFEST.MF Main.jar ./*.class
# java -cp $CLASS_PATH Main
