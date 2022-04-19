SRC="./src/main/java"
CLASS_PATH="$SRC/*:/cw/bdap/software/spark-3.2.1-bin-hadoop3.2/jars/*"

if [ -d "$SRC/sparkDistances" ]; then
    rm -r $SRC/sparkDistances
fi
/usr/lib/jvm/java-8-openjdk-amd64/bin/javac -cp $CLASS_PATH $SRC/Main.java
java -cp $CLASS_PATH -Xmx1800m Main
