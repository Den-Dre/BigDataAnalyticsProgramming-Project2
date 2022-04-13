import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SparkDistances {
    private final SparkSession spark;
    private SparkConf conf;
    private final JavaSparkContext jsc;
    private JavaRDD<Row> data;

    SparkDistances() {
        this("local");
    }

    private SparkDistances(String master) {
        SparkConf conf = new SparkConf().setMaster(master).setAppName("TripDistances");
        jsc = new JavaSparkContext(conf);
        spark = SparkSession
                .builder()
                .appName("TripDistances")
                .config("spark.master", master)
                .getOrCreate();
    }

    protected void calculateDistances() throws IOException {
        JavaRDD<Row> data = readData(System.getProperty("user.dir") + "/data/2010_03.trips");
        long currentTime = System.currentTimeMillis();
        List<Double> distances = getDistances(data);
        writeResults(distances);
        System.out.println("Spark took: " + (System.currentTimeMillis() - currentTime));
        jsc.stop();
    }

    private void writeResults(List<Double> distances) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/data/sparkDistances.csv"));
        for (double d: distances) {
            writer.write(d + "\n");
        }
        writer.flush();
        writer.close();
    }

    private JavaRDD<Row> readData(String path) {
        return jsc.parallelize(spark.read()
                .option("delimiter", " ")
                .option("header", "false")
                .csv(path).collectAsList());
    }

    private static List<Double> getDistances(JavaRDD<Row> data) {
        JavaRDD<Double> mapping = data.map((Function<Row, Double>) row -> {
                    try {
                        return GPSUtil.sphericalEarthDistance(
                                row.getString(2),
                                row.getString(3),
                                row.getString(5),
                                row.getString(6)
                        );
                    } catch (NumberFormatException e) {
                        return -1.0;
                    }
                }
        );

        return mapping.collect();
    }
}
