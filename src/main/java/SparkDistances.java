import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * A class to calculate the distances of .trip files
 * based on a Spark implementation
 */
public class SparkDistances {
    private final SparkSession spark;
    private final JavaSparkContext jsc;

    /**
     * Creates a new SparkDistances instance
     * with "local" as master, to execute
     * locally
     */
    SparkDistances() {
        this("local");
    }

    /**
     * Initialises a {@link SparkConf}, {@link JavaSparkContext}
     * and a {@link SparkSession} to be used to calculate
     * the distances using Spark.
     *
     * @param master: the master of this {@link SparkConf}
     */
    private SparkDistances(String master) {
        SparkConf conf = new SparkConf().setMaster(master).setAppName("TripDistances");
        jsc = new JavaSparkContext(conf);
        spark = SparkSession
                .builder()
                .appName("TripDistances")
                .config("spark.master", master)
                .getOrCreate();
    }

    /**
     * Calculate the distances of the trips in a .trips file
     * using a Spark transformation
     *
     * @throws IOException: If the .trips data can't be read in
     */
    protected void calculateDistances() throws IOException {
        JavaRDD<Row> data = readData(System.getProperty("user.dir") + "/2010_03.trips");
        long currentTime = System.currentTimeMillis();
        List<Double> distances = getDistances(data);
        writeResults(distances);
        System.out.println("Spark took: " + (System.currentTimeMillis() - currentTime));
        jsc.stop();
    }

    /**
     * Write the calculated distances to an output file
     *
     * @param distances: the calculated distances to write to a file
     * @throws IOException: when the file can't be opened
     *  or the distances can't be written to a file
     */
    private void writeResults(List<Double> distances) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/data/sparkDistances.csv"));
        for (double d: distances) {
            writer.write(d + "\n");
        }
        writer.flush();
        writer.close();
    }

    /**
     * Read the .trips file to a {@link JavaRDD<Row>} by parallelize.
     * @param path: the path to the data to be read in
     * @return rdd: a {@link JavaRDD<Row>} object
     */
    private JavaRDD<Row> readData(String path) {
        return jsc.parallelize(spark.read()
                .option("delimiter", " ")
                .option("header", "false")
                .csv(path).collectAsList());
    }

    /**
     * Calculate the distances of the data in a given {@link JavaRDD<Row>} using
     * a flat surface earth formula. The formula is applied to the data using
     * a Spark transformation.
     *
     * @param data: the data of which the distances are to be calculated
     * @return distances: a list containing the calculated distances
     */
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
