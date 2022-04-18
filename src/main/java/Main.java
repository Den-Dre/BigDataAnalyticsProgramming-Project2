import java.io.IOException;

/**
 * The driver class for exercise1, to calculate the
 * distances of 2010_03.trips using Spark
 */
public class Main {
    public static void main(String[] args) throws IOException {
        // Using Spark implementation
        SparkDistances sd = new SparkDistances();
        sd.calculateDistances();

        // Using regular implementation
        // GPSUtil.calculateDistances();
    }
}
