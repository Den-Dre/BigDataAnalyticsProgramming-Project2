import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        // Using Spark implementation
        SparkDistances sd = new SparkDistances();
        sd.calculateDistances();

        // Using regular implementation
        // GPSUtil.calculateDistances();
    }
}
