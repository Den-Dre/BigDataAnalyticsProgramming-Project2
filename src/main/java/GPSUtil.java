import java.io.*;

import static java.lang.Math.*;

public class GPSUtil {
    public static void main(String[] args) throws IOException {
        calculateDistances();
    }

    public static void calculateDistances() throws IOException {
        String userDir = System.getProperty("user.dir");
        BufferedReader reader = new BufferedReader(new FileReader(userDir + "/data/2010_03.trips"));
        BufferedWriter writer = new BufferedWriter(new FileWriter(userDir + "/data/distances.csv", false));
        String currentLine;
        writer.write("TaxiID Distance\n");
        long currentTime = System.currentTimeMillis();
        long totalTime = 0;
        while ((currentLine = reader.readLine()) != null) {
            currentTime = System.currentTimeMillis();
            double dist  = getDistance(currentLine);
            totalTime += System.currentTimeMillis() - currentTime;
            writer.write(currentLine.split(" ")[0] + " " + dist + "\n");
        }
        System.out.println("Default implementation took: " + totalTime);
        reader.close();
        writer.close();
    }

    private static double getDistance(String line) {
        // The parameter line has the following format:
        // <taxi-id> <start date> <start pos (lat)> <start pos (long)> <end date> <end pos (lat)> <end pos (long)>
        String[] split = line.split(" ");
        double lat1,  long1, lat2, long2;
        lat1 = Double.parseDouble(split[2]);
        long1 = Double.parseDouble(split[3]);
        lat2 = Double.parseDouble(split[5]);
        long2 = Double.parseDouble(split[6]);

        return sphericalEarthDistance(lat1, long1, lat2, long2);
    }

    /**
     * Calculate the flat surface spherical distance (in Kilometres?) between point 1 and point 2
     * Based on: https://en.wikipedia.org/wiki/Geographical_distance#Flat-surface_formulae
     *
     * @param lat1: The latitude of point1
     * @param long1: the longitude of point1
     * @param lat2: the latitude of point2
     * @param long2: the longitude of point2
     * @return: the spherical distance between point1 and point2
     */
    protected static double sphericalEarthDistance(double lat1, double long1, double lat2, double long2) {
        // Radius of the earth in kilometres
        final double R = 6371.009;

        // Difference in latitude in radians
        double deltaLat = (lat2 - lat1) * PI / 180;
        // Difference in longitude in radians
        double deltaLong = (long2 - long1) * PI / 180;
        // Mean of the latitudes in radians
        double latM = ((lat1 + lat2) / 2) * PI / 180;

        return R * sqrt(pow(deltaLat, 2) + pow(cos(latM) * deltaLong, 2));
    }
}
