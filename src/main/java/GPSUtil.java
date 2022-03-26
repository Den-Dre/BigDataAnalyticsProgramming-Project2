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
        return sphericalEarthDistance(split[2], split[3], split[5], split[6]);
    }

    /**
     * Calculate the flat surface spherical distance in kilometres between point 1 and point 2
     *
     * Based on: https://en.wikipedia.org/wiki/Geographical_distance#Flat-surface_formulae
     *
     * @param lat1Str: The latitude of point1 in String representation
     * @param long1Str: the longitude of point1 in String representation
     * @param lat2Str: the latitude of point2 in String representation
     * @param long2Str: the longitude of point2 in String representation
     * @return the spherical distance in kilometers between point1 and point2
     */
    protected static double sphericalEarthDistance(String lat1Str, String long1Str, String lat2Str, String long2Str)
            throws NumberFormatException {
        double lat1 = Double.parseDouble(lat1Str);
        double long1 = Double.parseDouble(long1Str);
        double lat2 = Double.parseDouble(lat2Str);
        double long2 = Double.parseDouble(long2Str);

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
