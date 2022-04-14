import java.io.*;

import static java.lang.Math.*;

/**
 * A class containing utility methods w.r.t.
 * calculating coordinate related stuff
 */
public class GPSUtil {
    /**
     * Calculate the distances between the endpoints of trips, defined in
     * [project-root]/data/2010_03.trips
     * The output is written to [project-root]/data/distances.csv
     *
     * @throws IOException: when the data can't be read, or outputs can't be written to disk
     */
    public static void calculateDistances() throws IOException {
        String userDir = System.getProperty("user.dir");
        BufferedReader reader = new BufferedReader(new FileReader(userDir + "/data/2010_03.trips"));
        BufferedWriter writer = new BufferedWriter(new FileWriter(userDir + "/data/distances.csv", false));
        String currentLine;
        writer.write("TaxiID Distance\n");
        long startTime = System.currentTimeMillis();
        double dist;
        while ((currentLine = reader.readLine()) != null) {
            dist  = getDistance(currentLine);
            // Writing each line to the output file separately,
            // results in the same performance as storing the results in a
            // StringBuilder and writing them once to the output file at the end.
            // Thus, we opt for the first option, as this limits memory usage.
            writer.write(currentLine.split(" ")[0] + " " + dist + "\n");
        }
        reader.close();
        writer.flush();
        writer.close();
        System.out.println("Default implementation took: " + (System.currentTimeMillis() - startTime));
    }

    /**
     * Calculates the distance between the two pairs of (latitude, longitude)
     * coordinates given in a trip segment of the form:
     * <br>[taxi-id] [start date] [start pos (lat)] [start pos (long)] [end date] [end pos (lat)] [end pos (long)]</br>
     *
     * @param line: the trip segment of which the distance is calculated
     * @return dist: the distance between the two pairs of coordinates of this trip
     */
    private static double getDistance(String line) {
        // The parameter line has the following format:
        // <taxi-id> <start date> <start pos (lat)> <start pos (long)> <end date> <end pos (lat)> <end pos (long)>
        String[] split = line.split(" ");
        return sphericalEarthDistance(split[2], split[3], split[5], split[6]);
    }

    /**
     * Calculate the flat surface spherical distance in kilometres between (lat1Str, long1Str) and (lat2Str, long2Str)
     * where latXStr is the String representation of the latitude of point X, and longXStr is the String representation
     * of the longitude of point X.
     *
     * <br>Based on: <a href="https://en.wikipedia.org/wiki/Geographical_distance#Flat-surface_formulae">Wikipedia's article</a>
     *
     * @param lat1Str: The latitude of point1 in String representation
     * @param long1Str: the longitude of point1 in String representation
     * @param lat2Str: the latitude of point2 in String representation
     * @param long2Str: the longitude of point2 in String representation
     * @return the spherical distance in kilometers between point1 and point2
     * @throws NumberFormatException: when a latitude or longitude can't be parsed into a double
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
