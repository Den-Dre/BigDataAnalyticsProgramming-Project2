import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * A class which contains the code
 * to reconstruct taxi trips from segments
 * using a map-reduce implementation
 */
public class TripReconstructor {

    /**
     * A class which contains the first mapper described in report/report.pdf
     * Emits: <(TaxiID,StartDate), Record> Key value pairs
     */
    public static class SegmentsMapper extends Mapper<Object, Text, Text, Text> {
        private boolean printKeyValues;
        // Create only one Text object, rather than creating a new one in every map call
        private final Text idDate = new Text();

        /**
         * Gets the value of the printKeyValues debug parameter,
         * which can be passed during debugging to print out all
         * calculated records.
         *
         * @param context: the context to which this option is written
         */
        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context) {
            Configuration conf = context.getConfiguration();
            printKeyValues = conf.getBoolean("printKeyValues", false);
        }

        /**
         *
         * Returns true if point lies to the right of the given line, or underneath of if viewed horizontally
         * Returns 0 if the points are collinear
         * Used for debugging and discovering 'sea trips' as described in report/report.pdf
         * Based on: https://stackoverflow.com/a/3461533/15482295
         *
         * @param parts: the segment split in parts separated by a ','
         * @return true if point lies to the right of the given line, or underneath of if viewed horizontally
         *          Returns 0 if the points are collinear
         */
        @Deprecated
        protected static boolean segmentLiesOnLand(String[] parts) {
            final double latStartLine = 37.594330;
            final double longStartLine = -122.520747;
            final double latEndLine = 37.779711;
            final double longEndLine = -122.514836;
            // We only check one point of the record to limit calculation time
            final double latitudePoint = Double.parseDouble(parts[2]);
            final double longitudePoint = Double.parseDouble(parts[3]);
            return
                Math.signum((longEndLine - longStartLine) * (latitudePoint - latStartLine)
                - (latEndLine - latStartLine) * (longitudePoint - longStartLine)) <= 0
                    || (latitudePoint > latEndLine || latitudePoint < latStartLine);
        }

        /**
         * The first mapper as described in report/report.pdf
         * Gets records as input and emits: <(TaxiID,StartDate), Record> Key value pairs
         *
         * @param key An {@link Object} key linked to a line of the input file
         * @param value the record (i.e. a line) read in from the input file
         * @param context the {@link org.apache.hadoop.mapreduce.Mapper.Context} to which the resulting key value pairs are written
         * @throws IOException when the result can't be written to the {@link org.apache.hadoop.mapreduce.Mapper.Context}
         * @throws InterruptedException when the result can't be written to the {@link org.apache.hadoop.mapreduce.Mapper.Context}
         */
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // We get as input one line of text and tokenize it into its separate parts
            String[] parts = value.toString().split(",");

            // E,E segments are of no use to reconstruct trips:
            // We don't send these over the network to limit delay due to network transmission
            if (parts.length != 9 || containsNull(value) || emptySegment(parts)) {
                return;
            }

            // We create a composite key based on the TaxiID + Start Date
            // This composite key is used in the group comparator, key comparator and partitioner defined below
            idDate.set(parts[0] + "," + parts[1]);
            context.write(idDate, value);
            if (printKeyValues) System.out.println("MAP: " + idDate + " : " + value);
        }

        // Detect whether a segment doesn't contain any passengers at its start location, nor at its end location
        private boolean emptySegment(String[] parts) {
            return parts[4].equals("'E'") && parts[8].equals("'E'");
        }

        // Detect whether a segment contains NULL values
        private boolean containsNull(Text record) {
            return record.toString().contains("NULL");
        }
    }

    /**
     * A class which contains the reducer from the first
     * map-reduce job as described in report/report.pdf
     */
    public static class SegmentsReducer extends Reducer<Text, Text, Text, Text> {
        private boolean printKeyValues;
        private boolean printSeaTripCoordinates;
        private final Logger logger = LoggerFactory.getLogger(SegmentsReducer.class);

        Text startOfTripRecord = new Text();
        Text value = new Text();
        String[] prevParts;
        String[] parts;

        double tripDistance = 0;
        boolean airportTrip = false;
        boolean tripActive = false;

        private static final String DATE_FORMAT = "yyyyy-MM-dd hh:mm:ss";
        private static final DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        private static final TimeZone timeZone = TimeZone.getTimeZone("America/Los_Angeles");
        private Text seaSegment = null;
        private final StringBuilder coordinates = new StringBuilder();
        // The constants below are no longer used: they were used when the approach
        // to eliminate illegal coordinates was used
        private static final String THRESH_LONG = "-122.61";
        private static final String THRESH_LAT_BOT = "-122.61";
        private static final String THRESH_LAT_TOP = "-122.61";

        /**
         * Parse options that are passed when the program is called
         * @param context the {@link org.apache.hadoop.mapreduce.Reducer.Context} to be configured based on the passed
         *                options.
         *                - printKeyValues: prints all parsed segments
         *                - printSeaTripCoordinates: prints the full coordinates of encountered sea trips (cfr report/report.pdf)
         */
        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) {
            Configuration conf = context.getConfiguration();
            printKeyValues = conf.getBoolean("printKeyValues", false);
            printSeaTripCoordinates = conf.getBoolean("printSeaTripCoordinates", false);
            dateFormat.setTimeZone(timeZone);
        }

        /**
         * The reduce method of the first map-reduce task as described in report/report.pdf.
         * The method constructs trips, starting at a segment that transitions from the
         * E to the M state, followed by M-M segments and ends the trip when an M-E segment
         * is encountered.
         *
         * <p>This method doesn't count segments that are subset of an earlier encountered
         * segment of the same trip. Trips that haven't ended at the end of the data set are
         * not included either. Trips that contain a segment which exceeds an average speed
         * of 200 km/h are rejected. </p>
         *
         * <p>A trip that satisfies all the conditions mentioned above, is counted as an airport
         * trip if at least one of its segments is located within a 1Km range of the San Francisco
         * Airport. If a trip is not an airport trip, it is rejected.</p>
         *
         * <p>A special case are so called 'seatrips'. These are trips that are valid airport trips,
         * but end up in an invalid location, namely the Ocean to the west of San Francisco. These
         * trips constitute a negligible percentage of the total revenue of the dataset, thus it was
         * decided to count these as valid trips. See report/report.pdf for a detailed discussion of this
         * decision</p>
         *
         * @param key: {TaxiID,StartDate}: a composite key which encapsulates the TaxiID and the StartDate of this segment
         * @param values: An original segment as it occurred in the dataset, with the same TaxiID and StartDate as the key above.
         * @param context: the {@link org.apache.hadoop.mapreduce.Reducer.Context} to which the results are written
         * @throws IOException when results can't be written to the {@link org.apache.hadoop.mapreduce.Reducer.Context}
         * @throws InterruptedException when results can't be written to the {@link org.apache.hadoop.mapreduce.Reducer.Context}
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Text trip : values) {
                parts = trip.toString().split(",");

                // This is the more expensive alternative of rejecting segments
                // that are located at sea, as discussed in report/report.pdf:
                //if (!SegmentsMapper.segmentLiesOnLand(parts))
                //    seaSegment = trip;
                try {
                    if (tripActive) {
                        if (differentTaxiID()) {
                            // TaxiID of previous segment != taxiID of current segment
                            // Given the ordering of the records, this indicates that a
                            // trip is still active past the end of the recorded data
                            // We choose to view these trips as incomplete, and thus we reject them

                            // This check doesn't (and shouldn't) print anything:
//                            if (!prevParts[1].split(" ")[0].split("-")[2].equals("31"))
//                                System.out.println("=== Different ID issue ===");
                            resetTrip();
                            continue;
                        }
                        if (subsetOfPreviousSegment()) {
                            // Current segment is subset of the previous segment
                            // Other cases (partial overlap, or this segment being a superset of the previous)
                            // can't occur due to the ordering of the records and the checks above,
                            // i.e. !(parts[1].compareTo(prevParts[1]) >= 0 && parts[5].compareTo(prevParts[5]) <= 0) && parts[1].compareTo(prevParts[5]) != 0
                            // is always false at this point.
                            // -> Continue the trip, but don't update the previously seen segment:

                            // This check doesn't (and shouldn't) print anything
//                            if (!(parts[1].compareTo(prevParts[1]) >= 0 && parts[5].compareTo(prevParts[5]) <= 0) && parts[1].compareTo(prevParts[5]) != 0)
//                                System.out.println("=== Separated or partially overlapping segment ===");
                            continue;
                        }

                        // Coordinates of subsequent segments always lign up at this point
                        // This check doesn't (and shouldn't) print anything
//                        if (!prevParts[6].equals(parts[2]) || !prevParts[7].equals(parts[3]))
//                            System.out.println("=== Coordinates don't match ===");

                        // We consider a trip as an airport trip if at least one of its GPS locations is within a 1km range of the airport
                        if (!airportTrip)
                            airportTrip = isAirportTrip(parts[2], parts[3]);
                        // Distance of the first segment (E-M transition) is not included,
                        // but distance of the last segment (M-E transition) is included to balance out differences
                        tripDistance += GPSUtil.sphericalEarthDistance(
                                parts[2],
                                parts[3],
                                parts[6],
                                parts[7]
                        );
                        // if (seaSegment != null) coordinates.append(parts[2]).append(",").append(parts[3]).append("\n");
                        if (printSeaTripCoordinates) coordinates.append(trip).append("\n");
                    }

                    // Start measuring a new trip if the Taxi's state changes from E (empty) to M (occupied)
                    // We don't count trips that start with M-M records when a trip is not yet active, which
                    // represent trips that have already started before the start of our data
                    if (!tripActive && tripStartsNow(parts)) {
                        resetTrip();
                        tripActive = true;
                        // We set the start of this segment as start of the trip, as
                        // it the customers are picked up somewhere in-between the time
                        // of state 'E' and time of state 'M'.
                        startOfTripRecord.set(parts[1] + " " + parts[2] + "," + parts[3]); // Start date and coordinates
                    } else if (tripActive && tripEndsNow(parts)) {
                        // We do not need to process trips that are not airport trips
                        if (airportTrip) {
                            if (printSeaTripCoordinates && seaSegment != null) {
                                logger.info("=== Used sea segment in airport trip: " + seaSegment + " coordinates: \n" + coordinates.toString() + " ===");
                            }
                            value.set(parts[6] + "," + parts[7] + " - " + calculateFee(tripDistance)); // End coordinates of trip + fee
                            context.write(startOfTripRecord, value);
                        }
                        resetTrip();
                        if (printKeyValues) System.out.println("REDUCE: " + startOfTripRecord + " : " + trip);
                    }

                    // We're still in a trip:
                    // Check if one of the current trip's segments exceeds a speed of 200 km/h
                    if (tripActive && !realisticSpeed(parts)) {
                        resetTrip();
                    }
                } catch (NumberFormatException e) {
                    System.out.println("(Parse) exception: " + e +  " in record: " + trip);
                    resetTrip();
                    logger.info("Malformed record: " + trip);
                }
                if (tripActive) {
                    // If we're in a trip, keep track of the previously seen segment
                    prevParts = parts;
                }
            }
        }

        /**
         * Cancel the current trip, set distance travelled to 0.
         */
        private void resetTrip() {
            prevParts = null;
            tripActive = false;
            airportTrip = false;
            tripDistance = 0.0;
            seaSegment = null;
            if (printSeaTripCoordinates) coordinates.setLength(0);
        }

        /**
         * Check whether the given coordinates are within a range of 1km of the airport coordinates.
         *
         * @param latitude The latitude of the segment to be checked.
         * @param longitude The longitude of the segment to be checked.
         * @return True iff. the given segment's coordinates are located within 1Km of the San Francisco Airport
         */
        private boolean isAirportTrip(String latitude, String longitude) {
            final String latAirport = String.valueOf(37.62131);
            final String longAirport = String.valueOf(-122.37896);
            double distanceInKilometers = GPSUtil.sphericalEarthDistance(latAirport, longAirport, latitude, longitude);
            return distanceInKilometers <= 1;
        }

        /**
         * Calculate the fee based on the distance traveled and according to the given formula
         * @param distance The distance traveled in kilometers
         * @return The fee in dollars based on the given distance in kilometers,
         * calculated as 3.25 * distance + 179
         */
        private String calculateFee(double distance) {
            return String.valueOf(3.25 + distance * 1.79);
        }


        /**
         * A trip starts as soon as we've encountered a record which
         * switches the state from the texi form Empty (E) to Occupied (M)
         *
         * @param parts The segments split into parts by ',' occurrences
         * @return True iff this segment transitions from an E (empty) to an M (occupied) state
         */
        private boolean tripStartsNow(String[] parts) {
            return parts[4].equals("'E'") && parts[8].equals("'M'");
        }

        /**
         * A trip ends as soon as we've encountered a record which switches the state from the texi form
         * Occupied (M) to Empty (E)
         *
         * @param parts The segments split into parts by ',' occurrences
         * @return True iff this segment transitions from an M (occupied) to an E (empty) state
         */
        private boolean tripEndsNow(String[] parts) {
            return parts[4].equals("'M'") && parts[8].equals("'E'");
        }

        /**
         * Check whether the current segment occurred within the timespan of the previous segment
         *
         * @return True iff. the current segment is a time-wise subset of the previously encountered segment
         */
        private boolean subsetOfPreviousSegment() {
            return parts[1].compareTo(prevParts[1]) >= 0 && parts[5].compareTo(prevParts[5]) <= 0;
        }

        /**
         * Check whether the TaxiID of the current segment doesn't match that of the previous segment
         *
         * @return True iff the previous segment had a different TaxiID than the current segment
         */
        private boolean differentTaxiID() {
            return !prevParts[0].equals(parts[0]);
        }

        /**
         * A method used for debugging, to print an URL that uses the Google Maps API
         * to visualize the trip between the two given coordinates: start and end
         *
         * @param start The start coordinate of the trip to visualize
         * @param end The end coordinate of the trip to visualize
         */
        public static void printGMapsURL(Text start, Text end) {
            final String baseURL = "https://www.google.com/maps/dir/";
            final String startCoordinates = start.toString().split(",")[2] + "%2C" + start.toString().split(",")[3];
            final String endCoordinates = end.toString().split(",")[2] + "%2C" + end.toString().split(",")[3];
            String query = "?api=1&origin=" + startCoordinates + "&destination=" + endCoordinates;
            System.out.println(baseURL + query);
        }

        /**
         * A method used for debugging, to print an URL that uses the Google Maps API
         * to visualize the trip between the two given coordinates: start and end
         *
         * @param parts The parts of the segment to visualize, split by ',' occurrences
         */
        public static String printGMapsURL(String[] parts) {
            System.out.println("https://www.google.com/maps/dir/?api=1&origin=" + parts[2] + "%2C" + parts[3] + "&destination=" + parts[6] + "%2C" + parts[7]);
            return "https://www.google.com/maps/dir/?api=1&origin=" + parts[2] + "%2C" + parts[3] + "&destination=" + parts[6] + "%2C" + parts[7];
        }

        /**
         * Verify whether the given segment doesn't exceed an
         * average speed of 200 km/h. The speed is calculated based
         * off of a Flat Earth distance formula.
         *
         * @param parts The parts of the segment to check, separated by ','
         * @return True iff the given segment doesn't exceed a speed of 200 km/h
         */
        // Check whether the speed at which the taxi traveled between the start and end point
        // of this record is realistic (i.e. < 200 km/h)
        private boolean realisticSpeed(String[] parts) {
            final double MAX_SPEED = 200.0;
            Date startDate;
            Date endDate;

            try {
                startDate = dateFormat.parse(parts[1].replace("'", ""));
                endDate = dateFormat.parse(parts[5].replace("'", ""));
            } catch (ParseException e) {
                // If we can't parse the records, they must be malformed.
                // Thus, we see this as an invalid speed
                logger.info("Date parse exception");
                return false;
            }

            // endDate is always larger than or equal to startDate:
            double deltaT = ((double) endDate.getTime() - startDate.getTime()) / (1000 * 3600);
            if (deltaT == 0) // TODO decide what to do when times are equal
                return false;
            double deltaX = GPSUtil.sphericalEarthDistance(
                    parts[2],
                    parts[3],
                    parts[6],
                    parts[7]
            );
            return deltaX / deltaT < MAX_SPEED;
        }
    }

    /**
     * A class which contains the {@link WritableComparator}
     * used in the reduce phase of the first mapreduce job.
     */
    public static class IDDateSortComparator extends WritableComparator {
        public IDDateSortComparator() {
            super(Text.class, true);
        }

        /**
         * This comparator is responsible for the sorting done by the mapreduce framework
         * when the segment transition from the mapper to the reducer.
         * The segments are primarily sorted on the TaxiID, and if
         * these are equal, secondarily sorted on the Start Date
         *
         * Based on: https://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm/
         */
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String[] split1 = a.toString().split(",");
            String[] split2 = b.toString().split(",");
            int compare = split1[0].compareTo(split2[0]);
            if (compare != 0)
                return compare;
            return split1[1].compareTo(split2[1]);
        }
    }

    /**
     * A class which contains the {@link org.apache.hadoop.io.WritableComparator}
     * used in the sorting phase when executed by the mapreduce framework
     * when the segments transition from the mapper to the reducer.
     */
    public static class TaxiIDGroupingComparator extends WritableComparator {
        protected TaxiIDGroupingComparator() {
            super(Text.class, true);
        }

        /**
         * This grouping comparator is responsible for grouping the values into iterators,
         * which are received as input by the reduce method.
         * We choose to group segments based on their TaxiID
         *
         * Based on: https://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm/
         */
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String id1 = a.toString().split(",")[0];
            String id2 = b.toString().split(",")[0];
            return id1.compareTo(id2);
        }
    }

    /**
     * A class which contains the {@link Partitioner}
     * used in the first mapreduce job
     */
    public static class IDPartitioner extends Partitioner<Text, Text> {

        /**
         * Get the partition number for a given key (hence record) given the total
         * number of partitions i.e. number of reduce-tasks for the job.
         * We partition based on the hash of theTaxiID, such that records
         * of the * same Taxi end up at the same reduce task.
         *
         * @param text          the key to be partioned.
         * @param text2         the entry value.
         * @param numPartitions the total number of partitions.
         * @return the partition number for the <code>key</code>.
         */
        @Override
        public int getPartition(Text text, Text text2, int numPartitions) {
            // Partition based on the TaxiID: send every record of the same taxi to the same reducer
            return text.toString().split(",")[0].hashCode() % numPartitions;
        }
    }

    /**
     * A class which contains the mapper for the
     * second mapreduce job.
     */
    public static class RevenuePerDayMapper extends Mapper<Object, Text, Text, DoubleWritable> {
       private final Text date = new Text();
       private final DoubleWritable revenue = new DoubleWritable();
       private final Logger logger = LoggerFactory.getLogger(RevenuePerDayMapper.class);
       Pattern yearMonthPattern = Pattern.compile("[0-9]{4}-[0-9]{2}");

        /**
         * This mappers maps the output records received from the reducer from the
         * first mapreduce job of the form: {StartDate ,StartCoordinates},{EndCoordinates - TripRevenue}
         * to {Year-Month} records.
         *
         * @param key The composite key {StartDate ,StartCoordinates} received from the
         *            reducer from the first mapreduce job
         * @param value The {EndCoordinates - TripRevenue} value as received from the reducer from
         *              the first mapreduce job
         * @param context The {@link org.apache.hadoop.mapreduce.Mapper.Context} to which the results of
         *                this map operation are written
         * @throws IOException When the results can't be written to the {@link org.apache.hadoop.mapreduce.Mapper.Context}
         * @throws InterruptedException When the results can't be written to the {@link org.apache.hadoop.mapreduce.Mapper.Context}
         */
       @Override
       protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
           String yearMonth = value.toString().substring(1,8); // year-month
           if (!yearMonthPattern.matcher(yearMonth).matches()) {
               logger.info("Invalid date: " + yearMonth);
               return;
           }
           double revenueVal = Double.parseDouble(value.toString().split(" - ")[1]);
           date.set(yearMonth);
           revenue.set(revenueVal);
           context.write(date, revenue);
       }
   }

    /**
     * A class which contains the reducer from the second mapreduce task
     */
   public static class RevenuePerDayReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
       private final DoubleWritable revenue = new DoubleWritable();

        /**
         * This reducer received {Year-Month},Iterator< TripRevenue > pairs from the
         * map phase of the second mapreduce task. These records are aggregated by their
         * keys by summing their corresponding revenues, such that the monthly revenues are obtained
         * as a result. These monthly revenues are written to the provided {@link org.apache.hadoop.mapreduce.Reducer.Context}
         *
         * @param key the composite {Year-Month} key as received from the mapper of the second mapreduce job
         * @param values Iterator< TripRevenue >s to be aggregated, as grouped by the {@link TaxiIDGroupingComparator}.
         * @param context The {@link org.apache.hadoop.mapreduce.Reducer.Context} to which the results are written
         * @throws IOException When the results can't be written to the {@link org.apache.hadoop.mapreduce.Reducer.Context}
         * @throws InterruptedException When the results can't be written to the {@link org.apache.hadoop.mapreduce.Reducer.Context}
         */
       @Override
       protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
           double revenueSum = 0;
           for (DoubleWritable val : values) {
               revenueSum += val.get();
           }
           revenue.set(revenueSum);
           context.write(key, revenue);
       }
   }

    /**
     * Initialises and configures a new {@link Job} which represents the first
     * mapreduce task as described in report/report.pdf. This first job makes use
     * of the {@link SegmentsMapper}, {@link SegmentsReducer}, {@link TaxiIDGroupingComparator} and
     * {@link IDPartitioner} and {@link IDDateSortComparator} as described above.
     *
     * @param conf The {@link Configuration} which is to be configured
     * @param input The {@link Path} of the input file for this job
     * @param output The {@link Path} of the output file for this job
     * @return job: the configured {@link Job}
     * @throws IOException When the input or output {@link Path} can't be parsed or found
     */
    private static Job getTripReconstructorJob(Configuration conf, Path input, Path output) throws IOException {
        Job job = Job.getInstance(conf, "Trip reconstruction");
        job.setJarByClass(TripReconstructor.class);
        job.setMapperClass(SegmentsMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(SegmentsReducer.class);
        job.setPartitionerClass(IDPartitioner.class);
        job.setGroupingComparatorClass(TaxiIDGroupingComparator.class);
        job.setSortComparatorClass(IDDateSortComparator.class);
        //job.setNumReduceTasks(10); // TODO decide on the number of tasks: maybe 10 as there are 10 nodes in the DFS?
        System.out.println(job.getNumReduceTasks());

        final Logger logger = LoggerFactory.getLogger("TripReconstructor");
        logger.info("Number of reducers: " + job.getNumReduceTasks());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }

    /**
     /**
     * Initialises and configures a new {@link Job} which represents the second
     * mapreduce task as described in report/report.pdf. This second job makes use
     * of the {@link RevenuePerDayMapper} and {@link RevenuePerDayReducer} as described
     * above.
     *
     * @param input The {@link Path} of the input file for this job. This is the
     *              same as the output path of the first mapreduce job.
     * @param output The {@link Path} of the output file for this job. This file
     *               will contain the final output: the monthly revenues
     * @throws IOException When the input or output {@link Path} can't be parsed or found
     */
    private static Job getRevenuePerDayJob(Path input, Path output) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Revenue per day job");
        job.setJarByClass(TripReconstructor.class);
        job.setMapperClass(RevenuePerDayMapper.class);
        job.setReducerClass(RevenuePerDayReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }

    /**
     * The driver method which creates the {@link Job}s of the first and
     * second mapreduce tasks. The first mapreduce task takes the all.segments
     * dataset as input, processes it, passes its output to the input of the second
     * mapreduce job, which processes it to finally output the airport trip revenues
     * aggregated by month.
     *
     * @param args The command line arguments passed to the program. These are provided with the -D < argument > flag.
     * @throws Exception When the command line arguments can't be parsed.
     */
    public static void main(String[] args) throws Exception {
        try {
            FileUtils.deleteDirectory(new File("../../../output"));
        } catch (IOException ignored) {}
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        List<String> otherArgs = new ArrayList<>(Arrays.asList(remainingArgs));
        assert otherArgs.size() == 2:
                "Jar must be called as: hadoop jar <jarFile> <mainClass> <inputFile> <outputDir>";

        Path input = new Path(otherArgs.get(0));
        Path intermediaryOutput = new Path(otherArgs.get(1), "revenuePerTrip");
        Path output = new Path(otherArgs.get(1), "revenuePerMonth");

        Job tripReconstructorJob = getTripReconstructorJob(conf, input, intermediaryOutput);
        if (!tripReconstructorJob.waitForCompletion(true))
            System.exit(1);

        Job revenuePerMonthJob = getRevenuePerDayJob(intermediaryOutput, output);
        if (!revenuePerMonthJob.waitForCompletion(true))
            System.exit(1);
    }
}
