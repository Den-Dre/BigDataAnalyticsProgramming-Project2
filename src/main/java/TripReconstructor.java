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

import java.awt.geom.Path2D;
import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class TripReconstructor {

    // Emits: <(TaxiID,StartDate), Record> Key value pairs
    // TODO optimise this: maybe we can get away with writing only a part of the record as value?
    public static class SegmentsMapper extends Mapper<Object, Text, Text, Text> {
        private boolean printKeyValues;
        // Create only one Text object, rather than creating a new one in every map call
        private final Text idDate = new Text();
        private String[] parts;
        final static double LONG_TOPR = 37.8133;
        final static double LAT_TOPR = -122.3914;
        final static double LONG_TOPL = 37.780677;
        final static double LAT_TOPL = -122.514566;

        final static double LONG_BOTL = 37.603109;
        final static double LAT_BOTL = -122.489704;
        final static double LONG_BOTR = 37.605810;
        final static double LAT_BOTR = -122.362741;

        @Deprecated
        final static double[] longitudePoints = new double[]{LONG_TOPR, LONG_TOPL, LONG_BOTL, LONG_BOTR};
        @Deprecated
        final static double[] latitudePoints = new double[]{LAT_TOPR, LAT_TOPL, LAT_BOTL, LAT_BOTR};
        static Path2D path = new Path2D.Double();

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context) {
            Configuration conf = context.getConfiguration();
            printKeyValues = conf.getBoolean("printKeyValues", false);
        }

        // Checks whether both the start and the end coordinates
        // of a record are in a user-defined allowed region
        @Deprecated
        private boolean legalGPSCoordinates(String[] parts) {
            return path.contains(Double.parseDouble(parts[2]), Double.parseDouble(parts[3]))
                && path.contains(Double.parseDouble(parts[6]), Double.parseDouble(parts[7]));
        }

        // Returns true if point lies to the right of the given line, or underneath of if viewed horizontally
        // Returns 0 if the points are collinear
        // Based on: https://stackoverflow.com/a/3461533/15482295
        private static boolean segmentLiesOnLand(String[] parts) {
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

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // We get as input one line of text and tokenize it into its separate parts
            parts = value.toString().split(",");
            // E,E segments are of no use to reconstruct trips:
            // We don't send these over the network to limit delay due to network transmission
            if (parts.length != 9 || containsNull(value) || emptySegment(parts) || !segmentLiesOnLand(parts)) {
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

        private boolean containsNull(Text record) {
            return record.toString().contains("NULL");
        }
    }

    // TODO use a combiner?

    public static class SegmentsReducer extends Reducer<Text, Text, Text, Text> {
        private boolean printKeyValues;
        private final Logger logger = LoggerFactory.getLogger(SegmentsReducer.class);

        Text startOfTripRecord = new Text();
        Text value = new Text();
        String[] prevParts;
        String[] parts;

        double tripDistance = 0;
        boolean airportTrip = false;
        boolean tripActive = false;
        private static final StringBuilder coordinates = new StringBuilder();

        private static final String DATE_FORMAT = "yyyyy-MM-dd hh:mm:ss";
        private static final DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        private static final TimeZone timeZone = TimeZone.getTimeZone("America/Los_Angeles");

        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) {
            Configuration conf = context.getConfiguration();
            printKeyValues = conf.getBoolean("printKeyValues", false);
            dateFormat.setTimeZone(timeZone);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Text trip : values) {
                parts = trip.toString().split(",");

                try {
                    if (tripActive) {
                        if (tooMuchTimeInbetween(prevParts, parts)) {
                            System.out.println("===" + Arrays.toString(prevParts) + " :: " + Arrays.toString(parts) + "===");
                            resetTrip();
                            continue;
                        }
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
                        coordinates.append(parts[6]).append(",").append(parts[7]).append("\n");
                    }

                    // Start measuring a new trip if the Taxi's state changes from E (empty) to M (occupied)
                    if (!tripActive && tripStartsNow(parts)) {
                        resetTrip();
                        tripActive = true;
                        // We set the start of this segment as start of the trip, as
                        // it the customers are picked up somewehre in-between the time
                        // of state 'E' and time of state 'M'.
                        startOfTripRecord.set(parts[1] + " " + parts[2] + "," + parts[3]); // Start date and coordinates
                    } else if (tripActive && tripEndsNow(parts)) {
                        // We do not need to process trips that are not airport trips
                        if (airportTrip) {
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
//                        logger.info("Record with excessive speed: " + trip);
                    }
                } catch (NumberFormatException e) {
                    System.out.println("(Parse) exception: " + e +  " in record: " + trip);
                    resetTrip();
                    logger.info("Malformed record: " + trip);
                }
                if (tripActive) {
                    prevParts = parts;
                }
            }
        }

        // Cancel the current trip and set distance travelled to 0
        private void resetTrip() {
            coordinates.setLength(0);
            prevParts = null;
            tripActive = false;
            airportTrip = false;
            tripDistance = 0.0;
        }

        private boolean tooMuchTimeInbetween(String[] prevParts, String[] parts) {
            Date endFirstTrip;
            Date startSecondTrip;

            try {
                endFirstTrip = dateFormat.parse(prevParts[5].replace("'", ""));
                startSecondTrip = dateFormat.parse(parts[1].replace("'", ""));
            } catch (ParseException e) {
                // If we can't parse the records, they must be malformed.
                logger.info("Date parse exception " + Arrays.toString(parts));
                return true;
            }
            // At most 10 seconds may pass in between two segments
            return (((double) startSecondTrip.getTime() - endFirstTrip.getTime()) / 1000) > 10;
        }

        // Check whether the given coordinates are within a range of 1km of the airport coordinates
        private boolean isAirportTrip(String latitude, String longitude) {
            final String latAirport = String.valueOf(37.62131);
            final String longAirport = String.valueOf(-122.37896);
            double distanceInKilometers = GPSUtil.sphericalEarthDistance(latAirport, longAirport, latitude, longitude);
            return distanceInKilometers <= 1;
        }

        // Calculate the fee based on the distance traveled and according to the given formula
        private String calculateFee(double distance) {
            return String.valueOf(3.25 + distance * 1.79);
        }

        // A trip starts as soon as we've encountered a record which switches the state from the texi form
        // Empty (E) to Occupied (M)
        private boolean tripStartsNow(String[] parts) {
            return parts[4].equals("'E'") && parts[8].equals("'M'");
        }

        // A trip ends as soon as we've encountered a record which switches the state from the texi form
        // Occupied (M) to Empty (E)
        private boolean tripEndsNow(String[] parts) {
            return parts[4].equals("'M'") && parts[8].equals("'E'");
        }

        // A utility method to construct the Google Maps API URL between two given coordinates
        public static void printGMapsURL(Text start, Text end) {
            final String baseURL = "https://www.google.com/maps/dir/";
            final String startCoordinates = start.toString().split(",")[2] + "%2C" + start.toString().split(",")[3];
            final String endCoordinates = end.toString().split(",")[2] + "%2C" + end.toString().split(",")[3];
            String query = "?api=1&origin=" + startCoordinates + "&destination=" + endCoordinates;
            System.out.println(baseURL + query);
        }
        public static String printGMapsURL(String[] parts) {
            System.out.println("https://www.google.com/maps/dir/?api=1&origin=" + parts[2] + "%2C" + parts[3] + "&destination=" + parts[6] + "%2C" + parts[7]);
            return "https://www.google.com/maps/dir/?api=1&origin=" + parts[2] + "%2C" + parts[3] + "&destination=" + parts[6] + "%2C" + parts[7];
        }

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


    // Based on: https://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm/
    // This comparator is responsible for the sorting of the keys.
    // We first sort on the TaxiID, and if these are equal, we sort on the Start Date
    public static class IDDateSortComparator extends WritableComparator {
        public IDDateSortComparator() {
            super(Text.class, true);
        }

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

    // Based on: https://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm/
    // This grouping comparator is responsible for grouping the values into an iterators, which the reduce method receives.
    // We choose to group values based on TaxiID
    public static class TaxiIDGroupingComparator extends WritableComparator {
        protected TaxiIDGroupingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String id1 = a.toString().split(",")[0];
            String id2 = b.toString().split(",")[0];
            return id1.compareTo(id2);
        }
    }

    public static class IDPartitioner extends Partitioner<Text, Text> {

        /**
         * Get the partition number for a given key (hence record) given the total
         * number of partitions i.e. number of reduce-tasks for the job.
         *
         * <p>Typically a hash function on a all or a subset of the key.</p>
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

   public static class RevenuePerDayMapper extends Mapper<Object, Text, Text, DoubleWritable> {
       private final Text date = new Text();
       private final DoubleWritable revenue = new DoubleWritable();
       private final Logger logger = LoggerFactory.getLogger(RevenuePerDayMapper.class);
       Pattern yearMonthPattern = Pattern.compile("[0-9]{4}-[0-9]{2}");

       @Override
       protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
//           logger.info(key.toString() + " :: " + value.toString());
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

   public static class RevenuePerDayReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
       private final DoubleWritable revenue = new DoubleWritable();

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
