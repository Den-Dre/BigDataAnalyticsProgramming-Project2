import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TripReconstructor {

    // Emits: <(TaxiID,StartDate), Record> Key value pairs
    // TODO optimise this: maybe we can get away with writing only a part of the record as value?
    public static class SegmentsMapper extends Mapper<Object, Text, Text, Text> {
        private boolean printKeyValues;
        // Create only one Text object, rather than creating a new one in every map call
        private final Text idDate = new Text();
        private String[] parts;

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context) {
            Configuration conf = context.getConfiguration();
            printKeyValues = conf.getBoolean("printKeyValues", false);
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // We get as input one line of text and tokenize it into its separate parts
            parts = value.toString().split(",");
            // E,E segments are of no use to reconstruct trips:
            // We don't send these over the network to limit network delay
            if (emptySegment(parts))
                return;
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
    }

    // TODO use a combiner?

    public static class SegmentsReducer extends Reducer<Text, Text, Text, Text> {
        private boolean printKeyValues;
        private boolean printTooFastTrips;
        private final Logger logger = LoggerFactory.getLogger(SegmentsReducer.class);

        Text startOfTripRecord = new Text();
        Text value = new Text();

        double tripDistance = 0;
        boolean airportTrip = false;
        boolean tripActive = false;

        String[] parts;
        String[] tempParts;
//        StringBuilder coordinates = new StringBuilder();
        String tripString;

        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) {
            Configuration conf = context.getConfiguration();
            printKeyValues = conf.getBoolean("printKeyValues", false);
            printTooFastTrips = conf.getBoolean("printTooFastTrips", false);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) {
            for (Text trip : values) {

                tripString = trip.toString();
                if (tripString.contains("NULL")) {
                    // TODO should trips that contain incomplete records still be considered valid?
//                    tripActive = false;
                    continue;
                }

                // Defer splitting of a record to when it's absolutely necessary
                // (i.e. when the test above is false)
                tempParts = tripString.split(",");

                // Skip incomplete records
                if (tempParts.length != 9) {
                    // TODO should trips that contain incomplete records still be considered valid?
//                    tripActive = false;
                    continue;
                }
                parts = tempParts;

                try {
                    if (tripActive) {
                        // We consider a trip as an airport trip if at least one of its GPS locations is within a 1km range of the airport
                        if (!airportTrip)
                            airportTrip = isAirportTrip(parts[2], parts[3]);
//                        airportTrip = airportTrip || isAirportTrip(parts[2], parts[3]);
                        tripDistance += GPSUtil.sphericalEarthDistance(
                                parts[2],
                                parts[3],
                                parts[6],
                                parts[7]
                        );
//                        coordinates.append(parts[2]).append(",").append(parts[3]).append(" ");
                    }

                    // Start measuring a new trip if the Taxi's state changes from E (empty) to M (occupied)
                    // TODO also allow trip to start when first record is already M,M?
                    if (!tripActive && tripStartsNow(parts)) {
                        tripActive = true;
                        startOfTripRecord.set(trip);
//                        coordinates.append(parts[2]).append(",").append(parts[3]).append(" ");
                    } else if (tripActive && tripEndsNow(parts)) {
                        if (airportTrip) {
                            value.set(
                                    trip + " - "
                                    + calculateFee(tripDistance) + " ; "
                                    + tripDistance
//                                    + (printTooFastTrips ? " - " + coordinates : "")
                            );
                            context.write(startOfTripRecord, value);
                            airportTrip = false;
                        }
                        // TODO do we need to process trips that are not airport trips?
//                    else {
//                        context.write(startOfTripRecord, trip);
//                    }
//                    printGMapsURL(startOfTripRecord, trip);
                        tripActive = false;
                        tripDistance = 0;
                        if (printKeyValues) System.out.println("REDUCE: " + startOfTripRecord + " : " + trip);
                    }

                    // We're still in a trip:
                    // Check if one of the current trip's segments exceeds a speed of 200 km/h
                    if (tripActive && !realisticSpeed(parts)) {
                        tripActive = false;
                        airportTrip = false;
                        tripDistance = 0.0;
                        logger.info("Record with excessive speed: " + trip);
                    }
                } catch (Exception e) {
                    System.out.println("(Parse) exception: " + e +  " in record: " + trip);
                    logger.info("Malformed record: " + trip);
                }
            }

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
        private void printGMapsURL(Text start, Text end) {
            final String baseURL = "https://www.google.com/maps/dir/";
            final String startCoordinates = start.toString().split(",")[2] + "%2C" + start.toString().split(",")[3];
            final String endCoordinates = end.toString().split(",")[2] + "%2C" + end.toString().split(",")[3];
            String query = "?api=1&origin=" + startCoordinates + "&destination=" + endCoordinates;
            System.out.println(baseURL + query);
        }

        // Check whether the speed at which the taxi traveled between the start and end point
        // of this record is realistic (i.e. < 200 km/h)
        private boolean realisticSpeed(String[] parts) {
            final double MAX_SPEED = 200.0;
            final String DATE_FORMAT = "yyyyy-MM-dd hh:mm:ss";
            DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
            TimeZone timeZone = TimeZone.getTimeZone("America/Los_Angeles");
            dateFormat.setTimeZone(timeZone);
            Date startDate;
            Date endDate;

            try {
                startDate = dateFormat.parse(parts[1].replace("'", ""));
                endDate = dateFormat.parse(parts[5].replace("'", ""));
            } catch (ParseException e) {
                // If we can't parse the records, they must be malformed.
                // Thus, we see this as an invalid speed
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
            int chosenPartition = text.toString().split(",")[0].hashCode() % numPartitions;
            // System.out.println("text: " + text + " text2: " + text2);
            // System.out.println("Num Partitions: " + numPartitions + ", chose partition: " + chosenPartition);
            return chosenPartition;
        }
    }

    public static void main(String[] args) throws Exception {
	// Initialise log4j
	//BasicConfigurator.configure();

        FileUtils.deleteDirectory(new File("../../../output"));
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        List<String> otherArgs = new ArrayList<>(Arrays.asList(remainingArgs));

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
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
