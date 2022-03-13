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
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TripReconstructor {

    // Emits: <(TaxiID,StartDate), Record> Key value pairs
    public static class SegmentsMapper extends Mapper<Object, Text, Text, Text> {
        private boolean printKeyValues;

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context) {
            Configuration conf = context.getConfiguration();
            printKeyValues = conf.getBoolean("printKeyValues", true);
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // We get as input one line of text and tokenize it into its separate parts
            String[] parts = value.toString().split(",");
            Text idDate = new Text(parts[0] + "," + parts[1]); // TaxiID,Start date
            // Such that MR-framework will sort on id + start date
            context.write(idDate, value);
            if (printKeyValues) System.out.println("MAP: " + idDate + " : " + value);
        }
    }

    public static class SegmentsReducer extends Reducer<Text, Text, Text, Text> {
        private boolean printKeyValues;

        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) {
            Configuration conf = context.getConfiguration();
            printKeyValues = conf.getBoolean("printKeyValues", true);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Text startOfTripRecord = new Text();
            boolean tripActive = false;
            String[] parts;

            for (Text trip : values) {
                parts = trip.toString().split(",");
                if (!tripActive && tripStartsNow(parts)) { // TODO also allow trip to start when first record is already M,M?
                    tripActive = true;
                    startOfTripRecord.set(trip);
                } else if (tripActive && tripEndsNow(parts)) {
                    tripActive = false;
                    context.write(startOfTripRecord, trip);
                    printGMapsURL(startOfTripRecord, trip);
                    if (printKeyValues) System.out.println("REDUCE: " + startOfTripRecord  + " : " + trip);
                }

                // Check if one of the current trip's segments exceeds a speed of 200 km/h
//                tripActive = checkSpeed(parts); // TODO use dedicated method below
                if (tripActive) {
                    boolean realisticSpeed = false;
                    try {
                        realisticSpeed = realisticSpeed(parts);
                    } catch (ParseException e) {
                        e.printStackTrace();
                        tripActive = false;
                    }
                    if (!realisticSpeed) {
                        tripActive = false;
                    }
                }
            }
        }

        private boolean tripStartsNow(String[] parts) {
            return parts[4].equals("'E'") && parts[8].equals("'M'");
        }

        private boolean tripEndsNow(String[] parts) {
            return parts[4].equals("'M'") && parts[8].equals("'E'");
        }

        private void printGMapsURL(Text start, Text end) {
            final String baseURL = "https://www.google.com/maps/dir/";
            final String startCoordinates = start.toString().split(",")[2] + "%2C" + start.toString().split(",")[3];
            final String endCoordinates = end.toString().split(",")[2] + "%2C" + end.toString().split(",")[3];
            String query = "?api=1&origin=" + startCoordinates + "&destination=" + endCoordinates;
            System.out.println(baseURL + query);
        }

        private boolean realisticSpeed(String[] parts) throws ParseException {
            final double MAX_SPEED = 200.0;
            final String DATE_FORMAT = "yyyyy-MM-dd hh:mm:ss";
            DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
            TimeZone timeZone = TimeZone.getTimeZone("America/Los_Angeles");
            dateFormat.setTimeZone(timeZone);

            Date startDate = dateFormat.parse(parts[1].replace("'", ""));
            Date endDate = dateFormat.parse(parts[5].replace("'", ""));
            // endDate is always larger than or equal to startDate:
            double deltaT = ((double) endDate.getTime() - startDate.getTime()) / (1000 * 3600);
//            if (deltaT == 0) // TODO decide what to do when times are equal
//                return false;
            double deltaX = GPSUtil.sphericalEarthDistance(
                    Double.parseDouble(parts[2]),
                    Double.parseDouble(parts[3]),
                    Double.parseDouble(parts[6]),
                    Double.parseDouble(parts[7])
            );
            return deltaX / deltaT < MAX_SPEED;
        }

        private boolean checkSpeed(String[] parts) {
            boolean realisticSpeed;
            try {
                realisticSpeed = realisticSpeed(parts);
            } catch (ParseException e) {
                e.printStackTrace();
                return false;
            }
            return realisticSpeed;
        }
    }

    // Based on: https://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm/
    public static class IDDateComparator extends WritableComparator {
        public IDDateComparator() {
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
            System.out.println("PARTITION: " + Integer.parseInt(text.toString().split(",")[0]) % numPartitions);
            return Integer.parseInt(text.toString().split(",")[0]) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
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
        job.setSortComparatorClass(IDDateComparator.class);
//        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}