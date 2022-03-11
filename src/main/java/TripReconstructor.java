import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import scala.Int;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;

class TripReconstructor {
    static class Trip { // implements Writable
        private final int taxiID;
        private final Date startDate, endDate;
        private final double startPosLat, startPosLong, endPosLat, endPosLong;
        private final boolean startIsOccupied, endIsOccupied;

        private final static String DATE_FORMAT = "yyyyy-MM-dd hh:mm:ss";
        DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);

        public Trip(String[] parts) throws ParseException {
            TimeZone timeZone = TimeZone.getTimeZone("America/Los_Angeles");
            dateFormat.setTimeZone(timeZone);

            taxiID = Integer.parseInt(parts[0]);
            startDate = dateFormat.parse(parts[1]);
            startPosLat = Double.parseDouble(parts[2]);
            startPosLong = Double.parseDouble(parts[3]);
            startIsOccupied = parts[4].equals("M");
            endDate = dateFormat.parse(parts[5]);
            endPosLat = Double.parseDouble(parts[6]);
            endPosLong = Double.parseDouble(parts[7]);
            endIsOccupied = parts[8].equals("M");
        }
    }

    public static class SegmentsMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // We get as input one line of text and tokenize it into its separate parts
            // Sort on id + start date
            // Emits: <(TaxiID,StartDate), Record> Key value pairs
            String[] parts = value.toString().split(",");
            Text idDate = new Text(parts[0] + "," + parts[1]);
            context.write(idDate, value);
        }
    }

    public static class SegmentsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Text prevTrip = new Text();
            Text startTrip = new Text();
            int count = 0;

            for (Text trip : values) {
                if (count == 0) {
                    prevTrip.set(trip);
                    startTrip.set(trip);
                }
                if (!consecutiveTrips(prevTrip, trip)) {
                    context.write(new Text(startTrip), new Text(prevTrip));
                    startTrip.set(trip);
                }
                count++;
            }
        }

        private boolean consecutiveTrips(Text t1, Text t2) {
            return t1.toString().split(",")[5].equals(t2.toString().split(",")[1]);
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
            return Integer.parseInt(text.toString().split(",")[0]) % numPartitions;
        }
    }

//    public static class IDDateGrouperComparator implements WritableComparable {
//
////        /**
////         * Compares its two arguments for order.  Returns a negative integer,
////         * zero, or a positive integer as the first argument is less than, equal
////         * to, or greater than the second.<p>
////         * <p>
////         * The implementor must ensure that {@code sgn(compare(x, y)) ==
////         * -sgn(compare(y, x))} for all {@code x} and {@code y}.  (This
////         * implies that {@code compare(x, y)} must throw an exception if and only
////         * if {@code compare(y, x)} throws an exception.)<p>
////         * <p>
////         * The implementor must also ensure that the relation is transitive:
////         * {@code ((compare(x, y)>0) && (compare(y, z)>0))} implies
////         * {@code compare(x, z)>0}.<p>
////         * <p>
////         * Finally, the implementor must ensure that {@code compare(x, y)==0}
////         * implies that {@code sgn(compare(x, z))==sgn(compare(y, z))} for all
////         * {@code z}.<p>
////         * <p>
////         * It is generally the case, but <i>not</i> strictly required that
////         * {@code (compare(x, y)==0) == (x.equals(y))}.  Generally speaking,
////         * any comparator that violates this condition should clearly indicate
////         * this fact.  The recommended language is "Note: this comparator
////         * imposes orderings that are inconsistent with equals."<p>
////         * <p>
////         * In the foregoing description, the notation
////         * {@code sgn(}<i>expression</i>{@code )} designates the mathematical
////         * <i>signum</i> function, which is defined to return one of {@code -1},
////         * {@code 0}, or {@code 1} according to whether the value of
////         * <i>expression</i> is negative, zero, or positive, respectively.
////         *
////         * @param t1 the first object to be compared.
////         * @param t2 the second object to be compared.
////         * @return a negative integer, zero, or a positive integer as the
////         * first argument is less than, equal to, or greater than the
////         * second.
////         * @throws NullPointerException if an argument is null and this
////         *                              comparator does not permit null arguments
////         * @throws ClassCastException   if the arguments' types prevent them from
////         *                              being compared by this comparator.
////         */
////        @Override
////        public int compare(Text t1, Text t2) {
////            return Integer.parseInt(t1.toString().split(",")[0]) - Integer.parseInt(t2.toString().split(",")[1]);
////        }
////
////        @Override
////        public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
////            return 0;
////        }
//
//        /**
//         * Compares this object with the specified object for order.  Returns a
//         * negative integer, zero, or a positive integer as this object is less
//         * than, equal to, or greater than the specified object.
//         *
//         * <p>The implementor must ensure
//         * {@code sgn(x.compareTo(y)) == -sgn(y.compareTo(x))}
//         * for all {@code x} and {@code y}.  (This
//         * implies that {@code x.compareTo(y)} must throw an exception iff
//         * {@code y.compareTo(x)} throws an exception.)
//         *
//         * <p>The implementor must also ensure that the relation is transitive:
//         * {@code (x.compareTo(y) > 0 && y.compareTo(z) > 0)} implies
//         * {@code x.compareTo(z) > 0}.
//         *
//         * <p>Finally, the implementor must ensure that {@code x.compareTo(y)==0}
//         * implies that {@code sgn(x.compareTo(z)) == sgn(y.compareTo(z))}, for
//         * all {@code z}.
//         *
//         * <p>It is strongly recommended, but <i>not</i> strictly required that
//         * {@code (x.compareTo(y)==0) == (x.equals(y))}.  Generally speaking, any
//         * class that implements the {@code Comparable} interface and violates
//         * this condition should clearly indicate this fact.  The recommended
//         * language is "Note: this class has a natural ordering that is
//         * inconsistent with equals."
//         *
//         * <p>In the foregoing description, the notation
//         * {@code sgn(}<i>expression</i>{@code )} designates the mathematical
//         * <i>signum</i> function, which is defined to return one of {@code -1},
//         * {@code 0}, or {@code 1} according to whether the value of
//         * <i>expression</i> is negative, zero, or positive, respectively.
//         *
//         * @param o the object to be compared.
//         * @return a negative integer, zero, or a positive integer as this object
//         * is less than, equal to, or greater than the specified object.
//         * @throws NullPointerException if the specified object is null
//         * @throws ClassCastException   if the specified object's type prevents it
//         *                              from being compared to this object.
//         */
//        @Override
//        public int compareTo(Object o) {
//            return Integer.parseInt(this.toString().split(",")[0]) - Integer.parseInt(o.toString().split(",")[1]);
//        }
//
//        @Override
//        public void write(DataOutput dataOutput) throws IOException {
//            dataOutput.writeUTF(this.toString());
//        }
//
//        @Override
//        public void readFields(DataInput dataInput) throws IOException {
//
//        }
//    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Trip reconstruction");
        job.setJarByClass(TripReconstructor.class);
        job.setMapperClass(SegmentsMapper.class);
//        job.setGroupingComparatorClass();
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(SegmentsReducer.class);
        job.setPartitionerClass(IDPartitioner.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}