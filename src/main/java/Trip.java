import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Trip { // implements Writable // to be able to use as value within MapReduce
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
