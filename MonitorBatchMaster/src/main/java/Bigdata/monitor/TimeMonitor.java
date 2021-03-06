package Bigdata.monitor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TimeMonitor {

    public static String getDate() {
        return LocalDateTime
                .now()
                .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    }
    public static String getDay(long timeStamp) {
        timeStamp *= 1000;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(":yyyyMMdd");
        return simpleDateFormat.format(timeStamp);
    }

    public static long getDayTimeStamp(String Day) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(":yyyyMMdd");
        return simpleDateFormat.parse(Day).getTime();
    }

    public static long getMinTimeStamp(String Day) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm");
        return simpleDateFormat.parse(Day).getTime();
    }

    public static String getMin(Long timeStamp) {
        String pattern = "yyyyMMddHHmm";
        timeStamp *= 1000;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        return simpleDateFormat.format(new Date(timeStamp));
    }

    public static String getLastDay( ){
        LocalDate localdate = LocalDate.now();
        return localdate.minusDays(1).toString().replace("-","");
    }
}
