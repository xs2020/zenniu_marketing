package cn.doitedu.rule.marketing.utils;

import org.apache.commons.lang3.time.DateUtils;

import java.util.Calendar;
import java.util.Date;

public class CrossTimeQueryUtil {
    public static long getSegmentPoint(long timeStamp){
        Date dt = DateUtils.ceiling(new Date(timeStamp-2*60*60*1000), Calendar.HOUR);
        return dt.getTime();
    }
}
