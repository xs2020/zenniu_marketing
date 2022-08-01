package cn.doitedu.rule.marketing.utils;

import cn.doitedu.rule.marketing.beans.EventBean;
import cn.doitedu.rule.marketing.beans.EventCondition;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class EventUtil {

    public static int sequenceStrMatchRegexCount(String str,String pattern){
        Pattern r = Pattern.compile(pattern);
        Matcher matcher = r.matcher(str);
        int count = 0;
        while(matcher.find()) count++;
        return count;
    }

     public static boolean eventMatchCondition(EventBean eventBean, EventCondition eventCondition){
         if (eventBean.getEventId().equals(eventCondition.getEventId())) {
             Set<String> keys = eventCondition.getEventProperties().keySet();
             for (String key : keys) {
                 String conditionValue = eventCondition.getEventProperties().get(key);
                 if (!conditionValue.equals(eventBean.getProperties().get(key))) {
                     return false;
                 }
             }
             return true;
         }
         return false;
     }
}
