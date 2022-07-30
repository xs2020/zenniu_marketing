package cn.doitedu.rule.engine.Utils;

import cn.doitedu.rule.engine.beans.EventBean;
import cn.doitedu.rule.engine.beans.EventCondition;

import java.util.Set;

public class EventParamComparator {

    public static boolean compare(EventCondition ruleEvent, EventCondition target){
        if(ruleEvent.getEventId().equals(target.getEventId())){
            Set<String> keys = ruleEvent.getEventProperties().keySet();
            for(String k:keys){
                if(!ruleEvent.getEventProperties().get(k).equals(target.getEventProperties().get(k))){
                  return false;
                }
            }
            return true;
        }
        return false;
    }

    public static boolean compare(EventCondition ruleEvent, EventBean target){
        if(ruleEvent.getEventId().equals(target.getEventId())){
            Set<String> keys = ruleEvent.getEventProperties().keySet();
            for(String k:keys){
                if(!ruleEvent.getEventProperties().get(k).equals(target.getProperties().get(k))){
                    return false;
                }
            }
            return true;
        }
        return false;
    }

}
