package cn.doitedu.rulemk.Utils;

import cn.doitedu.rulemk.beans.EventBean;
import cn.doitedu.rulemk.beans.EventParam;

import java.util.Map;
import java.util.Set;

public class EventParamComparator {

    public static boolean compare(EventParam ruleEvent,EventParam target){
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

    public static boolean compare(EventParam ruleEvent, EventBean target){
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
