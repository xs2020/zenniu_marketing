package cn.doitedu.rule.marketing.utils;


import cn.doitedu.rule.engine.beans.EventSequenceParam;
import cn.doitedu.rule.engine.beans.RuleConditions;
import cn.doitedu.rule.marketing.beans.EventCombinationCondition;
import cn.doitedu.rule.marketing.beans.EventCondition;
import cn.doitedu.rule.marketing.beans.MarketingRule;

import java.util.Arrays;
import java.util.HashMap;

/*
规则：
        触发事件：K事件，事件属性(p2=v1)
        画像属性条件：tag87=v2, tag26=v1
        行为次数条件：2021-06-18  ~ 当前，事件C(p6=v8,p12=v5) 做过>=2次
*/
public class RuleSimulator2 {

    public static MarketingRule getRule(){
        MarketingRule rule = new MarketingRule();
        rule.setRuleId("rule_001");


        //1.触发事件
        HashMap<String, String> map1 = new HashMap<>();
        map1.put("p2","v1");
        EventCondition triggerEvent = new EventCondition("K",map1,-1,Long.MAX_VALUE,1,999);
        rule.setTriggerEvent(triggerEvent);

        //2.画像条件
        HashMap<String, String> map2 = new HashMap<>();
        map2.put("tag87","v2");
        map2.put("tag26","v1");
        rule.setUserProfileCondition(map2);

        //3.单个用户行为次数条件
        String eventId = "C";
        HashMap<String, String> map3 = new HashMap<>();
        map3.put("p6","v8");
        map3.put("p12","v5");
        long startTime = -1L;
        long endTime = Long.MAX_VALUE;

        String sql1="" +
                "select \n" +
                "eventId \n" +
                " from zenniu_detail\n" +
                "where eventId='C' and properties[p6]='v8' and properties[p12]='v5 \n" +
                "and deviceId=? and timestamp between ? and ?";
        String rPattern1 = "(1)";
        EventCondition e = new EventCondition(eventId, map3, startTime,endTime,1,999);
        EventCombinationCondition eventCombinationCondition1 = new EventCombinationCondition(startTime, endTime, 1, 999, Arrays.asList(e), rPattern1, "ckQuery", sql1, "001");


        //4.行为序列
        long st = -1L;
        long ed = Long.MAX_VALUE;

        String eventId1 = "A";
        HashMap<String, String> m1 = new HashMap<>();
        m1.put("p1","v1");
        EventCondition ev1 = new EventCondition(eventId1, m1, st, ed, 1,999);

        String eventId2 = "B";
        HashMap<String, String> m2 = new HashMap<>();
        m2.put("p2","v2");
        EventCondition ev2 = new EventCondition(eventId2, m2, st, ed, 1,999);

        String eventId3 = "C";
        HashMap<String, String> m3 = new HashMap<>();
        m3.put("p3","v3");
        EventCondition ev3 = new EventCondition(eventId3, m3, st, ed, 1,999);
        String rPattern2 = "(1.*2.*3)";
        String sql2 = "" +
                "select \n" +
                "eventId \n" +
                "from zennoiu_detail\n" +
                "where deviceId = ? \n" +
                "and  timeStamp between ? And  ?\n" +
                "and " +
                "(eventId=’A’ and properties[‘p3’]=’v2’）\n" +
                "or\n" +
                "(eventId=’C’ and properties[‘p2’]=’v2’)\n" +
                "or\n" +
                "(eventId=’F’ and properties[‘p1’]=’v1’)\n" +
                ")";
        //EventSequenceParam eventSequenceParam = new EventSequenceParam("00000_1", startTime2, endTime2, Arrays.asList(ev1, ev2, ev3), seq1Sql);
        EventCombinationCondition eventCombinationCondition2 = new EventCombinationCondition(st, ed, 1, 999, Arrays.asList(ev1, ev2, ev3), rPattern2, sql2, "ck", "002");
        rule.setEventCombinationConditionList(Arrays.asList(eventCombinationCondition1,eventCombinationCondition2));

        return rule;
    }
}
