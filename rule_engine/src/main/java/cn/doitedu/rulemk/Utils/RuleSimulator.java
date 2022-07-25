package cn.doitedu.rulemk.Utils;
import cn.doitedu.rulemk.beans.EventParam;
import cn.doitedu.rulemk.beans.EventSequenceParam;
import cn.doitedu.rulemk.beans.RuleConditions;
import java.util.Arrays;
import java.util.HashMap;

/*
规则：
        触发事件：K事件，事件属性(p2=v1)
        画像属性条件：tag87=v2, tag26=v1
        行为次数条件：2021-06-18  ~ 当前，事件C(p6=v8,p12=v5) 做过>=2次
*/
public class RuleSimulator {

    public static RuleConditions getRule(){
        RuleConditions ruleConditions = new RuleConditions();
        ruleConditions.setRuleId("00000_1");

        //1.触发事件
        HashMap<String, String> hashMap1 = new HashMap<>();
        hashMap1.put("p2","v1");
        EventParam event1 = new EventParam("K",hashMap1,0,-1,-1,"");

        ruleConditions.setTrigEvent(event1);

        //2.画像条件
        HashMap<String, String> hashMap2 = new HashMap<>();
        hashMap2.put("tag87","v2");
        hashMap2.put("tag26","v1");
        ruleConditions.setUserProfiles(hashMap2);

        //3.用户行为次数条件
        HashMap<String, String> hashMap3 = new HashMap<>();
        //hashMap3.put("p6","v8");
        //hashMap3.put("p12","v5");
        long startTime = 1623945600000L;
        long endTime = Long.MAX_VALUE;
        String eventId = "C";
        String sql="select count(1) from zenniu_detail where eventId='C' and properties[p6]='v8' and properties[p12]='v5 " +
                "and deviceId=? and timestamp between ? and ?";

        EventParam event3 = new EventParam(eventId, hashMap3, 1,startTime,endTime,sql);
        ruleConditions.setActionCountConditionsList(Arrays.asList(event3));

        //4.行为序列
        long startTime2 = 1623945600000L;
        long endTime2 = Long.MAX_VALUE;

        String eventId1 = "A";
        HashMap<String, String> m1 = new HashMap<>();
        m1.put("p1","v1");
        EventParam ev1 = new EventParam(eventId1, m1, -1, startTime2, endTime2, null);

        String eventId2 = "B";
        HashMap<String, String> m2 = new HashMap<>();
        m2.put("p2","v2");
        EventParam ev2 = new EventParam(eventId2, m2, -1, startTime2, endTime2, null);

        String eventId3 = "C";
        HashMap<String, String> m3 = new HashMap<>();
        m3.put("p3","v3");
        EventParam ev3 = new EventParam(eventId3, m3, -1, startTime2, endTime2, null);

        String seq1Sql = "select  deviceId,\n" +
                "            sequenceMatch(‘.*(?1).*(?2).*(?3).*’) (\n" +
                "            toDateTime(‘timeStamp’),\n" +
                "            eventId=’A’,\n" +
                "            eventId=’C’,\n" +
                "            eventId=’F’ ) as isMatch3,\n" +
                "\n" +
                "           sequenceMatch(‘.*(?1).*(?2).*’) (\n" +
                "            toDateTime(‘timeStamp’),\n" +
                "            eventId=’A’,\n" +
                "            eventId=’C’,\n" +
                "            eventId=’F’ ) as isMatch2,\n" +
                "\n" +
                "          sequenceMatch(‘.*(?1).*(?2).*’) (\n" +
                "            toDateTime(‘timeStamp’),\n" +
                "            eventId=’A’,\n" +
                "            eventId=’C’,\n" +
                "            eventId=’F’ ) as isMatch2\n" +
                "\n" +
                "from zennoiu_detail\n" +
                "where deviceId = ?\n" +
                "  and  timeStamp between ? And  ?\n" +
                "  and \n" +
                "      (\n" +
                "     (eventId=’A’ and properties[‘p3’]=’v2’）\n" +
                "      or\n" +
                "     (eventId=’C’ and properties[‘p2’]=’v2’)\n" +
                "      or\n" +
                "     (eventId=’F’ and properties[‘p1’]=’v1’)\n" +
                "      )\n" +
                "group by deviceId;";
        EventSequenceParam eventSequenceParam = new EventSequenceParam("00000_1", startTime2, endTime2, Arrays.asList(ev1, ev2, ev3), seq1Sql);

        ruleConditions.setActionSequenceConditionsList(Arrays.asList(eventSequenceParam));

        return ruleConditions;
    }
}
