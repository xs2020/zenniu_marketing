package cn.doitedu.rulemk.beans;


import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class RuleConditions {

    //规则ID
    private String ruleId;

    //触发事件
     private EventParam trigEvent;

    //画像属性
    private Map<String,String> userProfiles;

    //行为事件次数
    private List<EventParam> actionCountConditionsList;

    //行为序列事件
    private List<EventSequenceParam> actionSequenceConditionsList;

}
