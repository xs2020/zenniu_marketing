package cn.doitedu.rule.marketing.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketingRule {

    private String ruleId;

    private EventCondition triggerEventCondition;

    private Map<String,String> userProfileCondition;

    private List<EventCombinationCondition>  eventCombinationConditionList;

    private boolean onTimer;

    private List<TimerCondition> timerConditionList;
}
