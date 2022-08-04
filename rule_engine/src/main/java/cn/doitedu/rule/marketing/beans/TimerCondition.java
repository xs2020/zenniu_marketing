package cn.doitedu.rule.marketing.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.kie.api.definition.rule.All;

import java.util.List;

/**
 * 规则定时条件封装类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TimerCondition {

    private long timeLate;

    private List<EventCombinationCondition> eventCombinationConditionList;
}
