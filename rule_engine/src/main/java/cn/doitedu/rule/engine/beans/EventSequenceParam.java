package cn.doitedu.rule.engine.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class EventSequenceParam {
    private String ruleId;
    private long timeStart;
    private long timeEnd;
    private List<EventCondition> eventSequence;

    private String sequenceQuerySql;
}
