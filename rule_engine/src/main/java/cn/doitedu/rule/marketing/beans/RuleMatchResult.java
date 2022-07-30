package cn.doitedu.rule.marketing.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RuleMatchResult {
    String deviceId;
    String ruleId;
    long trigEventTimeStamp;
    long matchTimeStamp;
}
