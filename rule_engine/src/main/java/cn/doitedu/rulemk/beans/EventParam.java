package cn.doitedu.rulemk.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
@AllArgsConstructor
public class EventParam {

    private String eventId;
    private Map<String,String> eventProperties;
    private int countThreshHold;
    private long timeRangeStart;
    private long timeRangeEnd;
    private String querySql;
}
