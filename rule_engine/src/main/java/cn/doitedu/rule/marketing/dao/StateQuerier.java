package cn.doitedu.rule.marketing.dao;


import cn.doitedu.rule.marketing.beans.EventBean;
import cn.doitedu.rule.marketing.beans.EventCombinationCondition;
import cn.doitedu.rule.marketing.beans.EventCondition;
import cn.doitedu.rule.marketing.utils.EventUtil;
import lombok.Data;
import org.apache.flink.api.common.state.ListState;

import java.util.List;

@Data
public class StateQuerier {
    private ListState<EventBean> listState;

    public StateQuerier(ListState<EventBean> listState) {
        this.listState = listState;
    }

    /**
     *在state中，根据组合条件以及时间范围，查询关心事件的1,2,3组合的字符串
     * @Param eventCombinationCondition 行为组合条件
     * @Param queryRangeStart 查询时间范围起始
     * @Param queryRangeEnd   查询时间范围结束
     * @return 关心事件对应编号的字符串
     */

    public String getEventCombinationConditionStr(String deviceId,
                                                  EventCombinationCondition eventCombinationCondition,
                                                  long queryRangeStart,
                                                  long queryRangeEnd) throws Exception {
        Iterable<EventBean> eventBeans = listState.get();
        List<EventCondition> eventConditionList = eventCombinationCondition.getEventConditionList();
        StringBuilder sb = new StringBuilder();

        for (EventBean eventBean : eventBeans) {
            if(eventBean.getTimeStamp()<=queryRangeEnd && eventBean.getTimeStamp()>=queryRangeStart){
                for(int i=1;i<=eventConditionList.size();i++){
                    if(EventUtil.eventMatchCondition(eventBean,eventConditionList.get(i-1))) {
                        sb.append(i);
                        break;
                    }
                }
            }
        }
        return sb.toString();
    }

    /**
     *在state中，根据组合条件以及时间范围，查询该组合条件出现的次数
     * @Param eventCombinationCondition 行为组合条件
     * @Param queryRangeStart 查询时间范围起始
     * @Param queryRangeEnd   查询时间范围结束
     * @return 出现的次数
     */
    public int queryEventCombinationConditionCount(String deviceId,
                                                   EventCombinationCondition eventCombinationCondition,
                                                   long queryRangeStart, long queryRangeEnd) throws Exception {

        String conditionStr = getEventCombinationConditionStr(deviceId, eventCombinationCondition, queryRangeStart, queryRangeEnd);

        int count = EventUtil.sequenceStrMatchRegexCount(conditionStr, eventCombinationCondition.getMatchPattern());
        return count;
    }
}
