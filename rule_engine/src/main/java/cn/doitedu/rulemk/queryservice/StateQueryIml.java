package cn.doitedu.rulemk.queryservice;

import cn.doitedu.rulemk.Utils.EventParamComparator;
import cn.doitedu.rulemk.beans.EventBean;
import cn.doitedu.rulemk.beans.EventParam;
import cn.doitedu.rulemk.beans.EventSequenceParam;
import org.apache.flink.api.common.state.ListState;

import java.util.List;

public class StateQueryIml implements QueryService{
    ListState<EventBean> eventBeanListState;
    public StateQueryIml(ListState<EventBean> eventBeanListState){
        this.eventBeanListState = eventBeanListState;
    }

    public int queryEventSequence(List<EventParam> eventSequence,long timeRangeStart,long timeRangeEnd) throws Exception {
        Iterable<EventBean> eventBeans = eventBeanListState.get();

        int i = 0;
        int maxStep = 0;

        for (EventBean eventBean : eventBeans) {
            if(eventBean.getTimeStamp()>=timeRangeStart
                    && eventBean.getTimeStamp()<=timeRangeEnd
                    && EventParamComparator.compare(eventSequence.get(i),eventBean)){
                i++;
                maxStep++;
                if(maxStep == eventSequence.size()) return maxStep;

            }
        }
        return maxStep;
    }
}
