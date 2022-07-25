package cn.doitedu.rulemk.Utils;

import cn.doitedu.rulemk.beans.EventBean;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

public class StateDescContainer {
   //近期行为事件状态描述器

    public static ListStateDescriptor<EventBean> getEventBeansDesc(){
        ListStateDescriptor<EventBean> eventBeans = new ListStateDescriptor<>("event_beans", EventBean.class);
        //Flink设置state存储数据的时长
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2)).build();
        eventBeans.enableTimeToLive(ttlConfig);

        return eventBeans;
    }

}
