package cn.doitedu.rule.marketing.utils;

import cn.doitedu.rule.marketing.beans.EventBean;
import cn.doitedu.rule.marketing.beans.MarketingRule;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

public class StateDescContainer {
   //近期行为事件状态描述器

    public static ListStateDescriptor<EventBean> getEventBeansDesc(){
        ListStateDescriptor<EventBean> eventBeans = new ListStateDescriptor<>("event_beans", EventBean.class);
        //Flink设置state存储数据的时长
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2)).build();
        eventBeans.enableTimeToLive(ttlConfig);

        return eventBeans;
    }

    public static ListStateDescriptor<Tuple2<MarketingRule,Long>> ruleTimerStateDesc
        = new ListStateDescriptor<Tuple2<MarketingRule,Long>>("rule_timer", TypeInformation.of(new TypeHint<Tuple2<MarketingRule, Long>>() {
    }));

}
