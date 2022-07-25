package cn.doitedu.rulemk.functions;


import cn.doitedu.rulemk.Utils.RuleSimulator;
import cn.doitedu.rulemk.Utils.StateDescContainer;
import cn.doitedu.rulemk.beans.*;
import cn.doitedu.rulemk.router.SimpleQueryRouter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;



@Slf4j
public class RuleMatchKeyedProcessFunction extends KeyedProcessFunction<String, EventBean, RuleMatchResult> {

    SimpleQueryRouter simpleQueryRouter;
    ListState<EventBean> beansState;

    @Override
    public void open(Configuration parameters) throws Exception {
        simpleQueryRouter = new SimpleQueryRouter();
        beansState = getRuntimeContext().getListState(StateDescContainer.getEventBeansDesc());
    }

    @Override
    public void processElement(EventBean event, Context context, Collector<RuleMatchResult> out) throws Exception {

        //将数据event放入flink state
         beansState.add(event);
        //获取规则
        RuleConditions rule = RuleSimulator.getRule();

        boolean isMatch = simpleQueryRouter.ruleMatch(rule, event);

        if(!isMatch) return;

        RuleMatchResult ruleMatchResult = new RuleMatchResult(event.getDeviceId(), rule.getRuleId(), event.getTimeStamp(), System.currentTimeMillis());
        out.collect(ruleMatchResult);

    }
}
