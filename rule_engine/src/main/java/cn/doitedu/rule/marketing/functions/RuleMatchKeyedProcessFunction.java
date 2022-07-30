package cn.doitedu.rule.marketing.functions;


import cn.doitedu.rule.engine.router.SimpleQueryRouter;
import cn.doitedu.rule.marketing.beans.EventBean;
import cn.doitedu.rule.marketing.beans.MarketingRule;
import cn.doitedu.rule.marketing.beans.RuleMatchResult;
import cn.doitedu.rule.marketing.utils.EventUtil;
import cn.doitedu.rule.marketing.utils.RuleSimulator2;
import cn.doitedu.rule.marketing.utils.StateDescContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


@Slf4j
public class RuleMatchKeyedProcessFunction extends KeyedProcessFunction<String, EventBean, RuleMatchResult> {


    @Override
    public void open(Configuration parameters) throws Exception {
            }

    @Override
    public void processElement(EventBean event, Context context, Collector<RuleMatchResult> out) throws Exception {
        MarketingRule rule = RuleSimulator2.getRule();
        if(! EventUtil.eventMatchCondition(event,rule.getTriggerEvent())) return ;

        //查询用户画像

        //查询行为组合
    }
}
