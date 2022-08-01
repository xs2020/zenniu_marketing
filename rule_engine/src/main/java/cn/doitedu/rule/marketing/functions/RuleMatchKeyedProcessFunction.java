package cn.doitedu.rule.marketing.functions;


import cn.doitedu.rule.marketing.beans.EventBean;
import cn.doitedu.rule.marketing.beans.MarketingRule;
import cn.doitedu.rule.marketing.beans.RuleMatchResult;
import cn.doitedu.rule.marketing.controller.TriggerModelRuleMatchController;
import cn.doitedu.rule.marketing.utils.EventUtil;
import cn.doitedu.rule.marketing.utils.RuleSimulator2;
import cn.doitedu.rule.marketing.utils.StateDescContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;


@Slf4j
public class RuleMatchKeyedProcessFunction extends KeyedProcessFunction<String, EventBean, RuleMatchResult> {

    MarketingRule rule;
    List<MarketingRule> ruleList;
    ListState<EventBean> listState;
    TriggerModelRuleMatchController triggerModelRuleMatchController;

    @Override
    public void open(Configuration parameters) throws Exception {
        //拿到规则
        rule = RuleSimulator2.getRule();
        ruleList = Arrays.asList(rule);

        listState = getRuntimeContext().getListState(StateDescContainer.getEventBeansDesc());
        triggerModelRuleMatchController = new TriggerModelRuleMatchController(listState);
    }

    @Override
    public void processElement(EventBean event, Context context, Collector<RuleMatchResult> out) throws Exception {


        for (MarketingRule rule : ruleList) {
            boolean b = triggerModelRuleMatchController.ruleIsMatch(rule, event);
            if(b){
                RuleMatchResult ruleMatchResult = new RuleMatchResult(event.getDeviceId(), rule.getRuleId(), event.getTimeStamp(), System.currentTimeMillis());
                out.collect(ruleMatchResult);
            }
        }
    }
}
