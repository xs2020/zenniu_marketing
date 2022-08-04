package cn.doitedu.rule.marketing.functions;


import cn.doitedu.rule.marketing.beans.EventBean;
import cn.doitedu.rule.marketing.beans.MarketingRule;
import cn.doitedu.rule.marketing.beans.RuleMatchResult;
import cn.doitedu.rule.marketing.beans.TimerCondition;
import cn.doitedu.rule.marketing.controller.TriggerModelRuleMatchController;
import cn.doitedu.rule.marketing.utils.EventUtil;
import cn.doitedu.rule.marketing.utils.RuleSimulator2;
import cn.doitedu.rule.marketing.utils.StateDescContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


@Slf4j
public class RuleMatchKeyedProcessFunction extends KeyedProcessFunction<String, EventBean, RuleMatchResult> {

    MarketingRule rule;
    List<MarketingRule> ruleList;
    ListState<EventBean> listState;
    TriggerModelRuleMatchController triggerModelRuleMatchController;
    ListState<Tuple2<MarketingRule, Long>> ruleTimerState;


    @Override
    public void open(Configuration parameters) throws Exception {
        //拿到规则
        rule = RuleSimulator2.getRule();
        ruleList = Arrays.asList(rule);

        listState = getRuntimeContext().getListState(StateDescContainer.getEventBeansDesc());
        triggerModelRuleMatchController = new TriggerModelRuleMatchController(listState);

        ruleTimerState = getRuntimeContext().getListState(StateDescContainer.ruleTimerStateDesc);
    }

    @Override
    public void processElement(EventBean event, Context ctx, Collector<RuleMatchResult> out) throws Exception {


        for (MarketingRule rule : ruleList) {
            boolean b = triggerModelRuleMatchController.ruleIsMatch(rule, event);
            if(b){
                if(rule.isOnTimer()){
                    List<TimerCondition> timerConditionList = rule.getTimerConditionList();

                    //限定定时条件里面只有一个条件
                    TimerCondition timerCondition = timerConditionList.get(0);
                    //注册定时器
                    ctx.timerService().registerEventTimeTimer(event.getTimeStamp()+timerCondition.getTimeLate());

                    ruleTimerState.add(Tuple2.of(rule,event.getTimeStamp()+timerCondition.getTimeLate()));

                }else{
                    RuleMatchResult ruleMatchResult = new RuleMatchResult(event.getDeviceId(), rule.getRuleId(), event.getTimeStamp(), System.currentTimeMillis());
                    out.collect(ruleMatchResult);
                }

            }

        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RuleMatchResult> out) throws Exception {
               //判断这个 "规则:定时点"，是否对应本次触发点
        Iterable<Tuple2<MarketingRule, Long>> ruleTimerIterator = ruleTimerState.get();
        Iterator<Tuple2<MarketingRule, Long>> iterator = ruleTimerIterator.iterator();

         while (iterator.hasNext()) {
             Tuple2<MarketingRule, Long> tp = iterator.next();
                 if (tp.f1 == timestamp) {
                     //如果对应，检查该规则的定时条件
                     MarketingRule rule = tp.f0;
                     TimerCondition timerCondition = rule.getTimerConditionList().get(0);

                     //调用controller去检查在条件指定的时间范围内，事件组合发生次数是否满足
                     boolean b = triggerModelRuleMatchController.ruleTimerIsMatch(timerCondition, ctx.getCurrentKey(), timestamp - timerCondition.getTimeLate(), timestamp);
                     //清除已经检查过的state里面的rule信息
                     iterator.remove();
                     if (b) {
                         RuleMatchResult ruleMatchResult = new RuleMatchResult(ctx.getCurrentKey(), rule.getRuleId(), timestamp, System.currentTimeMillis());
                         out.collect(ruleMatchResult);
                     }

                 if(tp.f1<timestamp){
                     iterator.remove();
                 }

             }
         }





    }
}
