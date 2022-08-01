package cn.doitedu.rule.marketing.controller;

import cn.doitedu.rule.marketing.beans.EventBean;
import cn.doitedu.rule.marketing.beans.EventCombinationCondition;
import cn.doitedu.rule.marketing.beans.EventCondition;
import cn.doitedu.rule.marketing.beans.MarketingRule;
import cn.doitedu.rule.marketing.service.TriggerModelRuleMatchServiceImpl;
import cn.doitedu.rule.marketing.utils.EventUtil;
import org.apache.flink.api.common.state.ListState;

import java.util.List;
import java.util.Map;

public class TriggerModelRuleMatchController {

    TriggerModelRuleMatchServiceImpl triggerModelRuleMatchService;

    public TriggerModelRuleMatchController(ListState<EventBean> listState) throws Exception {

        triggerModelRuleMatchService = new TriggerModelRuleMatchServiceImpl(listState);
    }

    public boolean ruleIsMatch(MarketingRule rule,EventBean eventBean) throws Exception {

        //判断当前事件是否满足触发条件
        EventCondition triggerEventCondition = rule.getTriggerEventCondition();
        if(!EventUtil.eventMatchCondition(eventBean,triggerEventCondition)) return false;

        //判断规则中是否有画像条件，并计算
        Map<String, String> userProfileCondition = rule.getUserProfileCondition();
        if(userProfileCondition !=null && userProfileCondition.size()>0){
            boolean b = triggerModelRuleMatchService.matchProfileCondition(userProfileCondition, eventBean.getDeviceId());
            if(!b) return false;
        }

        // 判断规则中是否有行为组合条件，并进行计算  ①  ②  ③  ④     。。。  p1 ||  （ ① || ②  &&  ③  || ④ ）
        List<EventCombinationCondition> eventCombinationConditionList = rule.getEventCombinationConditionList();

        if(eventCombinationConditionList !=null && eventCombinationConditionList.size()>0){


            for (EventCombinationCondition eventCombinationCondition : eventCombinationConditionList) {
                boolean b = triggerModelRuleMatchService.matchEventCombinationCondition(eventCombinationCondition, eventBean);
                if(!b) return false;
            }
        }
        return true;
    }
}
