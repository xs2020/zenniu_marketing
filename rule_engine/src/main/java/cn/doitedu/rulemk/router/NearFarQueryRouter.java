package cn.doitedu.rulemk.router;

import cn.doitedu.rulemk.Utils.ConnectionUtils;
import cn.doitedu.rulemk.Utils.EventParamComparator;
import cn.doitedu.rulemk.Utils.SegmentQueryUtil;
import cn.doitedu.rulemk.beans.EventBean;
import cn.doitedu.rulemk.beans.EventParam;
import cn.doitedu.rulemk.beans.EventSequenceParam;
import cn.doitedu.rulemk.beans.RuleConditions;
import cn.doitedu.rulemk.queryservice.ClickHouseQueryIml;
import cn.doitedu.rulemk.queryservice.HbaseQueryServiceIml;
import cn.doitedu.rulemk.queryservice.StateQueryIml;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.hadoop.hbase.client.Connection;

import java.util.List;
import java.util.Map;

@Slf4j
public class NearFarQueryRouter {

    HbaseQueryServiceIml hbaseQueryServiceIml;
    ClickHouseQueryIml clickHouseQueryIml;
    StateQueryIml stateQueryIml;

    public NearFarQueryRouter(ListState<EventBean> listState) throws Exception {
        Connection hbaseconn = ConnectionUtils.getHbaseConnection();
        java.sql.Connection clickHouseConn = ConnectionUtils.getClickHouseConnection();
        hbaseQueryServiceIml = new HbaseQueryServiceIml(hbaseconn);
        clickHouseQueryIml = new ClickHouseQueryIml(clickHouseConn);
        stateQueryIml = new StateQueryIml(listState);
    }

    public boolean ruleMatch(RuleConditions rule, EventBean event, ListState<EventBean> eventBeansState) throws Exception {

        //1.判断事件是否满足触发条件
        if (!EventParamComparator.compare(rule.getTrigEvent(), event)) return false;
        log.debug("");

        //2.画像条件是否满足
        Map<String, String> userProfiles = rule.getUserProfiles();
        if (userProfiles != null) {
            boolean hbaseQueryResult = hbaseQueryServiceIml.hbaseQueryResult(event.getDeviceId(), userProfiles);
            if (!hbaseQueryResult) return false;
        }

        //3.用户行为条件过滤
        //获取时间分界点
        long segmentPoint = SegmentQueryUtil.getSegmentPoint(event.getTimeStamp());

        List<EventParam> actionCountConditions = rule.getActionCountConditionsList();
        if (actionCountConditions != null && actionCountConditions.size() > 0) {
            for (EventParam actionCountCondition : actionCountConditions) {

                //分界点左边，全部查询clickhouse
                if(actionCountCondition.getTimeRangeEnd()<=segmentPoint){
                    long count = clickHouseQueryIml.eventActionQueryCount(event.getDeviceId(), actionCountCondition);
                    if (count < actionCountCondition.getCountThreshHold()) return false;
                }
                //分界点右边，全部查询Flink state
                else if(actionCountCondition.getTimeRangeStart()>=segmentPoint){

                    long stateCount = stateQueryEventCount(eventBeansState, actionCountCondition, actionCountCondition.getTimeRangeStart(), actionCountCondition.getTimeRangeEnd());
                    if(stateCount<actionCountCondition.getCountThreshHold()) return false;
                }
                //跨界
                else {
                    //先查询state (分界点----规则时间条件结束点)
                    long countInState = stateQueryEventCount(eventBeansState, actionCountCondition, segmentPoint, actionCountCondition.getTimeRangeEnd());
                    if(countInState<actionCountCondition.getCountThreshHold()){
                        //查询click house(规则时间条件起始点-----分界点)
                        long countInClick = clickHouseQueryIml.eventActionQueryCount(event.getDeviceId(), actionCountCondition, actionCountCondition.getTimeRangeStart(), segmentPoint);
                        if(countInClick+countInState < actionCountCondition.getCountThreshHold()) return false;
                    }
                }

            }
        }


        //用户行为序列过滤
        List<EventSequenceParam> actionSequenceConditions = rule.getActionSequenceConditionsList();
        if(actionSequenceConditions != null && actionSequenceConditions.size()>0){
            for (EventSequenceParam actionSequenceCondition : actionSequenceConditions) {
                //条件的区间结束点<=分界点，只用查询click house
                if(actionSequenceCondition.getTimeEnd()<=segmentPoint){
                    int maxStep = clickHouseQueryIml.eventSequenceMatchResult(event.getDeviceId(), actionSequenceCondition);
                    if(maxStep < actionSequenceCondition.getEventSequence().size()) return false;
                }
                //条件的区间起始点>=分界点，只用查询state
                else if(actionSequenceCondition.getTimeStart()>=segmentPoint){
                    // 从state中查询指定事件序列的最大匹配步骤
                    int step = stateQueryIml.queryEventSequence(actionSequenceCondition.getEventSequence(), actionSequenceCondition.getTimeStart(), actionSequenceCondition.getTimeEnd());
                    if(step < actionSequenceCondition.getEventSequence().size()) return false;

                }
                //跨界查询
                else {
                    //跨界查询指定事件的最大匹配步骤

                        //查询click house
                        int countInClik = clickHouseQueryIml.eventSequenceMatchResult(event.getDeviceId(), actionSequenceCondition,actionSequenceCondition.getTimeStart(),segmentPoint);
                        if(countInClik < actionSequenceCondition.getEventSequence().size()){
                            //click house查询结果不满足条件，返回最大匹配步数，将匹配的事件从序列移除到 state查询剩余的事件是否匹配
                            List<EventParam> eventSequence = actionSequenceCondition.getEventSequence();
                            List<EventParam> trimedEventSequence = eventSequence.subList(countInClik, eventSequence.size());

                            int countInState2 = stateQueryIml.queryEventSequence(trimedEventSequence, segmentPoint, actionSequenceCondition.getTimeEnd());
                            if(countInClik+countInState2 < eventSequence.size()) return false;
                        }

                }

            }
        }
      return true;
    }

    /*
     *Flink State满足规则的事件计数
     */
    private long stateQueryEventCount(ListState<EventBean> eventBeansState, EventParam actionCountCondition, long queryStart, long queryEnd) throws Exception {
        long stateCount = 0;
        Iterable<EventBean> evtbeansItr = eventBeansState.get();
        for (EventBean eventBean : evtbeansItr) {
            //判断state取出的事件事件是否在规则要求时间区间内
            if(eventBean.getTimeStamp()>= queryStart && eventBean.getTimeStamp()<= queryEnd){
                if (EventParamComparator.compare(actionCountCondition,eventBean)) stateCount++;
            }
        }
        return stateCount;
    }
}
