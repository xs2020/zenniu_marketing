package cn.doitedu.rule.engine.router;

import cn.doitedu.rule.engine.Utils.ConnectionUtils;
import cn.doitedu.rule.engine.Utils.EventParamComparator;
import cn.doitedu.rule.engine.beans.EventBean;
import cn.doitedu.rule.engine.beans.EventCondition;
import cn.doitedu.rule.engine.beans.EventSequenceParam;
import cn.doitedu.rule.engine.beans.RuleConditions;
import cn.doitedu.rule.engine.queryservice.ClickHouseQueryIml;
import cn.doitedu.rule.engine.queryservice.HbaseQueryServiceIml;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Connection;

import java.util.List;
import java.util.Map;

@Slf4j
public class SimpleQueryRouter {


    Connection hbaseconn;
    java.sql.Connection clickHouseConn;
    HbaseQueryServiceIml hbaseQueryServiceIml;
    ClickHouseQueryIml clickHouseQueryIml;

    public SimpleQueryRouter() throws Exception {
        hbaseconn = ConnectionUtils.getHbaseConnection();
        clickHouseConn = ConnectionUtils.getClickHouseConnection();
        hbaseQueryServiceIml = new HbaseQueryServiceIml(hbaseconn);
        clickHouseQueryIml = new ClickHouseQueryIml(clickHouseConn);
    }

    public boolean ruleMatch(RuleConditions rule, EventBean event) throws Exception {

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
        List<EventCondition> actionCountConditions = rule.getActionCountConditionsList();
        if (actionCountConditions != null && actionCountConditions.size() > 0) {
            for (EventCondition actionCountCondition : actionCountConditions) {
                long count = clickHouseQueryIml.eventActionQueryCount(event.getDeviceId(), actionCountCondition);
                if (count < actionCountCondition.getCountThreshHold()) return false;
            }
        }


        //TODO 用户行为序列过滤
        List<EventSequenceParam> actionSequenceConditions = rule.getActionSequenceConditionsList();
        if(actionSequenceConditions != null && actionSequenceConditions.size()>0){
            for (EventSequenceParam actionSequenceCondition : actionSequenceConditions) {
                int maxStep = clickHouseQueryIml.eventSequenceMatchResult(event.getDeviceId(), actionSequenceCondition);
                if(maxStep != actionSequenceCondition.getEventSequence().size()) return false;
            }
        }
      return true;
    }
}
