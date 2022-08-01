package cn.doitedu.rule.marketing.service;

import cn.doitedu.rule.marketing.beans.EventBean;
import cn.doitedu.rule.marketing.beans.EventCombinationCondition;
import cn.doitedu.rule.marketing.dao.ClickHouseQuerier;
import cn.doitedu.rule.marketing.dao.HbaseQuerier;
import cn.doitedu.rule.marketing.dao.StateQuerier;
import cn.doitedu.rule.marketing.utils.ConfigNames;
import cn.doitedu.rule.marketing.utils.ConnectionUtils;
import cn.doitedu.rule.marketing.utils.CrossTimeQueryUtil;
import cn.doitedu.rule.marketing.utils.EventUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.state.ListState;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Map;

public class TriggerModelRuleMatchServiceImpl {

    HbaseQuerier hbaseQuerier;
    ClickHouseQuerier clickHouseQuerier;
    StateQuerier stateQuerier;

    public TriggerModelRuleMatchServiceImpl(ListState<EventBean> listState) throws Exception {
        Config config = ConfigFactory.load();

        //构建hbase querier
        Connection hbaseConn = ConnectionUtils.getHbaseConnection();
        String tableName = config.getString(ConfigNames.HBASE_PROFILE_TABLE);
        String familyName = config.getString(ConfigNames.HBASE_PROFILE_FAMILY);
        hbaseQuerier = new HbaseQuerier(hbaseConn,tableName,familyName);

        //构建click house querier
        java.sql.Connection clickHouseConn = ConnectionUtils.getClickHouseConnection();
        clickHouseQuerier = new ClickHouseQuerier(clickHouseConn);

        //构建state querier
        stateQuerier = new StateQuerier(listState);
    }

    //查询画像条件是否满足
    public boolean matchProfileCondition(Map<String,String> profileCondition,String deviceId) throws IOException {
        return hbaseQuerier.queryProfileConditionIsMatch(profileCondition, deviceId);
    }


    /*
     *计算一个行为组合条件是否匹配
     */
    public boolean matchEventCombinationCondition(EventCombinationCondition eventCombinationCondition, EventBean event) throws Exception {
        long segmentPoint = CrossTimeQueryUtil.getSegmentPoint(event.getTimeStamp());
        //根据规则条件时间区间判断查询方式
        long conditionStart = eventCombinationCondition.getTimeRangeStart();
        long conditionEnd = eventCombinationCondition.getTimeRangeEnd();

        //查询state状态
        if(conditionStart>=segmentPoint){
            int count = stateQuerier.queryEventCombinationConditionCount(event.getDeviceId(), eventCombinationCondition, conditionStart, conditionEnd);
            return count>=eventCombinationCondition.getMinLimit() && count<=eventCombinationCondition.getMaxLimit();

        //查询click house
        }else if(conditionEnd<segmentPoint){
            int count = clickHouseQuerier.queryEventCombinationConditionCount(event.getDeviceId(), eventCombinationCondition, conditionStart, conditionEnd);
            return count>=eventCombinationCondition.getMinLimit() && count<=eventCombinationCondition.getMaxLimit();

        //跨界查询
        }else{

            //先查询state看是否满足
            int count = stateQuerier.queryEventCombinationConditionCount(event.getDeviceId(), eventCombinationCondition, segmentPoint, conditionEnd);
            if(count >=eventCombinationCondition.getMinLimit()) return true;

            // 分段组合查询,先从ck中查询序列字符串,再从state中查询序列字符串,拼接后作为整体匹配正则表达式
            String str1 = clickHouseQuerier.getEventCombinationConditionStr(event.getDeviceId(), eventCombinationCondition, conditionStart, segmentPoint);
            String str2 = stateQuerier.getEventCombinationConditionStr(event.getDeviceId(), eventCombinationCondition, segmentPoint, conditionEnd);
            count = EventUtil.sequenceStrMatchRegexCount(str1 + str2, eventCombinationCondition.getMatchPattern());
            return count>=eventCombinationCondition.getMinLimit() && count<=eventCombinationCondition.getMaxLimit();
        }

    }

}
