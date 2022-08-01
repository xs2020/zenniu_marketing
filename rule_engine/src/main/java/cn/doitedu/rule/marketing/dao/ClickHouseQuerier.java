package cn.doitedu.rule.marketing.dao;

import cn.doitedu.rule.marketing.beans.EventCombinationCondition;
import cn.doitedu.rule.marketing.beans.EventCondition;
import cn.doitedu.rule.marketing.utils.EventUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ClickHouseQuerier {
    Connection conn;
    public ClickHouseQuerier(Connection conn){
        this.conn = conn;
    }

    /**
     *在click house中，根据组合条件以及时间范围，查询关心事件的1,2,3组合的字符串
     * @Param eventCombinationCondition 行为组合条件
     * @Param queryRangeStart 查询时间范围起始
     * @Param queryRangeEnd   查询时间范围结束
     * @return 关心事件对应编号的字符串
     */
    public String getEventCombinationConditionStr(String deviceId,
                                                   EventCombinationCondition eventCombinationCondition,
                                                   long queryRangeStart,
                                                   long queryRangeEnd) throws Exception {

        String querySql = eventCombinationCondition.getQuerySql();
        PreparedStatement preparedStatement = conn.prepareStatement(querySql);
        preparedStatement.setString(1,deviceId);
        preparedStatement.setLong(2,queryRangeStart);
        preparedStatement.setLong(3,queryRangeEnd);

        //从行为组合条件规则取出该规则所关心的事件列表[A,C,F]
        List<EventCondition> eventConditionList = eventCombinationCondition.getEventConditionList();
        List<String> ids = eventConditionList.stream().map(e -> e.getEventId()).collect(Collectors.toList());
        //遍历click house查询结果
        StringBuilder sb = new StringBuilder();
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()){
            String eventId = resultSet.getString(1);
            sb.append(ids.indexOf(eventId)+1);

        }
        return sb.toString();
    }

    /**
     *在click house中，根据组合条件以及时间范围，查询该组合条件出现的次数
     * @Param eventCombinationCondition 行为组合条件
     * @Param queryRangeStart 查询时间范围起始
     * @Param queryRangeEnd   查询时间范围结束
     * @return 出现的次数
     */
    public int queryEventCombinationConditionCount(String deviceId,
                                                   EventCombinationCondition eventCombinationCondition,
                                                   long queryRangeStart,
                                                   long queryRangeEnd) throws Exception {
        //先查询到该用户满足规则组合行为条件里面的事件序列
        String eventMatchStr = getEventCombinationConditionStr(deviceId, eventCombinationCondition, queryRangeStart, queryRangeEnd);

        //拿到规则行为组合条件的匹配正则
        String matchPattern = eventCombinationCondition.getMatchPattern();

        //判断匹配次数
        int count = EventUtil.sequenceStrMatchRegexCount(eventMatchStr, matchPattern);
        return count;
    }
}

