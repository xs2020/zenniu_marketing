package cn.doitedu.rule.engine.queryservice;

import cn.doitedu.rule.engine.beans.EventCondition;
import cn.doitedu.rule.engine.beans.EventSequenceParam;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ClickHouseQueryIml implements QueryService{

    Connection clickHouseConn;

    public ClickHouseQueryIml(Connection clickHouseConn) {
        this.clickHouseConn = clickHouseConn;
    }

     /*
      * 根据规则条件时间查询
      */
    public long eventActionQueryCount(String deviceId, EventCondition eventCondition) throws Exception {

        return eventActionQueryCount(deviceId, eventCondition, eventCondition.getTimeRangeStart(), eventCondition.getTimeRangeEnd());

    }
    /*
     *根据分界点查询click house
     */
    public long eventActionQueryCount(String deviceId, EventCondition eventCondition, long queryStart, long queryEnd) throws Exception {

        PreparedStatement pst = clickHouseConn.prepareStatement(eventCondition.getQuerySql());
        pst.setString(1,deviceId);
        pst.setLong(2,queryStart);
        pst.setLong(3,queryEnd);
        ResultSet resultSet = pst.executeQuery();
        long countResult = 0;
        while(resultSet.next()){
            countResult = resultSet.getLong(1);
        }
        return countResult;
    }

    public int eventSequenceMatchResult(String deviceId, EventSequenceParam sequenceParam) throws Exception {
        return eventSequenceMatchResult(deviceId,sequenceParam,sequenceParam.getTimeStart(),sequenceParam.getTimeEnd());
    }


    public int eventSequenceMatchResult(String deviceId, EventSequenceParam sequenceParam,long timeRangeStart,long timeRangeEnd) throws SQLException {
        PreparedStatement pst = clickHouseConn.prepareStatement(sequenceParam.getSequenceQuerySql());
        pst.setString(1,deviceId);
        pst.setLong(2,timeRangeStart);
        pst.setLong(3,timeRangeEnd);

        ResultSet result = pst.executeQuery();

        int maxStep = 0;
        while (result.next()){
            for (int i=0;i<sequenceParam.getEventSequence().size()-1;i++){
                long isMatch = result.getLong(i);
                if(isMatch==1){
                    maxStep = sequenceParam.getEventSequence().size()-i;
                    break;
                }
            }
        }
        return maxStep;
    }


}

