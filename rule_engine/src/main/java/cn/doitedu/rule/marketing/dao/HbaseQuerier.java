package cn.doitedu.rule.marketing.dao;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

public class HbaseQuerier {

        Table table;
        String familyName;
    public HbaseQuerier(Connection conn,String profileTableName,String familyName) throws IOException {
        table = conn.getTable(TableName.valueOf(profileTableName));
        this.familyName = familyName;
    }

    /**
     *在hbase中查询画像条件是否满足
     * @Param eventCombinationCondition 行为组合条件
     * @Param queryRangeStart 查询时间范围起始
     * @Param queryRangeEnd   查询时间范围结束
     * @return 出现的次数
     */
    public boolean queryProfileConditionIsMatch(Map<String,String> profileConditions,String deviceId) throws IOException {
        Get get = new Get(deviceId.getBytes());
        Set<String> tags = profileConditions.keySet();
        for (String tag : tags) {
            get.addColumn(familyName.getBytes(),tag.getBytes());

        }

        //执行get查询
        Result result = table.get(get);
        for (String tag : tags) {
            byte[] v = result.getValue(familyName.getBytes(), tag.getBytes());
            String value = v.toString();
            if(StringUtils.isBlank(value) || !profileConditions.get(tag).equals(value)) return false;
        }

       return true;
    }
}
