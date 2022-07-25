package cn.doitedu.rulemk.queryservice;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

public class HbaseQueryServiceIml implements QueryService{
    Connection hbaseconn;
    public HbaseQueryServiceIml(Connection hbaseconn) {
        this.hbaseconn = hbaseconn;
    }


    public boolean hbaseQueryResult(String deviceId, Map<String,String> userProfileConditions) throws IOException {

        Table table = hbaseconn.getTable(TableName.valueOf("zenniu_profile"));
        Get rowKey = new Get(Bytes.toBytes(deviceId));
        Set<String> tags = userProfileConditions.keySet();
        for (String tag : tags) {
            rowKey.addColumn(Bytes.toBytes("f"),Bytes.toBytes("tag"));
        }
        Result result = table.get(rowKey);

        for (String tag:tags){
            String value = userProfileConditions.get(tag);
            byte[] value1 = result.getValue("f".getBytes(StandardCharsets.UTF_8), tag.getBytes());
            if(!value.equals(new String(value1))) return false;
        }

        return true;
    }
}
