package cn.doitedu.rule.engine.Utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

@Slf4j
public class ConnectionUtils {

    static Config  config = ConfigFactory.load();

    public static Connection getHbaseConnection() throws IOException {
        log.debug("Hbase 连接准备创建......");
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum",config.getString(ConfigNames.HBASE_ZK_QUORUM));
        Connection HbaseConn = ConnectionFactory.createConnection(conf);
        log.debug("Hbase 连接创建成功....");
        return HbaseConn;
    }

    public static java.sql.Connection getClickHouseConnection() throws SQLException, ClassNotFoundException {
        log.debug("ClickHouse 连接准备创建......");
        Class.forName(config.getString(ConfigNames.CLICKHOUSE_DRIVER));
        java.sql.Connection connection = DriverManager.getConnection(config.getString(ConfigNames.CLICKHOUSE_URL));
        return connection;
    }


}
