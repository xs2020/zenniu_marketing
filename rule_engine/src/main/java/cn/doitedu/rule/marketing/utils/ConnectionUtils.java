package cn.doitedu.rule.marketing.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import redis.clients.jedis.Jedis;

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
        Class.forName(config.getString(ConfigNames.CK_JDBC_DRIVER));
        java.sql.Connection connection = DriverManager.getConnection(config.getString(ConfigNames.CK_JDBC_URL));
        return connection;
    }

    public static Jedis getRedisConnection(){
        String host = config.getString(ConfigNames.REDIS_HOST);
        int port = config.getInt(ConfigNames.REDIS_HOST);

        Jedis jedis = new Jedis(host, port);
        String ping = jedis.ping();
        if(StringUtils.isNotBlank(ping)){
            log.debug("redis connection successfully created");
        }else {
            log.error("redis connection creation failed");
        }

        return jedis;
    }


}
