package cn.doitedu.rule.marketing.utils;

import org.apache.kafka.common.protocol.types.Field;

public class ConfigNames {

    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    public static final String KAFKA_AUTO_OFFSET_RESET = "kafka.auto.offset.reset";
    public static final String KAFKA_ACTION_DETAIL_TOPIC = "kafka.action.detail.topic";

    public static final String CK_JDBC_URL = "ck.jdbc.url";
    public static final String CK_JDBC_DRIVER = "ck.jdbc.driver";
    public static final String CK_JDBC_DETAIL_TABLE = "ck.jdbc.detail.table";

    public static final String HBASE_ZK_QUORUM = "hbase.zookeeper.quorum";
    public static final String HBASE_PROFILE_TABLE = "hbase.profile.table";

    public static final String REDIS_HOST = "redis.host";
    public static final String REDIS_PORT = "redis.port";
}
