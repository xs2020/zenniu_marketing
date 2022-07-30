package cn.doitedu.rule.engine.sources;

import cn.doitedu.rule.engine.Utils.ConfigNames;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSourceBuilder {

    Config config;

    public KafkaSourceBuilder() {
        config = ConfigFactory.load();
    }

    public FlinkKafkaConsumer<String> build(String topic){


        Properties props = new Properties();
        props.setProperty("bootstrap.servers",config.getString(ConfigNames.KAFKA_BOOTSTRAP_SERVERS));
        props.setProperty("auto.offset.reset",config.getString(ConfigNames.KAFKA_AUTO_OFFSET_RESET));

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);

        return kafkaConsumer;
    }
}
