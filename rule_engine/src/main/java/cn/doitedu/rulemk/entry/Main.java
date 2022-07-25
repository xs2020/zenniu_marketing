package cn.doitedu.rulemk.entry;

import cn.doitedu.rulemk.beans.EventBean;
import cn.doitedu.rulemk.beans.RuleMatchResult;
import cn.doitedu.rulemk.functions.JasonToEventBean;
import cn.doitedu.rulemk.functions.RuleMatchKeyedProcessFunction;
import cn.doitedu.rulemk.sources.KafkaSourceBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/*
   规则：
      触发事件：K事件，事件属性(p2=v1)
      画像属性条件：tag87=v2, tag26=v1
      行为次数条件：2021-06-18  ~ 当前，事件C(p6=v8,p12=v5) 做过>=2次
 */
public class Main {
    public static void main(String[] args) throws Exception {
        //构建env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //读取kafka source数据
        KafkaSourceBuilder kafkaSourceBuilder = new KafkaSourceBuilder();
        FlinkKafkaConsumer<String> kafkaSouce = kafkaSourceBuilder.build("zenniu_applog");
        DataStreamSource<String> dss = env.addSource(kafkaSouce);

        DataStream<EventBean> dsBean = dss.map(new JasonToEventBean()).filter(e -> e != null);
        //dsBean.print();

        KeyedStream<EventBean, String> keyedDs = dsBean.keyBy(bean -> bean.getDeviceId());
        SingleOutputStreamOperator<RuleMatchResult> matchResultDs = keyedDs.process(new RuleMatchKeyedProcessFunction());
        matchResultDs.print();

        env.execute();
    }
}
