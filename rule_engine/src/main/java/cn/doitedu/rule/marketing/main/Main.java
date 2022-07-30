package cn.doitedu.rule.marketing.main;


import cn.doitedu.rule.marketing.beans.EventBean;
import cn.doitedu.rule.marketing.beans.RuleMatchResult;
import cn.doitedu.rule.marketing.functions.JasonToEventBean;
import cn.doitedu.rule.marketing.functions.KafkaSourceBuilder;
import cn.doitedu.rule.marketing.functions.RuleMatchKeyedProcessFunction;
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
        env.setParallelism(1);

        //读取kafka source数据
        KafkaSourceBuilder kafkaSourceBuilder = new KafkaSourceBuilder();
        FlinkKafkaConsumer<String> kafkaSouce = kafkaSourceBuilder.build("zenniu_applog");
        DataStreamSource<String> dss = env.addSource(kafkaSouce);

        DataStream<EventBean> dsBean = dss.map(new JasonToEventBean()).filter(e -> e != null);
        //dsBean.print();

        KeyedStream<EventBean, String> keyedDs = dsBean.keyBy(bean -> bean.getDeviceId());
        DataStream<RuleMatchResult> matchResultDs = keyedDs.process(new RuleMatchKeyedProcessFunction());

        matchResultDs.print();

        env.execute();
    }
}
