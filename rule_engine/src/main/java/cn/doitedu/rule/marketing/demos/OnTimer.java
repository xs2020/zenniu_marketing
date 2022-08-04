package cn.doitedu.rule.marketing.demos;

import cn.doitedu.rule.marketing.utils.StateDescContainer;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


import java.time.Duration;
import java.util.Iterator;

public class OnTimer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStreamSource<String> ds = env.socketTextStream("localhost", 5656);
        SingleOutputStreamOperator<String> watermarks = ds.assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofMillis(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {

                    @Override
                    public long extractTimestamp(String in, long l) {
                        String[] splits = in.split(",");
                        return Long.parseLong(splits[2]);
                    }
                }));
        watermarks.keyBy(s->s.split(",")[0])
                .process(new KeyedProcessFunction<String, String, String>() {
                    ListState<Tuple2<String, Long>> listState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("time", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {})));
                    }

                    @Override
            public void processElement(String value, Context ctx, Collector<String> collector) throws Exception {
                        String[] split = value.split(",");
                        if("A".equals(split[1])){
                            ctx.timerService().registerEventTimeTimer(Long.parseLong(split[2])+3000);
                        }
                        if("C".equals(split[1])){
                            listState.add(Tuple2.of("C",Long.parseLong(split[2])));
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        Iterator<Tuple2<String, Long>> iterator = listState.get().iterator();
                        boolean flag = false;
                        while (iterator.hasNext()){
                            if(timestamp>=iterator.next().f1){
                                flag=true;
                                iterator.remove();
                            }

                        }

                        if(flag) System.out.println("在A事件被触发后，3秒内发生了C事件");
                    }
                });


        env.execute();
    }
}
