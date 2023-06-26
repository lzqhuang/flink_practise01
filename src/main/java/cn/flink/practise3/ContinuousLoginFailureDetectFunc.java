package cn.flink.practise3;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * ## 需求三：恶意登录监控
 * 用户在短时间内频繁登录失败，有程序恶意攻击的可能，同一用户（可以是不同IP）在2秒内连续两次登录失败，需要报警。
 */
public class ContinuousLoginFailureDetectFunc {

    public static void main(String[] args) throws Exception {
        //todo: 1、构建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //todo: 2、接受socket数据
        DataStream<LoginEvent> loginEventDataStream = env.fromElements(
                new LoginEvent("192.168.1.100", "000001", 1511658001L, "failed"),
                new LoginEvent("192.168.1.100", "000001", 1511658002L, "failed"),
                new LoginEvent("192.168.1.100", "000002", 1511658002L, "success"),
                new LoginEvent("192.168.1.100", "000003", 1511658002L, "success"),
                new LoginEvent("192.168.1.100", "000004", 1511658002L, "success"),
                new LoginEvent("192.168.1.100", "000005", 1511658002L, "failed"),
                new LoginEvent("192.168.1.100", "000005", 1511658005L, "failed"),
                new LoginEvent("192.168.1.100", "000006", 1511658009L, "failed"),
                new LoginEvent("192.168.1.100", "000006", 1511658015L, "failed")
        );

        //todo: 3、数据处理分组
        KeyedStream<LoginEvent, Object> keyedStream = loginEventDataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.eventTime * 1000;
                            }
                        })
                )
                .keyBy(loginEvent -> loginEvent.userId);

        //定义pattern
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "failed".equals(value.result);
            }
        }).followedByAny("second").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "failed".equals(value.result);
            }
        }).within(Time.seconds(2));

        PatternStream<LoginEvent> LoginEventPatternStream = CEP.pattern(keyedStream, pattern).inEventTime();

        LoginEventPatternStream.flatSelect(new PatternFlatSelectFunction<LoginEvent, Void>() {
            @Override
            public void flatSelect(Map<String, List<LoginEvent>> patternMap, Collector<Void> collector) throws Exception {
                List<LoginEvent> startMatchList = patternMap.get("start");
                List<LoginEvent> secondMatchList = patternMap.get("second");

                LoginEvent startResult = startMatchList.iterator().next();
                LoginEvent secondResult = secondMatchList.iterator().next();

                System.out.println("第一条数据：" + startResult.userId + ", eventTime：" + startResult.eventTime);
                System.out.println("第二条数据：" + secondResult.userId + ", eventTime：" + secondResult.eventTime);
            }
        });

        env.execute();
    }
}