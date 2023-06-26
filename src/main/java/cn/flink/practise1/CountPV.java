package cn.flink.practise1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * ## 需求一  热门商品PV统计
 * 问题： 每隔5分钟输出最近一小时内点击量最多的前N个商品，热门度点击量用浏览次数（“pv”）来衡量
 */

public class CountPV {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 假设我们已经有了一个名为dataStream的DataStream<UserBehavior>
        DataStream<UserBehavior> dataStream = env.fromElements(new UserBehavior(1L, 1001L, 101, "click", 1511658000L),
                new UserBehavior(1L, 1002L, 103, "click", 1511658001L),
                new UserBehavior(2L, 1003L, 102, "click", 1511658002L),
                new UserBehavior(2L, 1003L, 102, "pv", 1511658002L),
                new UserBehavior(2L, 1002L, 102, "pv", 1511658002L),
                new UserBehavior(1L, 1002L, 102, "pv", 1511658002L),
                new UserBehavior(1L, 1001L, 102, "pv", 1511658002L),
                new UserBehavior(1L, 1001L, 102, "pv", 1511658002L),
                new UserBehavior(1L, 1001L, 102, "pv", 1511658002L)
        );

        DataStream<UserBehavior> processedDataStream = dataStream.map((MapFunction<UserBehavior, UserBehavior>) userBehavior -> {
            userBehavior.timestamp = userBehavior.timestamp * 1000;
            return userBehavior;
        });

        DataStream<UserBehavior> timeStream = processedDataStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(UserBehavior userBehavior) {
                        return userBehavior.timestamp;
                    }

                });

        DataStream<UserBehavior> pvStream = timeStream.filter(

                new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior value) throws Exception {
                        return "pv".equals(value.behavior);
                    }
                });

        //pvStream.print();
        DataStream<ItemViewCount> processedStream = pvStream.keyBy(new KeySelector<UserBehavior, Long>() {
                    @Override
                    public Long getKey(UserBehavior value) throws Exception {
                        return value.itemId;
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new CountAgg(), new WindowResultFunction());
        //processedStream.print();

        int N = 1;
        processedStream.keyBy("windowEnd").process(new TopNGoods(N)).print();

        env.execute("Hot Items Job");
    }
}
